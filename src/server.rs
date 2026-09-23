//! A TCP server that speaks the Redis protocol over the store, so that
//! `redis-cli` and any Redis client library can use it.
//!
//! Each connection gets a thread, and the store sits behind a read-write
//! lock. Reads share it: a read is a hash lookup and a positional read of
//! the file, and neither touches anything another reader could disturb.
//! Writes take it alone, so the store never sees two writers.
//!
//! Writes are group-committed. A write appends under the lock and lets go
//! of it before the disk has the bytes; its reply is then held until an
//! fsync has covered it. Whoever needs an fsync first runs one, outside the
//! lock, and every write that landed before it started is made durable by
//! that one call, so many clients writing at once cost a few fsyncs rather
//! than one each. A pipelined batch from one client is one wait.
//!
//! ```no_run
//! use minicask::{Server, Store};
//!
//! let store = Store::open("./my-data")?;
//! let server = Server::bind("127.0.0.1:6379", store)?;
//! server.run()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use crate::log::SyncPolicy;
use crate::resp::{self, Reply};
use crate::store::Store;
use std::io::{self, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread;

/// Replies held for a pipelining client before they are sent regardless,
/// so that one client cannot make the server buffer without limit.
const MAX_HELD_REPLIES: usize = 64 * 1024;

pub struct Server {
    listener: TcpListener,
    shared: Arc<Shared>,
}

struct Shared {
    store: RwLock<Store>,
    commits: GroupCommit,
}

/// Where durability has got to, and whether someone is moving it.
struct Durable {
    point: (u64, u64),
    syncing: bool,
}

/// One fsync for every write that is waiting on one. See the module notes.
struct GroupCommit {
    state: Mutex<Durable>,
    synced: Condvar,
    /// How many fsyncs have been run, for the tests.
    syncs: AtomicU64,
}

impl GroupCommit {
    fn new(durable: (u64, u64)) -> GroupCommit {
        GroupCommit {
            state: Mutex::new(Durable {
                point: durable,
                syncing: false,
            }),
            synced: Condvar::new(),
            syncs: AtomicU64::new(0),
        }
    }

    /// Return once everything up to `point` is on the disk.
    fn wait(&self, store: &RwLock<Store>, point: (u64, u64)) -> crate::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if state.point >= point {
                return Ok(());
            }
            if !state.syncing {
                // Nobody is syncing, so this thread does, for everyone. The
                // handle is taken first and covers whatever was appended
                // by then, which includes `point`.
                state.syncing = true;
                drop(state);
                // The store's lock is let go before the fsync, not after: a
                // `shared(store)` guard used inline would live to the end of
                // the statement, fsync and all, and every writer would queue
                // behind it instead of appending into the next batch.
                let handle = exclusive(store).sync_handle();
                let result = handle.and_then(|handle| handle.sync());
                if let Ok(reached) = result {
                    exclusive(store).note_synced(reached);
                }
                self.syncs.fetch_add(1, Ordering::Relaxed);
                state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                state.syncing = false;
                self.synced.notify_all();
                let reached = result?;
                if reached > state.point {
                    state.point = reached;
                }
                continue;
            }
            state = self.synced.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }
}

impl Server {
    /// Take ownership of an open store and start listening. Nothing is
    /// served until `run` is called.
    pub fn bind<A: ToSocketAddrs>(addr: A, store: Store) -> io::Result<Server> {
        // Whatever the store opened with was read back off the disk, so it
        // is durable already.
        let durable = store.append_point();
        Ok(Server {
            listener: TcpListener::bind(addr)?,
            shared: Arc::new(Shared {
                store: RwLock::new(store),
                commits: GroupCommit::new(durable),
            }),
        })
    }

    /// The address actually bound, which matters when port 0 was asked for.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept connections forever. Only an error from `accept` itself
    /// returns; a misbehaving client only ends its own connection.
    pub fn run(self) -> io::Result<()> {
        for stream in self.listener.incoming() {
            let stream = stream?;
            let shared = Arc::clone(&self.shared);
            thread::spawn(move || {
                // A client that hangs up mid-command or sends garbage is
                // not the server's problem.
                let _ = serve(stream, &shared);
            });
        }
        Ok(())
    }
}

fn serve(stream: TcpStream, shared: &Shared) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;
    // Replies wait here rather than in a `BufWriter`, which sends whenever
    // it fills: a reply to a write must not leave before the write is on
    // the disk, and `durable_by` is how far the disk has to have got.
    let mut out = Vec::new();
    let mut durable_by = None;

    loop {
        let args = match resp::read_command(&mut reader) {
            Ok(Some(args)) => args,
            Ok(None) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                // Redis does the same: report it and hang up, because once
                // framing is lost there is no telling where the next
                // command starts.
                Reply::err(format!("Protocol error: {e}")).write_to(&mut out)?;
                return send(&mut writer, &mut out, &mut durable_by, shared);
            }
            Err(e) => return Err(e),
        };
        if args.is_empty() {
            continue;
        }

        let quit = args[0].eq_ignore_ascii_case(b"QUIT");
        dispatch(shared, &args, &mut durable_by).write_to(&mut out)?;

        // Pipelined clients send a batch of commands and then read a batch
        // of replies. Sending only when there is nothing left to parse
        // turns the batch of replies into one write, and the batch of
        // writes into one wait for the disk.
        if quit || reader.buffer().is_empty() || out.len() >= MAX_HELD_REPLIES {
            send(&mut writer, &mut out, &mut durable_by, shared)?;
        }
        if quit {
            return Ok(());
        }
    }
}

/// Send the held replies, once the writes they acknowledge are durable.
fn send(
    writer: &mut TcpStream,
    out: &mut Vec<u8>,
    durable_by: &mut Option<(u64, u64)>,
    shared: &Shared,
) -> io::Result<()> {
    if let Some(point) = durable_by.take() {
        if let Err(e) = shared.commits.wait(&shared.store, point) {
            // Sending the replies would claim writes the disk has not
            // confirmed. Say so instead, and hang up, since the client
            // is now owed replies that will never come.
            out.clear();
            Reply::err(format!(
                "the disk refused to sync, so earlier writes may not be durable: {e}"
            ))
            .write_to(out)?;
            writer.write_all(out)?;
            return Err(io::Error::new(io::ErrorKind::Other, "sync failed"));
        }
    }
    writer.write_all(out)?;
    out.clear();
    Ok(())
}

// A poisoned lock means a handler panicked mid-command. The store's own
// invariants hold regardless, since it never leaves a write half done in
// memory, so carry on rather than taking every client down.
fn shared(store: &RwLock<Store>) -> RwLockReadGuard<'_, Store> {
    store.read().unwrap_or_else(|e| e.into_inner())
}

fn exclusive(store: &RwLock<Store>) -> RwLockWriteGuard<'_, Store> {
    store.write().unwrap_or_else(|e| e.into_inner())
}

/// A write for a store that syncs every write goes into the store's buffer
/// and its reply waits for a group commit. For a store that does not, it
/// goes to the kernel as usual, since its reply will not wait for anything
/// and the write must still survive the process dying.
fn put(store: &mut Store, key: &[u8], value: &[u8]) -> crate::Result<()> {
    if store.options().sync == SyncPolicy::EveryWrite {
        store.put_deferred(key, value)
    } else {
        store.put(key, value)
    }
}

fn delete(store: &mut Store, key: &[u8]) -> crate::Result<bool> {
    if store.options().sync == SyncPolicy::EveryWrite {
        store.delete_deferred(key)
    } else {
        store.delete(key)
    }
}

/// Note that the reply to a write must wait for the disk to reach where the
/// store's appends have got to. A store that does not sync every write
/// never promised durability, so its replies do not wait.
fn must_reach(store: &Store, durable_by: &mut Option<(u64, u64)>) {
    if store.options().sync == SyncPolicy::EveryWrite {
        let point = store.append_point();
        if durable_by.map_or(true, |p| p < point) {
            *durable_by = Some(point);
        }
    }
}

/// Run one command against the store, under a shared lock for a read and
/// an exclusive one for a write. Writes do not sync; they record in
/// `durable_by` what the reply has to wait for.
fn dispatch(server: &Shared, args: &[Vec<u8>], durable_by: &mut Option<(u64, u64)>) -> Reply {
    let store = &server.store;
    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    let args = &args[1..];

    let result: crate::Result<Reply> = match name.as_str() {
        "PING" => Ok(match args {
            [] => Reply::Simple("PONG".to_string()),
            [msg] => Reply::Bulk(msg.clone()),
            _ => return wrong_arity(&name),
        }),
        "ECHO" => match args {
            [msg] => Ok(Reply::Bulk(msg.clone())),
            _ => return wrong_arity(&name),
        },
        "GET" => match args {
            [key] => shared(store).get(key).map(bulk_or_null),
            _ => return wrong_arity(&name),
        },
        "SET" => match args {
            [key, value, options @ ..] => {
                let mut store = exclusive(store);
                let reply = set(&mut store, key, value, options);
                must_reach(&store, durable_by);
                reply
            }
            _ => return wrong_arity(&name),
        },
        "DEL" => match args {
            [] => return wrong_arity(&name),
            keys => {
                let mut store = exclusive(store);
                let reply = count(keys.iter().map(|k| delete(&mut store, k)));
                must_reach(&store, durable_by);
                reply
            }
        },
        "EXISTS" => match args {
            [] => return wrong_arity(&name),
            keys => {
                let store = shared(store);
                Ok(Reply::Integer(
                    keys.iter().filter(|k| store.contains_key(k)).count() as i64,
                ))
            }
        },
        "MGET" => match args {
            [] => return wrong_arity(&name),
            keys => {
                // One guard for the lot, so the values come from one
                // moment rather than from between two writes.
                let store = shared(store);
                keys.iter()
                    .map(|k| store.get(k).map(bulk_or_null))
                    .collect::<crate::Result<Vec<_>>>()
                    .map(Reply::Array)
            }
        },
        "MSET" => match args {
            [] => return wrong_arity(&name),
            pairs if pairs.len() % 2 != 0 => return wrong_arity(&name),
            pairs => {
                let mut store = exclusive(store);
                let reply = pairs
                    .chunks(2)
                    .try_for_each(|pair| put(&mut store, &pair[0], &pair[1]))
                    .map(|()| Reply::ok());
                must_reach(&store, durable_by);
                reply
            }
        },
        "KEYS" => match args {
            [pattern] => {
                let store = shared(store);
                let mut keys: Vec<&[u8]> =
                    store.keys().filter(|k| glob_match(pattern, k)).collect();
                keys.sort_unstable();
                Ok(Reply::Array(
                    keys.into_iter().map(|k| Reply::Bulk(k.to_vec())).collect(),
                ))
            }
            _ => return wrong_arity(&name),
        },
        "DBSIZE" => match args {
            [] => Ok(Reply::Integer(shared(store).len() as i64)),
            _ => return wrong_arity(&name),
        },
        "FLUSHDB" | "FLUSHALL" => {
            let mut store = exclusive(store);
            let keys: Vec<Vec<u8>> = store.keys().map(<[u8]>::to_vec).collect();
            let reply = keys
                .iter()
                .try_for_each(|k| delete(&mut store, k).map(|_| ()))
                .map(|()| Reply::ok());
            must_reach(&store, durable_by);
            reply
        }
        "SELECT" => match args {
            [db] if db.as_slice() == b"0" => Ok(Reply::ok()),
            [_] => Ok(Reply::err("DB index is out of range")),
            _ => return wrong_arity(&name),
        },
        // Sent by redis-cli on connect to learn about the server; an empty
        // answer is enough for it to carry on.
        "COMMAND" => Ok(Reply::Array(Vec::new())),
        // Client libraries announce themselves with these. Nothing to do.
        "CLIENT" => Ok(Reply::ok()),
        "QUIT" => Ok(Reply::ok()),
        _ => Ok(Reply::err(format!(
            "unknown command '{}'",
            name.to_ascii_lowercase()
        ))),
    };

    result.unwrap_or_else(|e| Reply::err(e.to_string()))
}

/// `SET key value [NX|XX]`. Expiry options are refused rather than silently
/// accepted, since the store has no notion of a TTL and a client asking
/// for one should find out.
fn set(store: &mut Store, key: &[u8], value: &[u8], options: &[Vec<u8>]) -> crate::Result<Reply> {
    let mut only_if_missing = false;
    let mut only_if_present = false;
    for option in options {
        match option.to_ascii_uppercase().as_slice() {
            b"NX" => only_if_missing = true,
            b"XX" => only_if_present = true,
            _ => return Ok(Reply::err("syntax error")),
        }
    }
    if only_if_missing && only_if_present {
        return Ok(Reply::err("syntax error"));
    }
    let present = store.contains_key(key);
    if (only_if_missing && present) || (only_if_present && !present) {
        return Ok(Reply::Null);
    }
    put(store, key, value)?;
    Ok(Reply::ok())
}

fn bulk_or_null(value: Option<Vec<u8>>) -> Reply {
    value.map_or(Reply::Null, Reply::Bulk)
}

/// How many of a batch of operations reported `true`, stopping at the first
/// error.
fn count(results: impl Iterator<Item = crate::Result<bool>>) -> crate::Result<Reply> {
    let mut n = 0;
    for result in results {
        if result? {
            n += 1;
        }
    }
    Ok(Reply::Integer(n))
}

fn wrong_arity(name: &str) -> Reply {
    Reply::err(format!(
        "wrong number of arguments for '{}' command",
        name.to_ascii_lowercase()
    ))
}

/// The subset of Redis glob syntax that `KEYS` needs: `*`, `?` and
/// backslash escapes. Character classes are treated as literals.
pub(crate) fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    match (pattern.split_first(), text.split_first()) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some((b'*', rest)), _) => {
            glob_match(rest, text) || (!text.is_empty() && glob_match(pattern, &text[1..]))
        }
        (Some((b'?', rest)), Some((_, text_rest))) => glob_match(rest, text_rest),
        (Some((b'\\', rest)), Some((t, text_rest))) if !rest.is_empty() => {
            rest[0] == *t && glob_match(&rest[1..], text_rest)
        }
        (Some((p, rest)), Some((t, text_rest))) => p == t && glob_match(rest, text_rest),
        (Some(_), None) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    /// A client that pipelines a hundred writes is owed a hundred replies,
    /// and none of them may be sent before its write is on the disk. One
    /// fsync covers the lot. Syncing each write as it came, which is what
    /// the server did before, would take a hundred.
    #[test]
    fn a_pipelined_batch_of_writes_shares_an_fsync() {
        let dir = std::env::temp_dir().join(format!(
            "minicask-group-commit-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let server = Server::bind("127.0.0.1:0", Store::open(&dir).unwrap()).unwrap();
        let addr = server.local_addr().unwrap();
        let shared = Arc::clone(&server.shared);
        thread::spawn(move || server.run());

        let mut batch = Vec::new();
        for i in 0..100 {
            let key = format!("key-{i:03}");
            batch.extend_from_slice(
                format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len()).as_bytes(),
            );
        }
        let mut conn = TcpStream::connect(addr).unwrap();
        conn.write_all(&batch).unwrap();
        let mut replies = vec![0u8; 100 * b"+OK\r\n".len()];
        conn.read_exact(&mut replies).unwrap();
        assert!(replies.chunks(5).all(|r| r == b"+OK\r\n"));

        let syncs = shared.commits.syncs.load(Ordering::Relaxed);
        assert!(
            (1..=4).contains(&syncs),
            "{syncs} fsyncs for one pipelined batch of a hundred writes"
        );
        // Every reply was sent, so the disk has everything the store wrote.
        assert!(
            shared.commits.state.lock().unwrap().point
                >= shared.store.read().unwrap().append_point()
        );
    }

    /// Once a group commit returns for a write, the write is on the disk,
    /// not merely in the kernel or in the store's own buffer.
    #[test]
    fn a_write_is_on_the_disk_once_its_group_commit_returns() {
        let dir = std::env::temp_dir().join(format!(
            "minicask-group-durable-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = Store::open(&dir).unwrap();
        let durable = store.append_point();
        let shared = Shared {
            store: RwLock::new(store),
            commits: GroupCommit::new(durable),
        };
        let point = {
            let mut store = exclusive(&shared.store);
            store.put_deferred(b"acknowledged", b"yes").unwrap();
            store.append_point()
        };
        shared.commits.wait(&shared.store, point).unwrap();

        let store = shared.store.into_inner().unwrap();
        store.simulate_power_cut().unwrap();
        let store = Store::open(&dir).unwrap();
        assert_eq!(store.get(b"acknowledged").unwrap(), Some(b"yes".to_vec()));
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn glob_patterns() {
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"user:*", b"user:42"));
        assert!(!glob_match(b"user:*", b"order:42"));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(!glob_match(b"h?llo", b"hllo"));
        assert!(glob_match(b"*:*", b"a:b"));
        assert!(glob_match(b"\\*", b"*"));
        assert!(!glob_match(b"\\*", b"x"));
        assert!(glob_match(b"exact", b"exact"));
        assert!(!glob_match(b"exact", b"exactly"));
    }
}

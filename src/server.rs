//! A TCP server that speaks the Redis protocol over the store, so that
//! `redis-cli` and any Redis client library can use it.
//!
//! The store is single-threaded by design, so the server keeps it behind a
//! mutex and each connection gets a thread. That is the simplest thing that
//! is correct: a command holds the lock for one append or one read, and the
//! store never sees two writers.
//!
//! ```no_run
//! use minicask::{Server, Store};
//!
//! let store = Store::open("./my-data")?;
//! let server = Server::bind("127.0.0.1:6379", store)?;
//! server.run()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use crate::resp::{self, Reply};
use crate::store::Store;
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;

pub struct Server {
    listener: TcpListener,
    store: Arc<Mutex<Store>>,
}

impl Server {
    /// Take ownership of an open store and start listening. Nothing is
    /// served until `run` is called.
    pub fn bind<A: ToSocketAddrs>(addr: A, store: Store) -> io::Result<Server> {
        Ok(Server {
            listener: TcpListener::bind(addr)?,
            store: Arc::new(Mutex::new(store)),
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
            let store = Arc::clone(&self.store);
            thread::spawn(move || {
                // A client that hangs up mid-command or sends garbage is
                // not the server's problem.
                let _ = serve(stream, &store);
            });
        }
        Ok(())
    }
}

fn serve(stream: TcpStream, store: &Mutex<Store>) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);

    loop {
        let args = match resp::read_command(&mut reader) {
            Ok(Some(args)) => args,
            Ok(None) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                // Redis does the same: report it and hang up, because once
                // framing is lost there is no telling where the next
                // command starts.
                Reply::err(format!("Protocol error: {e}")).write_to(&mut writer)?;
                writer.flush()?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        if args.is_empty() {
            continue;
        }

        let quit = args[0].eq_ignore_ascii_case(b"QUIT");
        dispatch(store, &args).write_to(&mut writer)?;

        // Pipelined clients send a batch of commands and then read a batch
        // of replies. Flushing only when there is nothing left to parse
        // turns the batch of replies into one write.
        if quit || reader.buffer().is_empty() {
            writer.flush()?;
        }
        if quit {
            return Ok(());
        }
    }
}

/// Run one command against the store.
fn dispatch(store: &Mutex<Store>, args: &[Vec<u8>]) -> Reply {
    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    let args = &args[1..];

    // A poisoned lock means a handler panicked mid-command. The store's own
    // invariants hold regardless, since it never leaves a write half done
    // in memory, so carry on rather than taking every client down.
    let mut store = store.lock().unwrap_or_else(|e| e.into_inner());

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
            [key] => store.get(key).map(bulk_or_null),
            _ => return wrong_arity(&name),
        },
        "SET" => match args {
            [key, value, options @ ..] => set(&mut store, key, value, options),
            _ => return wrong_arity(&name),
        },
        "DEL" => match args {
            [] => return wrong_arity(&name),
            keys => count(keys.iter().map(|k| store.delete(k))),
        },
        "EXISTS" => match args {
            [] => return wrong_arity(&name),
            keys => Ok(Reply::Integer(
                keys.iter().filter(|k| store.contains_key(k)).count() as i64,
            )),
        },
        "MGET" => match args {
            [] => return wrong_arity(&name),
            keys => keys
                .iter()
                .map(|k| store.get(k).map(bulk_or_null))
                .collect::<crate::Result<Vec<_>>>()
                .map(Reply::Array),
        },
        "MSET" => match args {
            [] => return wrong_arity(&name),
            pairs if pairs.len() % 2 != 0 => return wrong_arity(&name),
            pairs => pairs
                .chunks(2)
                .try_for_each(|pair| store.put(&pair[0], &pair[1]))
                .map(|()| Reply::ok()),
        },
        "KEYS" => match args {
            [pattern] => {
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
            [] => Ok(Reply::Integer(store.len() as i64)),
            _ => return wrong_arity(&name),
        },
        "FLUSHDB" | "FLUSHALL" => {
            let keys: Vec<Vec<u8>> = store.keys().map(<[u8]>::to_vec).collect();
            keys.iter()
                .try_for_each(|k| store.delete(k).map(|_| ()))
                .map(|()| Reply::ok())
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
    store.put(key, value)?;
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
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
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
    use super::glob_match;

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

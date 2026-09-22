//! A replicated node as a running process.
//!
//! Three things share one [`ReplicatedStore`] behind a mutex:
//!
//! - a **ticker**, which gives the consensus node its sense of time;
//! - a **peer listener**, one thread per inbound peer connection, feeding
//!   messages in;
//! - a **client listener**, one thread per Redis client, turning `SET` and
//!   `DEL` into proposals and waiting for them to commit.
//!
//! Outbound peer traffic goes through one thread and one queue per peer,
//! which reconnects on its own. Nothing blocks the node's lock on a socket:
//! a message is handed to a queue and forgotten, because Raft already
//! retries anything that does not arrive.
//!
//! The lock is the same trade the single-node server makes. A command holds
//! it for one append or one read, and consensus is a network round trip
//! anyway, so the mutex is not what limits throughput here.

use crate::error::Result;
use crate::raft::{wire, Action, Config, DiskStorage, Message, Node, NodeId, ProposeError, Role};
use crate::replicated::ReplicatedStore;
use crate::resp::{self, Reply};
use crate::store::Store;
use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

type Replica = ReplicatedStore<DiskStorage>;

/// How long a client waits for its write to commit before being told it
/// did not. The write may still commit afterwards: this is a reply
/// deadline, not a cancellation, and there is no way to cancel a proposal
/// that a majority may already hold.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(5);

/// One peer: who it is and where to reach it.
#[derive(Debug, Clone)]
pub struct Peer {
    pub id: NodeId,
    /// Where its consensus port is, for this node to dial.
    pub raft_addr: String,
    /// Where its clients connect, so this node can redirect them.
    pub client_addr: String,
}

/// Everything a node needs to know to join a cluster.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub id: NodeId,
    pub peers: Vec<Peer>,
    /// Milliseconds per consensus tick. The election timeouts in
    /// [`Config`] are counted in these.
    pub tick_ms: u64,
    pub raft: Config,
}

impl ClusterConfig {
    fn ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self.peers.iter().map(|p| p.id).collect();
        ids.push(self.id);
        ids.sort_unstable();
        ids
    }
}

/// The shared node, plus the signal that something was applied.
struct Shared {
    replica: Mutex<Replica>,
    /// Woken whenever the applied index or the role changes, so a client
    /// waiting on a write does not have to poll.
    progress: Condvar,
    /// One queue per peer. The mutex is not for contention, which there
    /// is none of: a `Sender` is `Send` but was not `Sync` until Rust
    /// 1.72, and this crate builds on 1.70. Wrapping each one separately
    /// rather than the map keeps dispatch to one peer off another's path.
    senders: HashMap<NodeId, Mutex<Sender<Message>>>,
    client_addrs: HashMap<NodeId, String>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Replica> {
        // A panicking command leaves the store's own invariants intact, so
        // there is no reason to take the rest of the cluster down with it.
        self.replica.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Hand a node's outgoing messages to the per-peer queues.
    fn dispatch(&self, actions: Vec<Action>) {
        for Action::Send { to, message } in actions {
            if let Some(sender) = self.senders.get(&to) {
                // A full or dead queue is not an error worth reporting:
                // Raft resends on the next heartbeat.
                let sender = sender.lock().unwrap_or_else(|e| e.into_inner());
                let _ = sender.send(message);
            }
        }
    }

    fn step(&self, from: NodeId, message: Message) {
        let actions = {
            let mut replica = self.lock();
            match replica.step(from, message) {
                Ok(actions) => actions,
                Err(e) => {
                    // A storage failure here means the log could not be
                    // made durable, and continuing would mean
                    // acknowledging what is not on disk.
                    eprintln!("minicask-cluster: consensus storage failed: {e}");
                    std::process::abort();
                }
            }
        };
        self.progress.notify_all();
        self.dispatch(actions);
    }
}

/// A running cluster node.
pub struct ClusterNode {
    shared: Arc<Shared>,
    raft_listener: TcpListener,
    client_listener: TcpListener,
    tick: Duration,
}

impl ClusterNode {
    /// Open both directories, bind both ports and connect to the peers.
    ///
    /// `raft_addr` carries consensus traffic and should not be reachable by
    /// anything but the other nodes; `client_addr` speaks RESP.
    pub fn bind<A: ToSocketAddrs, B: ToSocketAddrs>(
        raft_dir: &Path,
        store_dir: &Path,
        raft_addr: A,
        client_addr: B,
        config: ClusterConfig,
    ) -> Result<ClusterNode> {
        let storage = DiskStorage::open(raft_dir)?;
        let store = Store::open(store_dir)?;
        let node = Node::new(config.id, config.ids(), config.raft, storage);
        let replica = ReplicatedStore::new(node, store);

        let raft_listener = TcpListener::bind(raft_addr)?;
        let client_listener = TcpListener::bind(client_addr)?;

        let mut senders = HashMap::new();
        let mut client_addrs = HashMap::new();
        for peer in &config.peers {
            let (tx, rx) = mpsc::channel();
            senders.insert(peer.id, Mutex::new(tx));
            client_addrs.insert(peer.id, peer.client_addr.clone());
            let addr = peer.raft_addr.clone();
            let id = config.id;
            thread::Builder::new()
                .name(format!("peer-{}", peer.id))
                .spawn(move || send_to_peer(id, &addr, rx))?;
        }

        Ok(ClusterNode {
            shared: Arc::new(Shared {
                replica: Mutex::new(replica),
                progress: Condvar::new(),
                senders,
                client_addrs,
            }),
            raft_listener,
            client_listener,
            tick: Duration::from_millis(config.tick_ms.max(1)),
        })
    }

    pub fn raft_addr(&self) -> io::Result<SocketAddr> {
        self.raft_listener.local_addr()
    }

    pub fn client_addr(&self) -> io::Result<SocketAddr> {
        self.client_listener.local_addr()
    }

    /// Start the ticker and both listeners. Only an `accept` failure on the
    /// client port returns.
    pub fn run(self) -> io::Result<()> {
        let ClusterNode {
            shared,
            raft_listener,
            client_listener,
            tick,
        } = self;

        {
            let shared = Arc::clone(&shared);
            thread::Builder::new()
                .name("ticker".to_string())
                .spawn(move || ticker(&shared, tick))?;
        }
        {
            let shared = Arc::clone(&shared);
            thread::Builder::new()
                .name("peers".to_string())
                .spawn(move || accept_peers(&shared, &raft_listener))?;
        }

        for stream in client_listener.incoming() {
            let stream = stream?;
            let shared = Arc::clone(&shared);
            thread::spawn(move || {
                let _ = serve_client(stream, &shared);
            });
        }
        Ok(())
    }
}

fn ticker(shared: &Arc<Shared>, tick: Duration) {
    loop {
        thread::sleep(tick);
        let actions = {
            let mut replica = shared.lock();
            match replica.tick() {
                Ok(actions) => actions,
                Err(e) => {
                    eprintln!("minicask-cluster: consensus storage failed: {e}");
                    std::process::abort();
                }
            }
        };
        shared.progress.notify_all();
        shared.dispatch(actions);
    }
}

fn accept_peers(shared: &Arc<Shared>, listener: &TcpListener) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let shared = Arc::clone(shared);
        thread::spawn(move || {
            let _ = read_from_peer(stream, &shared);
        });
    }
}

fn read_from_peer(stream: TcpStream, shared: &Arc<Shared>) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut reader = BufReader::new(stream);
    while let Some((from, message)) = wire::read_message(&mut reader)? {
        shared.step(from, message);
    }
    Ok(())
}

/// Own one peer's connection, reconnecting whenever it breaks.
fn send_to_peer(me: NodeId, addr: &str, rx: Receiver<Message>) {
    let mut stream: Option<BufWriter<TcpStream>> = None;
    // A peer that is down should not be dialled in a tight loop.
    let mut backoff = Duration::from_millis(20);
    const MAX_BACKOFF: Duration = Duration::from_secs(1);

    while let Ok(message) = rx.recv() {
        if stream.is_none() {
            match TcpStream::connect(addr) {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    stream = Some(BufWriter::new(s));
                    backoff = Duration::from_millis(20);
                }
                Err(_) => {
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue; // the message is dropped; Raft will resend
                }
            }
        }

        let writer = stream.as_mut().expect("just connected");
        if wire::write_message(writer, me, &message)
            .and_then(|()| writer.flush())
            .is_err()
        {
            // Drop it and reconnect on the next message.
            stream = None;
        }
    }
}

// -- clients ------------------------------------------------------------

fn serve_client(stream: TcpStream, shared: &Arc<Shared>) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);

    loop {
        let args = match resp::read_command(&mut reader) {
            Ok(Some(args)) => args,
            Ok(None) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
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
        dispatch(shared, &args).write_to(&mut writer)?;
        if quit || reader.buffer().is_empty() {
            writer.flush()?;
        }
        if quit {
            return Ok(());
        }
    }
}

fn dispatch(shared: &Arc<Shared>, args: &[Vec<u8>]) -> Reply {
    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    let rest = &args[1..];

    match name.as_str() {
        "PING" => match rest {
            [] => Reply::Simple("PONG".to_string()),
            [msg] => Reply::Bulk(msg.clone()),
            _ => wrong_arity(&name),
        },
        "ECHO" => match rest {
            [msg] => Reply::Bulk(msg.clone()),
            _ => wrong_arity(&name),
        },
        "QUIT" => Reply::ok(),
        "COMMAND" => Reply::Array(Vec::new()),
        "CLIENT" => Reply::ok(),
        "SELECT" => match rest {
            [db] if db.as_slice() == b"0" => Reply::ok(),
            [_] => Reply::err("DB index is out of range"),
            _ => wrong_arity(&name),
        },

        // Reads and writes both go to the leader. A follower's store is
        // only as current as the last entry it applied, so answering from
        // one would hand back a value that a later read could contradict.
        "GET" => match rest {
            [key] => read(shared, |r| match r.get(key) {
                Ok(Some(value)) => Reply::Bulk(value),
                Ok(None) => Reply::Null,
                Err(e) => Reply::err(e.to_string()),
            }),
            _ => wrong_arity(&name),
        },
        "MGET" => match rest {
            [] => wrong_arity(&name),
            keys => read(shared, |r| {
                Reply::Array(
                    keys.iter()
                        .map(|k| match r.get(k) {
                            Ok(Some(v)) => Reply::Bulk(v),
                            _ => Reply::Null,
                        })
                        .collect(),
                )
            }),
        },
        "EXISTS" => match rest {
            [] => wrong_arity(&name),
            keys => read(shared, |r| {
                Reply::Integer(keys.iter().filter(|k| r.contains_key(k)).count() as i64)
            }),
        },
        "DBSIZE" => match rest {
            [] => read(shared, |r| Reply::Integer(r.len() as i64)),
            _ => wrong_arity(&name),
        },
        "KEYS" => match rest {
            [pattern] => read(shared, |r| {
                let mut keys: Vec<Vec<u8>> = r
                    .store()
                    .keys()
                    .filter(|k| crate::server::glob_match(pattern, k))
                    .map(<[u8]>::to_vec)
                    .collect();
                keys.sort_unstable();
                Reply::Array(keys.into_iter().map(Reply::Bulk).collect())
            }),
            _ => wrong_arity(&name),
        },

        "SET" => match rest {
            [key, value] => write(shared, |r| r.put(key, value)),
            [_, _, ..] => Reply::err("this cluster's SET takes no options"),
            _ => wrong_arity(&name),
        },
        "DEL" => match rest {
            [] => wrong_arity(&name),
            [key] => write(shared, |r| r.delete(key)),
            _ => Reply::err("this cluster's DEL takes one key"),
        },

        // Where the cluster stands, which is the first thing anyone asks.
        "RAFT" | "INFO" => {
            let replica = shared.lock();
            let node = replica.node();
            Reply::Bulk(
                format!(
                    "id:{}\r\nrole:{}\r\nterm:{}\r\nleader:{}\r\ncommit_index:{}\r\napplied_index:{}\r\nlast_index:{}\r\nkeys:{}\r\n",
                    node.id(),
                    match node.role() {
                        Role::Leader => "leader",
                        Role::Candidate => "candidate",
                        Role::PreCandidate => "pre-candidate",
                        Role::Follower => "follower",
                    },
                    node.term(),
                    node.leader().map_or_else(|| "none".to_string(), |id| id.to_string()),
                    node.commit_index(),
                    replica.applied_index(),
                    node.last_index(),
                    replica.len(),
                )
                .into_bytes(),
            )
        }

        _ => Reply::err(format!("unknown command '{}'", name.to_ascii_lowercase())),
    }
}

/// Run a read, but only on a leader that is fit to answer one.
///
/// A leader elected moments ago may still be applying what it inherited,
/// and answering during that window can miss a write that was already
/// acknowledged. Rather than redirect a client in a loop, wait for the
/// node to settle; it takes one round trip.
fn read(shared: &Arc<Shared>, f: impl FnOnce(&Replica) -> Reply) -> Reply {
    let deadline = Instant::now() + COMMIT_TIMEOUT;
    let mut replica = shared.lock();
    loop {
        if replica.ready_to_serve() {
            return f(&replica);
        }
        if !replica.is_leader() {
            return not_leader(shared, &replica);
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Reply::Error(
                "TIMEOUT this node leads but has not caught up enough to answer".to_string(),
            );
        };
        let (guard, _) = shared
            .progress
            .wait_timeout(replica, remaining)
            .unwrap_or_else(|e| e.into_inner());
        replica = guard;
    }
}

/// Propose a write and wait for it to take effect here, which means a
/// majority has it on disk.
fn write(
    shared: &Arc<Shared>,
    propose: impl FnOnce(&mut Replica) -> std::result::Result<crate::raft::Accepted, ProposeError>,
) -> Reply {
    let (index, term, actions) = {
        let mut replica = shared.lock();
        match propose(&mut replica) {
            Ok(accepted) => (accepted.index, replica.node().term(), accepted.actions),
            Err(ProposeError::NotLeader { .. }) => return not_leader(shared, &replica),
            Err(e) => return Reply::err(e.to_string()),
        }
    };
    shared.dispatch(actions);

    let deadline = Instant::now() + COMMIT_TIMEOUT;
    let mut replica = shared.lock();
    loop {
        if replica.applied_index() >= index {
            return Reply::ok();
        }
        // Losing the term means this proposal may never commit, and may
        // yet be overwritten. Saying so beats waiting out the clock.
        if replica.node().term() != term || !replica.is_leader() {
            return Reply::Error(
                "NOTLEADER leadership was lost before the write committed; its fate is unknown"
                    .to_string(),
            );
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Reply::Error(
                "TIMEOUT the write did not commit in time; it may still do so".to_string(),
            );
        };
        let (guard, _) = shared
            .progress
            .wait_timeout(replica, remaining)
            .unwrap_or_else(|e| e.into_inner());
        replica = guard;
    }
}

/// Redis clients know `-MOVED addr` from cluster mode, so a redirect in
/// that shape is the one most likely to be understood.
fn not_leader(shared: &Arc<Shared>, replica: &Replica) -> Reply {
    match replica.leader().and_then(|id| shared.client_addrs.get(&id)) {
        Some(addr) => Reply::Error(format!("MOVED 0 {addr}")),
        None => {
            Reply::Error("CLUSTERDOWN no leader is known; the cluster has no majority".to_string())
        }
    }
}

fn wrong_arity(name: &str) -> Reply {
    Reply::err(format!(
        "wrong number of arguments for '{}' command",
        name.to_ascii_lowercase()
    ))
}

//! A replicated node as a running process.
//!
//! Three things share one [`ReplicatedStore`] behind a read-write lock:
//!
//! - a **ticker**, which gives the consensus node its sense of time;
//! - a **peer listener**, one thread per inbound peer connection, feeding
//!   messages in;
//! - a **client listener**, one thread per Redis client, turning `SET` and
//!   `DEL` into proposals and waiting for them to commit, and serving
//!   reads on whichever node the client is connected to.
//!
//! Outbound peer traffic goes through one thread and one queue per peer,
//! which reconnects on its own. Nothing blocks the node's lock on a socket:
//! a message is handed to a queue and forgotten, because Raft already
//! retries anything that does not arrive.
//!
//! Consensus takes the lock alone, for a tick or a message. Reads share it:
//! once a read has been confirmed, answering it is a lookup in the local
//! store, and any number of those run at once. Waiting is done on a
//! separate signal rather than on the lock, so a client waiting for its
//! write to commit holds nothing while it waits.
//!
//! The one long job, writing a snapshot, happens off the lock: the ticker
//! starts it, a thread of its own writes it from a point-in-time view of
//! the store, and the lock is only taken again to install the result.

use crate::error::Result;
use crate::raft::{
    wire, Action, Config, DiskStorage, Member, Message, Node, NodeId, ProposeError, ReadState, Role,
};
use crate::replicated::{Op, ReplicatedStore};
use crate::resp::{self, Reply};
use crate::store::Store;
use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
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
    /// Where to reach the other nodes. On a new cluster these, with this
    /// node, are also its first membership; see `join`. An address given
    /// here always wins over one learned from the log.
    pub peers: Vec<Peer>,
    /// Join a cluster that already exists instead of starting a new one.
    /// The node then starts with no membership, stands for nothing, and
    /// waits for a leader to add it with `RAFT.ADD`. Only matters the first
    /// time: once the node has a membership in its log, it uses that.
    pub join: bool,
    /// Milliseconds per consensus tick. The election timeouts in
    /// [`Config`] are counted in these.
    pub tick_ms: u64,
    pub raft: Config,
    /// Applied entries between snapshots; see
    /// [`ReplicatedStore::set_snapshot_every`].
    pub snapshot_every: u64,
}

/// How a member's addresses travel in the log: both of them, space
/// separated, as the member's context.
fn member_context(raft_addr: &str, client_addr: &str) -> Vec<u8> {
    format!("{raft_addr} {client_addr}").into_bytes()
}

fn parse_context(context: &[u8]) -> Option<(String, String)> {
    let text = std::str::from_utf8(context).ok()?;
    let (raft, client) = text.split_once(' ')?;
    Some((raft.to_string(), client.to_string()))
}

/// Where to reach every node this one knows of. The configured peers are
/// fixed; members learned from the log come and go with the membership.
struct AddressBook {
    me: NodeId,
    configured: HashMap<NodeId, Peer>,
    /// One queue per peer. The mutex is not for contention, which there
    /// is none of: a `Sender` is `Send` but was not `Sync` until Rust
    /// 1.72, and this crate builds on 1.70. Wrapping each one separately
    /// rather than the map keeps dispatch to one peer off another's path.
    senders: HashMap<NodeId, Mutex<Sender<Message>>>,
    client_addrs: HashMap<NodeId, String>,
    /// The membership index this was last brought up to date with.
    seen: u64,
}

impl AddressBook {
    /// Start a sender for a peer, or leave the one that is there.
    fn connect(&mut self, id: NodeId, raft_addr: String, client_addr: String) {
        if id == self.me || self.senders.contains_key(&id) {
            return;
        }
        let (tx, rx) = mpsc::channel();
        let me = self.me;
        let spawned = thread::Builder::new()
            .name(format!("peer-{id}"))
            .spawn(move || send_to_peer(me, &raft_addr, rx));
        if spawned.is_ok() {
            self.senders.insert(id, Mutex::new(tx));
            self.client_addrs.insert(id, client_addr);
        }
    }

    /// Make the book match a membership: reach every member, and stop
    /// reaching anyone it has dropped who was only known from the log.
    /// Dropping a sender ends its thread.
    fn update(&mut self, members: &[Member]) {
        for member in members {
            if let Some(peer) = self.configured.get(&member.id) {
                let (raft, client) = (peer.raft_addr.clone(), peer.client_addr.clone());
                self.connect(member.id, raft, client);
            } else if let Some((raft, client)) = parse_context(&member.context) {
                self.connect(member.id, raft, client);
            }
        }
        let keep: Vec<NodeId> = members.iter().map(|m| m.id).collect();
        let configured = &self.configured;
        self.senders
            .retain(|id, _| keep.contains(id) || configured.contains_key(id));
        self.client_addrs
            .retain(|id, _| keep.contains(id) || configured.contains_key(id));
    }
}

/// The shared node, plus the signal that something changed.
struct Shared {
    replica: RwLock<Replica>,
    /// Counts changes to the replica. Bumped, and `progress` woken, after
    /// every tick and message, so a client waiting on a write or a read
    /// does not have to poll, and does not hold the replica while it waits.
    changes: Mutex<u64>,
    progress: Condvar,
    addresses: RwLock<AddressBook>,
    proposals: Proposals,
}

/// What a proposal came to: where it landed in the log and in which term,
/// or the reply that says why it did not.
type Proposed = std::result::Result<(u64, u64), Reply>;

/// Group commit for writes.
///
/// Appending to the log means an fsync, and the node's lock is held while
/// it happens, so proposing one write at a time caps a node at one write
/// per fsync however many clients there are. Instead, each write joins a
/// queue, and whoever finds nobody proposing takes the whole queue and
/// proposes it as one batch: one append, one fsync, one message to each
/// follower. Writes that arrive meanwhile queue up for the next batch.
struct Proposals {
    queue: Mutex<Queue>,
    answered: Condvar,
}

#[derive(Default)]
struct Queue {
    waiting: Vec<(u64, Vec<u8>)>,
    answers: HashMap<u64, Proposed>,
    busy: bool,
    next_ticket: u64,
}

impl Proposals {
    fn new() -> Proposals {
        Proposals {
            queue: Mutex::new(Queue::default()),
            answered: Condvar::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Propose `command`, alone or with whatever else is waiting.
    ///
    /// `propose` is called at most once, with the batch this thread ended
    /// up proposing, and must answer every command in it in order. A thread
    /// whose command someone else proposed never calls it.
    fn submit(
        &self,
        command: Vec<u8>,
        propose: impl FnOnce(Vec<Vec<u8>>) -> Vec<Proposed>,
    ) -> Proposed {
        let mut queue = self.lock();
        let ticket = queue.next_ticket;
        queue.next_ticket += 1;
        queue.waiting.push((ticket, command));

        let mut propose = Some(propose);
        loop {
            if let Some(answer) = queue.answers.remove(&ticket) {
                return answer;
            }
            if !queue.busy {
                // Nobody is proposing, so this command has not been taken,
                // and is in the batch this thread is about to propose.
                queue.busy = true;
                let (tickets, commands): (Vec<u64>, Vec<Vec<u8>>) =
                    std::mem::take(&mut queue.waiting).into_iter().unzip();
                drop(queue);
                let propose = propose.take().expect("a thread proposes at most once");
                let answers = propose(commands);
                queue = self.lock();
                queue.busy = false;
                queue.answers.extend(tickets.into_iter().zip(answers));
                self.answered.notify_all();
                continue;
            }
            queue = self.answered.wait(queue).unwrap_or_else(|e| e.into_inner());
        }
    }
}

impl Shared {
    // A panicking command leaves the store's own invariants intact, so
    // there is no reason to take the rest of the cluster down with it.
    fn lock(&self) -> RwLockWriteGuard<'_, Replica> {
        self.replica.write().unwrap_or_else(|e| e.into_inner())
    }

    fn read(&self) -> RwLockReadGuard<'_, Replica> {
        self.replica.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Say that the replica has changed, to anyone waiting on it.
    fn changed(&self) {
        let mut changes = self.changes.lock().unwrap_or_else(|e| e.into_inner());
        *changes += 1;
        self.progress.notify_all();
    }

    /// Wait until `check` has an answer, or `deadline` passes.
    ///
    /// `check` runs under the shared lock, so several waiters check at
    /// once. The change count is read before checking and compared after,
    /// which is what makes this free of lost wakeups: a change made after
    /// the check bumps the count, and the wait sees it and checks again
    /// instead of sleeping through it.
    fn wait_for<T>(
        &self,
        deadline: Instant,
        mut check: impl FnMut(&Replica) -> Option<T>,
    ) -> Option<T> {
        loop {
            let seen = *self.changes.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(answer) = check(&self.read()) {
                return Some(answer);
            }
            let mut changes = self.changes.lock().unwrap_or_else(|e| e.into_inner());
            while *changes == seen {
                let remaining = deadline.checked_duration_since(Instant::now())?;
                changes = self
                    .progress
                    .wait_timeout(changes, remaining)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        }
    }

    fn addresses(&self) -> RwLockReadGuard<'_, AddressBook> {
        self.addresses.read().unwrap_or_else(|e| e.into_inner())
    }

    fn client_addr(&self, id: NodeId) -> Option<String> {
        self.addresses().client_addrs.get(&id).cloned()
    }

    /// Hand a node's outgoing messages to the per-peer queues.
    fn dispatch(&self, actions: Vec<Action>) {
        let book = self.addresses();
        for Action::Send { to, message } in actions {
            if let Some(sender) = book.senders.get(&to) {
                // A full or dead queue is not an error worth reporting:
                // Raft resends on the next heartbeat.
                let sender = sender.lock().unwrap_or_else(|e| e.into_inner());
                let _ = sender.send(message);
            }
        }
    }

    /// If the membership has moved since the address book last looked,
    /// bring the book up to date. Called with the replica in hand, so the
    /// membership cannot move again while this reads it.
    fn follow_membership(&self, replica: &Replica) {
        let index = replica.node().membership_index();
        if self.addresses().seen == index {
            return;
        }
        let mut book = self.addresses.write().unwrap_or_else(|e| e.into_inner());
        book.update(replica.node().members());
        book.seen = index;
    }

    fn step(&self, from: NodeId, message: Message) {
        let actions = {
            let mut replica = self.lock();
            let actions = match replica.step(from, message) {
                Ok(actions) => actions,
                Err(e) => storage_failed(&e),
            };
            self.follow_membership(&replica);
            actions
        };
        self.changed();
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
        // The largest message a leader sends is one batch plus, at worst,
        // one oversized entry. If that could pass the frame limit a peer
        // would refuse it forever, which is the failure the budget exists
        // to prevent, so refuse the configuration instead.
        let worst = config.raft.max_append_bytes as u64 + config.raft.max_entry_bytes as u64;
        if worst + 1024 > wire::MAX_FRAME as u64 {
            return Err(crate::Error::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_append_bytes plus max_entry_bytes must stay under the frame limit",
            )));
        }
        let raft_listener = TcpListener::bind(raft_addr)?;
        let client_listener = TcpListener::bind(client_addr)?;

        // A new cluster's first membership is this node and its peers. A
        // node joining one that exists has none until a leader adds it.
        let bootstrap = if config.join {
            Vec::new()
        } else {
            let mut members: Vec<Member> = config
                .peers
                .iter()
                .map(|p| Member {
                    id: p.id,
                    context: member_context(&p.raft_addr, &p.client_addr),
                    learner: false,
                })
                .collect();
            members.push(Member {
                id: config.id,
                context: member_context(
                    &raft_listener.local_addr()?.to_string(),
                    &client_listener.local_addr()?.to_string(),
                ),
                learner: false,
            });
            members.sort_unstable_by_key(|m| m.id);
            members
        };

        let storage = DiskStorage::open(raft_dir)?;
        let store = Store::open(store_dir)?;
        let node = Node::with_members(config.id, bootstrap, config.raft, storage);
        let mut replica = ReplicatedStore::new(node, store)?;
        replica.set_snapshot_every(config.snapshot_every);
        replica.set_background_snapshots(true);

        let mut book = AddressBook {
            me: config.id,
            configured: config.peers.iter().map(|p| (p.id, p.clone())).collect(),
            senders: HashMap::new(),
            client_addrs: HashMap::new(),
            seen: replica.node().membership_index(),
        };
        for peer in &config.peers {
            book.connect(peer.id, peer.raft_addr.clone(), peer.client_addr.clone());
        }
        book.update(replica.node().members());

        Ok(ClusterNode {
            shared: Arc::new(Shared {
                replica: RwLock::new(replica),
                changes: Mutex::new(0),
                progress: Condvar::new(),
                addresses: RwLock::new(book),
                proposals: Proposals::new(),
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
        let (actions, job, compaction) = {
            let mut replica = shared.lock();
            let actions = match replica.tick() {
                Ok(actions) => actions,
                Err(e) => storage_failed(&e),
            };
            shared.follow_membership(&replica);
            let compaction = match replica.start_compaction() {
                Ok(job) => job,
                Err(e) => {
                    eprintln!("minicask-cluster: could not start compacting the store: {e}");
                    None
                }
            };
            let job = match replica.start_snapshot() {
                Ok(job) => job,
                Err(e) => {
                    // A snapshot that cannot be started costs a longer log,
                    // not correctness. Try again once more has been applied.
                    eprintln!("minicask-cluster: could not start a snapshot: {e}");
                    None
                }
            };
            (actions, job, compaction)
        };
        shared.changed();
        shared.dispatch(actions);

        if let Some(job) = compaction {
            let worker = Arc::clone(shared);
            let spawned = thread::Builder::new()
                .name("compaction".to_string())
                .spawn(move || compact_store(&worker, job));
            if spawned.is_err() {
                shared.lock().abandon_compaction();
            }
        }

        if let Some(job) = job {
            let writer = Arc::clone(shared);
            let spawned = thread::Builder::new()
                .name("snapshot".to_string())
                .spawn(move || write_snapshot(&writer, job));
            if spawned.is_err() {
                // The job went down with the thread that never started.
                shared.lock().abandon_snapshot();
            }
        }
    }
}

/// Write a snapshot without the lock, then take it just long enough to
/// install the result.
fn write_snapshot(shared: &Shared, job: crate::replicated::SnapshotJob<crate::raft::DiskSnapshot>) {
    let index = job.index();
    let written = job.run();
    let mut replica = shared.lock();
    match written {
        Ok(sink) => {
            if let Err(e) = replica.finish_snapshot(sink) {
                storage_failed(&e);
            }
        }
        Err(e) => {
            eprintln!("minicask-cluster: writing the snapshot at {index} failed: {e}");
            replica.abandon_snapshot();
        }
    }
}

/// Merge the store's files without the lock, then take it just long enough
/// to point the index at the merge.
fn compact_store(shared: &Shared, job: crate::replicated::CompactionJob) {
    let merged = job.run();
    let mut replica = shared.lock();
    match merged {
        Ok(done) => {
            if let Err(e) = replica.finish_compaction(done) {
                storage_failed(&e);
            }
        }
        Err(e) => {
            eprintln!("minicask-cluster: compacting the store failed: {e}");
            replica.abandon_compaction();
        }
    }
}

/// A storage failure means the log could not be made durable, and
/// continuing would mean acknowledging what is not on disk.
fn storage_failed(e: &crate::Error) -> ! {
    eprintln!("minicask-cluster: consensus storage failed: {e}");
    std::process::abort();
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

        // Reads are answered wherever they arrive, leader or follower, and
        // see every write acknowledged before them either way. See `read`.
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
            [key, value] => write(
                shared,
                &Op::Put {
                    key: key.clone(),
                    value: value.clone(),
                },
            ),
            [_, _, ..] => Reply::err("this cluster's SET takes no options"),
            _ => wrong_arity(&name),
        },
        "DEL" => match rest {
            [] => wrong_arity(&name),
            [key] => write(shared, &Op::Delete { key: key.clone() }),
            _ => Reply::err("this cluster's DEL takes one key"),
        },

        // Changing who is in the cluster, one node at a time.
        "RAFT.ADD" => match rest {
            [id, raft_addr, client_addr] => {
                let Some(id) = parse_id(id) else {
                    return Reply::err("RAFT.ADD wants a numeric node id");
                };
                let (Ok(raft_addr), Ok(client_addr)) = (
                    std::str::from_utf8(raft_addr),
                    std::str::from_utf8(client_addr),
                ) else {
                    return Reply::err("addresses must be text");
                };
                let context = member_context(raft_addr, client_addr);
                change_membership(shared, |members| {
                    if members.iter().any(|m| m.id == id) {
                        return Err(format!("node {id} is already a member"));
                    }
                    members.push(Member {
                        id,
                        context,
                        learner: true,
                    });
                    members.sort_unstable_by_key(|m| m.id);
                    Ok(())
                })
            }
            _ => wrong_arity(&name),
        },
        "RAFT.REMOVE" => match rest {
            [id] => {
                let Some(id) = parse_id(id) else {
                    return Reply::err("RAFT.REMOVE wants a numeric node id");
                };
                change_membership(shared, |members| {
                    let before = members.len();
                    members.retain(|m| m.id != id);
                    if members.len() == before {
                        return Err(format!("node {id} is not a member"));
                    }
                    Ok(())
                })
            }
            _ => wrong_arity(&name),
        },

        // Where the cluster stands, which is the first thing anyone asks.
        "RAFT" | "INFO" => {
            let replica = shared.read();
            let node = replica.node();
            Reply::Bulk(
                format!(
                    "id:{}\r\nrole:{}\r\nterm:{}\r\nleader:{}\r\ncommit_index:{}\r\napplied_index:{}\r\nlast_index:{}\r\nsnapshot_index:{}\r\nkeys:{}\r\nmembers:{}\r\nvoters:{}\r\n",
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
                    replica.snapshot_index(),
                    replica.len(),
                    node.members()
                        .iter()
                        .map(|m| m.id.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    node.members()
                        .iter()
                        .filter(|m| !m.learner)
                        .map(|m| m.id.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                )
                .into_bytes(),
            )
        }

        _ => Reply::err(format!("unknown command '{}'", name.to_ascii_lowercase())),
    }
}

/// Run a read that sees every write acknowledged before it, on this node.
///
/// A local store is not enough on its own. A follower's is only as current
/// as the last entry it applied, and even a leader's can be stale if the
/// leader has been cut off and replaced without noticing yet. So the read
/// first gets a read index, from this node if it leads and from the leader
/// if not, which a majority has confirmed; then it waits for the local
/// store to apply that far; then it reads. See [`Node::read_index`].
fn read(shared: &Arc<Shared>, f: impl FnOnce(&Replica) -> Reply) -> Reply {
    let deadline = Instant::now() + COMMIT_TIMEOUT;
    let (request, actions) = shared.lock().read_index();
    shared.dispatch(actions);

    let mut f = Some(f);
    let outcome = shared.wait_for(deadline, |replica| match replica.read_state(&request) {
        ReadState::Pending => None,
        ReadState::Ready(_) => Some(Ok(f.take().expect("answered once")(replica))),
        ReadState::Failed => Some(Err(not_leader(shared, replica))),
    });
    shared.lock().forget_read(&request);
    match outcome {
        Some(Ok(reply)) | Some(Err(reply)) => reply,
        None => Reply::Error(
            "TIMEOUT the read could not be confirmed in time; no majority answered".to_string(),
        ),
    }
}

/// Propose a write, batched with any others, and wait for it to take
/// effect here, which means a majority has it on disk.
fn write(shared: &Arc<Shared>, op: &Op) -> Reply {
    let command = match op.encode() {
        Ok(command) => command,
        Err(e) => return Reply::err(e.to_string()),
    };
    let proposed = shared.proposals.submit(command, |commands| {
        let count = commands.len();
        let (answers, actions) = {
            let mut replica = shared.lock();
            let term = replica.node().term();
            match replica.propose_batch(commands) {
                Ok((results, actions)) => (
                    results
                        .into_iter()
                        .map(|r| {
                            r.map(|index| (index, term))
                                .map_err(|e| Reply::err(e.to_string()))
                        })
                        .collect(),
                    actions,
                ),
                Err(ProposeError::NotLeader { .. }) => {
                    (vec![Err(not_leader(shared, &replica)); count], Vec::new())
                }
                Err(e) => (vec![Err(Reply::err(e.to_string())); count], Vec::new()),
            }
        };
        shared.dispatch(actions);
        answers
    });
    let (index, term) = match proposed {
        Ok(landed) => landed,
        Err(reply) => return reply,
    };

    let deadline = Instant::now() + COMMIT_TIMEOUT;
    let outcome = shared.wait_for(deadline, |replica| {
        if replica.applied_index() >= index {
            return Some(Reply::ok());
        }
        // Losing the term means this proposal may never commit, and may
        // yet be overwritten. Saying so beats waiting out the clock.
        if replica.node().term() != term || !replica.is_leader() {
            return Some(Reply::Error(
                "NOTLEADER leadership was lost before the write committed; its fate is unknown"
                    .to_string(),
            ));
        }
        None
    });
    outcome.unwrap_or_else(|| {
        Reply::Error("TIMEOUT the write did not commit in time; it may still do so".to_string())
    })
}

fn parse_id(bytes: &[u8]) -> Option<NodeId> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// Propose a membership made by editing the current one, and wait for it
/// to commit, which is when it is safe to make the next change.
fn change_membership(
    shared: &Arc<Shared>,
    edit: impl FnOnce(&mut Vec<Member>) -> std::result::Result<(), String>,
) -> Reply {
    let (index, term, actions) = {
        let mut replica = shared.lock();
        let mut members = replica.node().members().to_vec();
        if let Err(why) = edit(&mut members) {
            return Reply::err(why);
        }
        match replica.propose_membership(members) {
            Ok(accepted) => {
                shared.follow_membership(&replica);
                (accepted.index, replica.node().term(), accepted.actions)
            }
            Err(ProposeError::NotLeader { .. }) => return not_leader(shared, &replica),
            Err(e) => return Reply::err(e.to_string()),
        }
    };
    shared.dispatch(actions);

    let deadline = Instant::now() + COMMIT_TIMEOUT;
    let outcome = shared.wait_for(deadline, |replica| {
        if replica.commit_index() >= index {
            return Some(Reply::ok());
        }
        if replica.node().term() != term {
            return Some(Reply::Error(
                "NOTLEADER leadership was lost before the change committed; its fate is unknown"
                    .to_string(),
            ));
        }
        None
    });
    outcome.unwrap_or_else(|| {
        Reply::Error("TIMEOUT the change did not commit in time; it may still do so".to_string())
    })
}

/// Redis clients know `-MOVED addr` from cluster mode, so a redirect in
/// that shape is the one most likely to be understood.
fn not_leader(shared: &Arc<Shared>, replica: &Replica) -> Reply {
    match replica.leader().and_then(|id| shared.client_addr(id)) {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes that arrive while a batch is being proposed go out together
    /// in the next one, rather than one by one behind it.
    #[test]
    fn proposals_that_queue_behind_a_batch_go_out_together() {
        let proposals = Arc::new(Proposals::new());
        let batches = Arc::new(Mutex::new(Vec::<Vec<Vec<u8>>>::new()));
        let answer = |commands: Vec<Vec<u8>>| -> Vec<Proposed> {
            commands.iter().map(|c| Ok((c[0] as u64, 1))).collect()
        };

        let (inside_tx, inside_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let first = {
            let (proposals, batches) = (Arc::clone(&proposals), Arc::clone(&batches));
            thread::spawn(move || {
                proposals.submit(vec![1], |commands| {
                    batches.lock().unwrap().push(commands.clone());
                    inside_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    answer(commands)
                })
            })
        };
        inside_rx.recv().unwrap();

        let later: Vec<_> = [2u8, 3]
            .into_iter()
            .map(|n| {
                let (proposals, batches) = (Arc::clone(&proposals), Arc::clone(&batches));
                thread::spawn(move || {
                    proposals.submit(vec![n], |commands| {
                        batches.lock().unwrap().push(commands.clone());
                        answer(commands)
                    })
                })
            })
            .collect();
        // Both have to be queued behind the first batch. If either goes
        // out on its own instead, they never both queue.
        let deadline = Instant::now() + Duration::from_secs(5);
        while proposals.lock().waiting.len() < 2 {
            assert!(
                Instant::now() < deadline,
                "a second batch was proposed while the first was still in flight"
            );
            thread::yield_now();
        }
        release_tx.send(()).unwrap();

        assert_eq!(first.join().unwrap(), Ok((1, 1)));
        let mut results: Vec<Proposed> = later.into_iter().map(|t| t.join().unwrap()).collect();
        results.sort_by_key(|r| r.as_ref().map(|&(i, _)| i).unwrap_or(0));
        assert_eq!(results, vec![Ok((2, 1)), Ok((3, 1))]);

        let mut batches = batches.lock().unwrap().clone();
        batches[1].sort();
        assert_eq!(batches, vec![vec![vec![1]], vec![vec![2], vec![3]]]);
    }
}

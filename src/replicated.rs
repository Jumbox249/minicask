//! The store, driven by consensus instead of by one process.
//!
//! [`Store`] is a state machine: feed it the same writes in the same order
//! and it lands in the same state. [`crate::raft`] agrees an order across a
//! set of nodes. Bolting the two together is the whole of this module.
//!
//! A write is no longer applied where it arrives. It is proposed, and it
//! takes effect on every node once a majority has it on disk:
//!
//! ```text
//! SET k v  ->  Op::Put -> raft entry -> majority acknowledges
//!                                    -> committed
//!                                    -> store.put on every node
//! ```
//!
//! The node never inspects a command, so the consensus layer stays a
//! consensus layer; encoding and applying live here.
//!
//! So does the snapshot. Every so often the store's contents, as of the
//! last applied index, are written out and handed to the node, which
//! discards the log they cover. A snapshot is a sequence of the store's own
//! records, one per live key, so it is checksummed per record the same way
//! everything else is. A follower too far behind for the leader to send it
//! entries is sent the snapshot instead, and its store is replaced by it.
//!
//! None of that holds the store in memory. A snapshot is written from a
//! [`view`](crate::Store) of the store, a copy of its index as of one
//! applied entry, one value at a time, and it can be written on another
//! thread while this one carries on applying: see [`SnapshotJob`]. A
//! restore reads the snapshot back the same way.

use crate::crc::crc32_parts;
use crate::error::{Error, Result};
use crate::raft::{
    Accepted, Action, Command, Message, Node, NodeId, ProposeError, ReadRequest, ReadState, Role,
    SnapshotSink, Storage,
};
use crate::record::{self, Header, HEADER_LEN};
use crate::store::{Store, StoreView};
use std::io::{Read, Write};
use std::path::Path;

/// Applied entries between snapshots, by default. Small enough that a log
/// never grows far, large enough that the cost of writing out the whole
/// store is spread over plenty of writes.
pub const DEFAULT_SNAPSHOT_EVERY: u64 = 10_000;

/// Where a store records the last log index whose effect it holds. It sits
/// beside the data files, which the store ignores because it does not end
/// in `.log`, so that removing the data removes it too: an index that
/// outlived its data would skip writes the store no longer has.
const APPLIED_FILE: &str = "applied-index";

/// A write, in the form that travels through the log.
///
/// The encoding is the store's own record format, so a command on the wire
/// is checksummed and self-delimiting for free, and a delete is the same
/// tombstone the store already understands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Op {
    pub fn encode(&self) -> Result<Vec<u8>> {
        match self {
            Op::Put { key, value } => record::encode(key, Some(value), record::now_millis()),
            Op::Delete { key } => record::encode(key, None, record::now_millis()),
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Op> {
        if bytes.len() < HEADER_LEN {
            return Err(Error::Corrupt {
                file_id: 0,
                offset: 0,
                detail: "replicated command is shorter than a header",
            });
        }
        let header = Header::decode(bytes[..HEADER_LEN].try_into().expect("checked length"));
        if header.record_len() as usize != bytes.len() {
            return Err(Error::Corrupt {
                file_id: 0,
                offset: 0,
                detail: "replicated command length does not match its header",
            });
        }
        let key_end = HEADER_LEN + header.key_len as usize;
        let key = &bytes[HEADER_LEN..key_end];
        let value = &bytes[key_end..];
        if !header.verify(key, value) {
            return Err(Error::Corrupt {
                file_id: 0,
                offset: 0,
                detail: "checksum mismatch on a replicated command",
            });
        }
        Ok(if header.is_tombstone() {
            Op::Delete { key: key.to_vec() }
        } else {
            Op::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            }
        })
    }
}

/// One node of a replicated store: a consensus node and the store it
/// drives.
///
/// Reads are served from the local store and never go through the log.
/// [`get`](Self::get) reads whatever this node has applied, which on a
/// follower, or on a leader deposed without knowing it yet, can be behind
/// the cluster. For a read that sees every acknowledged write, take a
/// [`read_index`](Self::read_index) first and read once
/// [`read_state`](Self::read_state) says it is ready and the store has
/// applied that far.
pub struct ReplicatedStore<S: Storage> {
    node: Node<S>,
    store: Store,
    applied: u64,
    snapshot_every: u64,
    /// Whether snapshots are left to the caller, via
    /// [`start_snapshot`](Self::start_snapshot), rather than taken inline.
    background_snapshots: bool,
    /// A snapshot job is out and has not been finished or abandoned.
    snapshotting: bool,
}

/// A snapshot being taken: the store as of one applied index, and the sink
/// it is being written into.
///
/// It holds nothing borrowed, so it can be moved to another thread and run
/// there while the replica carries on, which is the point: writing out a
/// large store takes as long as reading all of it, and consensus should
/// not stop for that. Hand the result to
/// [`finish_snapshot`](ReplicatedStore::finish_snapshot).
pub struct SnapshotJob<K: SnapshotSink> {
    view: StoreView,
    sink: K,
}

impl<K: SnapshotSink> SnapshotJob<K> {
    /// The index the snapshot covers.
    pub fn index(&self) -> u64 {
        self.sink.meta().index
    }

    /// Write the snapshot out. Needs no access to the replica.
    pub fn run(self) -> Result<K> {
        let SnapshotJob { view, mut sink } = self;
        write_snapshot(&view, &mut sink)?;
        sink.flush()?;
        Ok(sink)
    }
}

impl<S: Storage> ReplicatedStore<S> {
    /// Join a consensus node to a store.
    ///
    /// The store remembers how far through the log it has applied, so a
    /// restart resumes from there rather than writing every committed
    /// entry into the store a second time.
    ///
    /// If the node holds a snapshot newer than that, the store is brought
    /// up to it here, before anything can read from it. That happens when
    /// a crash came after a snapshot was installed from a leader but before
    /// the store caught up with it, which includes a crash in the middle of
    /// catching up.
    pub fn new(node: Node<S>, store: Store) -> Result<ReplicatedStore<S>> {
        let applied = read_applied(store.dir());
        let mut replica = ReplicatedStore {
            node,
            store,
            applied,
            snapshot_every: DEFAULT_SNAPSHOT_EVERY,
            background_snapshots: false,
            snapshotting: false,
        };
        if replica.snapshot_index() > replica.applied {
            replica.restore()?;
        }
        Ok(replica)
    }

    /// How many applied entries to let accumulate before folding them into
    /// a snapshot. Zero turns snapshots off, and the log grows for ever.
    pub fn set_snapshot_every(&mut self, entries: u64) {
        self.snapshot_every = entries;
    }

    /// Stop taking snapshots inline, as part of applying, and leave it to
    /// the caller to run [`start_snapshot`](Self::start_snapshot) and write
    /// the job out wherever it likes. A server does this so that writing a
    /// snapshot never holds the lock that consensus needs.
    pub fn set_background_snapshots(&mut self, background: bool) {
        self.background_snapshots = background;
    }

    /// Whether enough has been applied since the last snapshot to be worth
    /// another, and none is being taken already.
    pub fn snapshot_due(&self) -> bool {
        self.snapshot_every != 0
            && !self.snapshotting
            && self.applied >= self.snapshot_index() + self.snapshot_every
    }

    /// Begin a snapshot of the store as it stands, if one is due.
    ///
    /// This is the cheap half: the store's index is copied and a sink is
    /// opened. The expensive half is [`SnapshotJob::run`], which reads the
    /// values, and nothing it reads can change underneath it, because
    /// every write from here on appends somewhere new. Until the job is
    /// finished or abandoned no other is started.
    pub fn start_snapshot(&mut self) -> Result<Option<SnapshotJob<S::Sink>>> {
        if !self.snapshot_due() {
            return Ok(None);
        }
        let Some(sink) = self.node.begin_compaction(self.applied)? else {
            return Ok(None);
        };
        // The store was synced up to `applied` as the last entry was
        // applied, which matters: once the log is discarded, the snapshot
        // and the store are the only record of it.
        let view = self.store.view()?;
        self.snapshotting = true;
        Ok(Some(SnapshotJob { view, sink }))
    }

    /// Install a snapshot a job has written, and discard the log it covers.
    ///
    /// If the node installed a newer snapshot from its leader while the job
    /// ran, this one is simply dropped.
    pub fn finish_snapshot(&mut self, sink: S::Sink) -> Result<()> {
        self.snapshotting = false;
        if self.node.finish_compaction(sink)? {
            // The store grows by an append for every write, and in a
            // cluster nothing else ever compacts it. The moment the log is
            // compacted is the natural moment to compact the store too.
            if self.store.stats().fragmentation() > 0.5 {
                self.store.compact()?;
            }
        }
        Ok(())
    }

    /// Give up on a job that failed, so that another can be started.
    pub fn abandon_snapshot(&mut self) {
        self.snapshotting = false;
    }

    /// The last index covered by the node's current snapshot, or 0.
    pub fn snapshot_index(&self) -> u64 {
        self.node.storage().snapshot_meta().index
    }

    pub fn node(&self) -> &Node<S> {
        &self.node
    }

    pub fn id(&self) -> NodeId {
        self.node.id()
    }

    pub fn role(&self) -> Role {
        self.node.role()
    }

    pub fn is_leader(&self) -> bool {
        self.node.is_leader()
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.node.leader()
    }

    /// The highest log index whose effect is in the store.
    pub fn applied_index(&self) -> u64 {
        self.applied
    }

    pub fn commit_index(&self) -> u64 {
        self.node.commit_index()
    }

    /// Whether this node may answer a read. See [`Node::ready_to_serve`];
    /// the store is applied up to the commit index whenever this is
    /// consulted, because applying happens inside `tick` and `step`.
    pub fn ready_to_serve(&self) -> bool {
        self.node.ready_to_serve()
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Take the two halves back apart.
    pub fn into_parts(self) -> (S, Store) {
        (self.node.into_storage(), self.store)
    }

    pub fn tick(&mut self) -> Result<Vec<Action>> {
        let actions = self.node.tick()?;
        self.apply()?;
        Ok(actions)
    }

    pub fn step(&mut self, from: NodeId, message: Message) -> Result<Vec<Action>> {
        let actions = self.node.step(from, message)?;
        self.apply()?;
        Ok(actions)
    }

    /// Stand for election now. See [`Node::campaign`].
    pub fn campaign(&mut self) -> Result<Vec<Action>> {
        self.node.campaign()
    }

    /// Propose a write. The returned index has taken effect once
    /// [`applied_index`](Self::applied_index) reaches it.
    pub fn propose(&mut self, op: &Op) -> std::result::Result<Accepted, ProposeError> {
        let bytes = op.encode()?;
        self.node.propose(bytes)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> std::result::Result<Accepted, ProposeError> {
        self.propose(&Op::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        })
    }

    pub fn delete(&mut self, key: &[u8]) -> std::result::Result<Accepted, ProposeError> {
        self.propose(&Op::Delete { key: key.to_vec() })
    }

    /// Start a read that sees every write acknowledged before it. See
    /// [`Node::read_index`].
    pub fn read_index(&mut self) -> (ReadRequest, Vec<Action>) {
        self.node.read_index()
    }

    /// Where a read stands, from the store's side. [`ReadState::Ready`]
    /// means it can be answered from this store now: the node has confirmed
    /// it, and the store has applied as far as the read has to see. A
    /// follower often has the leader's answer before it has applied that
    /// far, and answering then would miss the very writes the read index
    /// exists to include.
    pub fn read_state(&self, request: &ReadRequest) -> ReadState {
        match self.node.read_state(request) {
            ReadState::Ready(index) if self.applied < index => ReadState::Pending,
            other => other,
        }
    }

    pub fn forget_read(&mut self, request: &ReadRequest) {
        self.node.forget_read(request)
    }

    /// Read from the local store. See the caveat on the type.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.store.get(key)
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.store.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Put every newly committed entry into the store, in log order.
    ///
    /// A command that will not decode is a corrupt log rather than a bad
    /// request: it was checksummed on the way in and agreed by a majority.
    /// Applying the rest of the log around it would leave this node's state
    /// quietly different from everyone else's, so it stops instead.
    fn apply(&mut self) -> Result<()> {
        // A snapshot ahead of the store, which means one arrived from a
        // leader. The entries it covers are gone from this node's log, so
        // there is nothing to apply them from: the store has to become the
        // snapshot before anything after it can be applied.
        if self.snapshot_index() > self.applied {
            self.restore()?;
        }

        let before = self.applied;
        for entry in self.node.take_committed() {
            // A node relearns its commit index from zero after a restart,
            // so everything up to what the store already holds comes past
            // again. Applying it twice would give the same state, but each
            // pass appends the whole history to the store's files again.
            if entry.index <= self.applied {
                continue;
            }
            match &entry.command {
                Command::Noop => {}
                Command::Data(bytes) => match Op::decode(bytes)? {
                    Op::Put { key, value } => self.store.put(&key, &value)?,
                    Op::Delete { key } => {
                        self.store.delete(&key)?;
                    }
                },
            }
            self.applied = entry.index;
        }

        if self.applied > before {
            // The writes first, the claim second. If the index reached disk
            // ahead of the data it describes, a crash in between would leave
            // it pointing past writes the store lost, and they would be
            // skipped for good. This matters when the store trades fsyncs
            // for speed; with every write synced it costs one cheap call.
            self.store.sync()?;
            write_applied(self.store.dir(), self.applied)?;
        }

        if !self.background_snapshots {
            if let Some(job) = self.start_snapshot()? {
                let sink = job.run()?;
                self.finish_snapshot(sink)?;
            }
        }
        Ok(())
    }

    /// Make the store exactly the node's snapshot.
    ///
    /// Exactly, not merely including it. A key this node still holds but
    /// the cluster deleted during the entries it missed is not in the
    /// snapshot, and writing the snapshot over the store would leave it
    /// there. So everything the snapshot does not have is deleted first.
    ///
    /// Interruptible at any point: the applied index is only moved once
    /// the store is synced, so a crash part way through leaves the snapshot
    /// still ahead of the store, and the next start runs this again. Values
    /// already right are left alone, so running it twice does not grow the
    /// store twice.
    ///
    /// The snapshot is read as a stream, one record at a time, and walked
    /// in step with the store's own keys in sorted order, so what is held
    /// in memory is the store's key list and one value, not the snapshot.
    fn restore(&mut self) -> Result<()> {
        let index = self.snapshot_index();
        let mut keys: Vec<Vec<u8>> = self.store.keys().map(<[u8]>::to_vec).collect();
        keys.sort_unstable();
        let mut held = keys.into_iter().peekable();

        let mut records = SnapshotRecords::new(self.node.storage().snapshot_reader()?);
        while let Some((key, value)) = records.next_record()? {
            // Everything the store holds that sorts before this key is
            // absent from the snapshot, which means the cluster deleted it.
            while let Some(stale) = held.next_if(|k| *k < key) {
                self.store.delete_deferred(&stale)?;
            }
            held.next_if(|k| *k == key);
            if self.store.get(&key)?.as_deref() != Some(value.as_slice()) {
                self.store.put_deferred(&key, &value)?;
            }
        }
        for stale in held {
            self.store.delete_deferred(&stale)?;
        }

        self.store.sync()?;
        self.applied = index;
        write_applied(self.store.dir(), index)
    }
}

/// The store's live contents as a snapshot: one record per key, in key
/// order, so that two identical stores produce identical snapshots, and so
/// that a restore can walk it in step with the store's own sorted keys.
fn write_snapshot(view: &StoreView, out: &mut impl Write) -> Result<()> {
    view.for_each(|key, value| {
        // A timestamp of zero, so the bytes depend on the contents alone.
        out.write_all(&record::encode(key, Some(value), 0)?)?;
        Ok(())
    })
}

/// Reads a snapshot back one record at a time, checking each one.
struct SnapshotRecords<R: Read> {
    reader: R,
    offset: u64,
    previous: Option<Vec<u8>>,
}

impl<R: Read> SnapshotRecords<R> {
    fn new(reader: R) -> Self {
        SnapshotRecords {
            reader,
            offset: 0,
            previous: None,
        }
    }

    /// The next key and value, or `None` at a clean end.
    fn next_record(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let at = self.offset;
        let corrupt = |detail| Error::Corrupt {
            file_id: 0,
            offset: at,
            detail,
        };
        let mut header = [0u8; HEADER_LEN];
        match read_full(&mut self.reader, &mut header)? {
            0 => return Ok(None),
            HEADER_LEN => {}
            _ => return Err(corrupt("snapshot ends inside a record header")),
        }
        let header = Header::decode(&header);
        let mut key = vec![0u8; header.key_len as usize];
        let mut value = vec![0u8; header.value_len as usize];
        if read_full(&mut self.reader, &mut key)? < key.len()
            || read_full(&mut self.reader, &mut value)? < value.len()
        {
            return Err(corrupt("snapshot ends inside a record"));
        }
        if !header.verify(&key, &value) {
            return Err(corrupt("checksum mismatch in a snapshot record"));
        }
        if header.is_tombstone() {
            return Err(corrupt("a snapshot holds live keys, never deletions"));
        }
        // Order is what lets a restore find stale keys without holding the
        // snapshot in memory, so a snapshot out of order is refused rather
        // than half applied.
        if self.previous.as_ref().is_some_and(|p| *p >= key) {
            return Err(corrupt("snapshot keys are not in order"));
        }
        self.previous = Some(key.clone());
        self.offset += header.record_len();
        Ok(Some((key, value)))
    }
}

/// Fill `buf` as far as the stream allows, and say how far that was.
fn read_full(reader: &mut impl Read, buf: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(filled)
}

/// The applied index, or 0 if there is none to be had.
///
/// Every failure falls back to 0, and that is always safe: it means
/// replaying every committed write in order onto a store that already has
/// some of them, which lands in the same state. The file only ever saves
/// work, so there is nothing to be gained by refusing to start over it.
fn read_applied(dir: &Path) -> u64 {
    let Ok(bytes) = std::fs::read(dir.join(APPLIED_FILE)) else {
        return 0;
    };
    let Ok(bytes) = <[u8; 12]>::try_from(bytes.as_slice()) else {
        return 0;
    };
    let crc = u32::from_le_bytes(bytes[0..4].try_into().expect("four bytes"));
    if crc32_parts(&[&bytes[4..]]) != crc {
        return 0;
    }
    u64::from_le_bytes(bytes[4..12].try_into().expect("eight bytes"))
}

fn write_applied(dir: &Path, index: u64) -> Result<()> {
    let body = index.to_le_bytes();
    let mut buf = [0u8; 12];
    buf[0..4].copy_from_slice(&crc32_parts(&[&body]).to_le_bytes());
    buf[4..].copy_from_slice(&body);
    crate::log::write_atomically(dir, APPLIED_FILE, &buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::StorageExt;
    use std::io::Read;

    fn encode_snapshot(store: &Store) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        write_snapshot(&store.view()?, &mut out)?;
        Ok(out)
    }

    fn decode_snapshot(data: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut records = SnapshotRecords::new(data);
        let mut out = Vec::new();
        while let Some(pair) = records.next_record()? {
            out.push(pair);
        }
        Ok(out)
    }

    fn temp_store(label: &str) -> (std::path::PathBuf, Store) {
        let path = std::env::temp_dir().join(format!(
            "minicask-snapshot-{label}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&path);
        let store = Store::open(&path).unwrap();
        (path, store)
    }

    #[test]
    fn a_snapshot_round_trips_the_live_keys_only() {
        let (path, mut store) = temp_store("round-trip");
        store.put(b"a", b"1").unwrap();
        store.put(b"b", b"").unwrap();
        store.put(b"gone", b"x").unwrap();
        store.delete(b"gone").unwrap();
        store.put(b"a", b"overwritten").unwrap();

        let pairs = decode_snapshot(&encode_snapshot(&store).unwrap()).unwrap();
        assert_eq!(
            pairs,
            vec![
                (b"a".to_vec(), b"overwritten".to_vec()),
                (b"b".to_vec(), Vec::new()),
            ]
        );
        drop(store);
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn identical_stores_make_identical_snapshots() {
        let (p1, mut one) = temp_store("same-1");
        let (p2, mut two) = temp_store("same-2");
        for (k, v) in [("z", "1"), ("a", "2"), ("m", "3")] {
            one.put(k.as_bytes(), v.as_bytes()).unwrap();
        }
        for (k, v) in [("m", "3"), ("z", "1"), ("a", "2")] {
            two.put(k.as_bytes(), v.as_bytes()).unwrap();
        }
        assert_eq!(
            encode_snapshot(&one).unwrap(),
            encode_snapshot(&two).unwrap()
        );
        drop((one, two));
        let _ = std::fs::remove_dir_all(&p1);
        let _ = std::fs::remove_dir_all(&p2);
    }

    #[test]
    fn a_damaged_snapshot_is_refused() {
        let (path, mut store) = temp_store("damaged");
        store.put(b"key", b"value").unwrap();
        let mut data = encode_snapshot(&store).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0b0000_0100;
        assert!(matches!(decode_snapshot(&data), Err(Error::Corrupt { .. })));
        assert!(matches!(
            decode_snapshot(&data[..data.len() - 2]),
            Err(Error::Corrupt { .. })
        ));
        drop(store);
        let _ = std::fs::remove_dir_all(&path);
    }

    /// A node whose storage holds `snapshot`, joined to `store`, which
    /// makes `new` restore the store to it.
    fn restored(store: Store, snapshot: &[u8]) -> ReplicatedStore<crate::raft::MemStorage> {
        use crate::raft::{Config, MemStorage, SnapshotMeta};
        let mut storage = MemStorage::new();
        storage
            .save_snapshot(SnapshotMeta { index: 5, term: 1 }, snapshot)
            .unwrap();
        let node = Node::new(1, vec![1], Config::default(), storage);
        ReplicatedStore::new(node, store).unwrap()
    }

    #[test]
    fn a_restore_makes_the_store_exactly_the_snapshot() {
        let (path, mut store) = temp_store("restore-exact");
        store.put(b"same", b"1").unwrap();
        store.put(b"changed", b"old").unwrap();
        store.put(b"extra", b"not in the snapshot").unwrap();

        let (path2, mut source) = temp_store("restore-source");
        source.put(b"same", b"1").unwrap();
        source.put(b"changed", b"new").unwrap();
        source.put(b"missing", b"only in the snapshot").unwrap();
        let snapshot = encode_snapshot(&source).unwrap();

        let replica = restored(store, &snapshot);
        assert_eq!(replica.applied_index(), 5);
        assert_eq!(replica.get(b"extra").unwrap(), None);
        assert_eq!(replica.get(b"changed").unwrap(), Some(b"new".to_vec()));
        assert_eq!(
            replica.get(b"missing").unwrap(),
            Some(b"only in the snapshot".to_vec())
        );
        assert_eq!(replica.len(), 3);
        drop((replica, source));
        let _ = std::fs::remove_dir_all(&path);
        let _ = std::fs::remove_dir_all(&path2);
    }

    /// A restore that is interrupted runs again on the next start, and a
    /// node stuck crashing mid-restore runs it again and again. Each run
    /// must only write what actually differs, or every attempt grows the
    /// store by its whole size.
    #[test]
    fn restoring_onto_a_store_that_already_matches_writes_nothing() {
        let (path, mut store) = temp_store("restore-idempotent");
        for i in 0..20u8 {
            store.put(&[b'k', i], &[b'v'; 64]).unwrap();
        }
        let snapshot = encode_snapshot(&store).unwrap();
        let before = store.stats().disk_bytes;

        let replica = restored(store, &snapshot);
        assert_eq!(
            replica.store().stats().disk_bytes,
            before,
            "restoring identical contents rewrote them"
        );
        drop(replica);
        let _ = std::fs::remove_dir_all(&path);
    }

    /// A restore walks the snapshot and the store's sorted keys in step.
    /// Stale keys can sort before the first snapshot key, between two of
    /// them, and after the last, and every one of them has to go.
    #[test]
    fn a_restore_deletes_stale_keys_wherever_they_sort() {
        let (path, mut store) = temp_store("restore-merge");
        for key in ["a", "c", "m", "x", "z"] {
            store.put(key.as_bytes(), b"old").unwrap();
        }
        let (path2, mut source) = temp_store("restore-merge-source");
        for key in ["b", "c", "n", "x"] {
            source.put(key.as_bytes(), key.as_bytes()).unwrap();
        }
        let replica = restored(store, &encode_snapshot(&source).unwrap());

        let mut keys: Vec<Vec<u8>> = replica.store().keys().map(<[u8]>::to_vec).collect();
        keys.sort();
        assert_eq!(keys, [&b"b"[..], b"c", b"n", b"x"]);
        for key in ["b", "c", "n", "x"] {
            assert_eq!(
                replica.get(key.as_bytes()).unwrap().as_deref(),
                Some(key.as_bytes())
            );
        }
        drop((replica, source));
        let _ = std::fs::remove_dir_all(&path);
        let _ = std::fs::remove_dir_all(&path2);
    }

    /// Order is what the restore relies on to find stale keys, so a
    /// snapshot out of order is refused rather than half believed.
    #[test]
    fn a_snapshot_out_of_order_is_refused() {
        let mut data = record::encode(b"b", Some(b"2"), 0).unwrap();
        data.extend_from_slice(&record::encode(b"a", Some(b"1"), 0).unwrap());
        assert!(matches!(decode_snapshot(&data), Err(Error::Corrupt { .. })));
        let mut twice = record::encode(b"a", Some(b"1"), 0).unwrap();
        twice.extend_from_slice(&record::encode(b"a", Some(b"1"), 0).unwrap());
        assert!(matches!(
            decode_snapshot(&twice),
            Err(Error::Corrupt { .. })
        ));
    }

    /// A one-node replica in office, with its no-op applied.
    fn single_node(store: Store) -> ReplicatedStore<crate::raft::MemStorage> {
        use crate::raft::{Config, MemStorage};
        let node = Node::new(1, vec![1], Config::default(), MemStorage::new());
        let mut replica = ReplicatedStore::new(node, store).unwrap();
        while !replica.ready_to_serve() {
            replica.tick().unwrap();
        }
        replica.tick().unwrap();
        replica
    }

    fn put_and_apply(
        replica: &mut ReplicatedStore<crate::raft::MemStorage>,
        key: &str,
        value: &str,
    ) {
        replica.put(key.as_bytes(), value.as_bytes()).unwrap();
        replica.tick().unwrap();
    }

    /// The point of a job is that the replica carries on while it runs, so
    /// what it writes has to be the store as of its index, not the store as
    /// it has become by the time each value is read.
    #[test]
    fn a_background_snapshot_is_the_store_as_of_its_index() {
        let (path, store) = temp_store("background");
        let mut replica = single_node(store);
        replica.set_background_snapshots(true);
        replica.set_snapshot_every(3);

        for (k, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
            put_and_apply(&mut replica, k, v);
        }
        assert!(replica.snapshot_due());
        assert_eq!(replica.snapshot_index(), 0, "inline snapshots are off");
        let job = replica.start_snapshot().unwrap().expect("one is due");
        let index = job.index();
        assert_eq!(index, replica.applied_index());
        assert!(!replica.snapshot_due(), "one is already being taken");

        // The replica moves on underneath the job.
        put_and_apply(&mut replica, "a", "overwritten");
        replica.delete(b"b").unwrap();
        replica.tick().unwrap();
        put_and_apply(&mut replica, "d", "new");

        let sink = job.run().unwrap();
        replica.finish_snapshot(sink).unwrap();
        assert_eq!(replica.snapshot_index(), index);

        let mut data = Vec::new();
        replica
            .node()
            .storage()
            .snapshot_reader()
            .unwrap()
            .read_to_end(&mut data)
            .unwrap();
        assert_eq!(
            decode_snapshot(&data).unwrap(),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ]
        );
        // And the store itself has carried on regardless.
        assert_eq!(replica.get(b"a").unwrap(), Some(b"overwritten".to_vec()));
        assert_eq!(replica.get(b"b").unwrap(), None);
        drop(replica);
        let _ = std::fs::remove_dir_all(&path);
    }

    /// A follower gets the leader's answer to a read before it has applied
    /// the writes that answer covers, whenever the answer outruns the
    /// commit index. The read has to wait for the store, not just for the
    /// answer.
    #[test]
    fn a_read_waits_for_the_store_as_well_as_the_leader() {
        use crate::raft::{Config, Entry, MemStorage};
        let (path, store) = temp_store("read-waits");
        let node = Node::new(1, vec![1, 2, 3], Config::default(), MemStorage::new());
        let mut replica = ReplicatedStore::new(node, store).unwrap();

        let put = Op::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let append = |leader_commit| Message::AppendEntries {
            term: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![
                Entry {
                    term: 1,
                    index: 1,
                    command: Command::Noop,
                },
                Entry {
                    term: 1,
                    index: 2,
                    command: Command::Data(put.encode().unwrap()),
                },
            ],
            leader_commit,
            seq: 1,
        };
        // The entries arrive, but not yet the news that they committed.
        replica.step(2, append(0)).unwrap();
        assert_eq!(replica.applied_index(), 0);

        let (read, actions) = replica.read_index();
        let id = match actions.as_slice() {
            [Action::Send {
                message: Message::ReadIndex { id, .. },
                ..
            }] => *id,
            other => panic!("expected the question to go to the leader: {other:?}"),
        };
        replica
            .step(
                2,
                Message::ReadIndexReply {
                    term: 1,
                    id,
                    index: Some(2),
                },
            )
            .unwrap();
        assert_eq!(
            replica.read_state(&read),
            ReadState::Pending,
            "the read was allowed before the store had the write it must see"
        );
        assert_eq!(replica.get(b"k").unwrap(), None);

        replica.step(2, append(2)).unwrap();
        assert_eq!(replica.read_state(&read), ReadState::Ready(2));
        assert_eq!(replica.get(b"k").unwrap(), Some(b"v".to_vec()));
        drop(replica);
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn a_put_round_trips() {
        let op = Op::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        assert_eq!(Op::decode(&op.encode().unwrap()).unwrap(), op);
    }

    #[test]
    fn a_delete_round_trips() {
        let op = Op::Delete {
            key: b"gone".to_vec(),
        };
        assert_eq!(Op::decode(&op.encode().unwrap()).unwrap(), op);
    }

    #[test]
    fn an_empty_value_is_not_a_delete() {
        let op = Op::Put {
            key: b"k".to_vec(),
            value: Vec::new(),
        };
        assert_eq!(Op::decode(&op.encode().unwrap()).unwrap(), op);
    }

    #[test]
    fn binary_keys_and_values_survive() {
        let op = Op::Put {
            key: b"\x00\xff\r\n".to_vec(),
            value: b"\r\n\x00value".to_vec(),
        };
        assert_eq!(Op::decode(&op.encode().unwrap()).unwrap(), op);
    }

    #[test]
    fn a_damaged_command_is_refused() {
        let op = Op::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };
        let mut bytes = op.encode().unwrap();
        bytes[HEADER_LEN + 1] ^= 0b0010_0000;
        assert!(
            matches!(Op::decode(&bytes), Err(Error::Corrupt { .. })),
            "a flipped bit in a command must not be applied"
        );
    }

    #[test]
    fn a_truncated_command_is_refused() {
        let op = Op::Delete {
            key: b"key".to_vec(),
        };
        let bytes = op.encode().unwrap();
        assert!(matches!(
            Op::decode(&bytes[..bytes.len() - 1]),
            Err(Error::Corrupt { .. })
        ));
        assert!(matches!(Op::decode(&[]), Err(Error::Corrupt { .. })));
    }
}

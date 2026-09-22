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

use crate::crc::crc32_parts;
use crate::error::{Error, Result};
use crate::raft::{Accepted, Action, Command, Message, Node, NodeId, ProposeError, Role, Storage};
use crate::record::{self, Header, HEADER_LEN};
use crate::store::Store;
use std::collections::HashMap;
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
/// Reads are served from the local store and are not put through the log,
/// which makes them fast and leaves one caveat worth stating: a leader that
/// has been deposed without hearing about it yet can answer a read with a
/// value that is one write out of date. Closing that needs a read barrier,
/// which is on the list rather than in the code.
pub struct ReplicatedStore<S: Storage> {
    node: Node<S>,
    store: Store,
    applied: u64,
    snapshot_every: u64,
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

        self.maybe_snapshot()
    }

    /// Fold the applied log into a snapshot once enough of it has built up.
    ///
    /// The store has already been synced up to `applied` by the time this
    /// runs, which matters: once the log is discarded, the snapshot and the
    /// store are the only record of it.
    fn maybe_snapshot(&mut self) -> Result<()> {
        if self.snapshot_every == 0 {
            return Ok(());
        }
        if self.applied < self.snapshot_index() + self.snapshot_every {
            return Ok(());
        }
        let data = encode_snapshot(&self.store)?;
        if self.node.compact(self.applied, &data)? {
            // The store grows by an append for every write, and in a
            // cluster nothing else ever compacts it. The moment the log is
            // compacted is the natural moment to compact the store too.
            if self.store.stats().fragmentation() > 0.5 {
                self.store.compact()?;
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
    fn restore(&mut self) -> Result<()> {
        let index = self.snapshot_index();
        let len = self.node.storage().snapshot_len();
        let Ok(len) = usize::try_from(len) else {
            return Err(Error::Corrupt {
                file_id: 0,
                offset: 0,
                detail: "snapshot is larger than this machine can address",
            });
        };
        let data = self.node.storage().read_snapshot(0, len)?;
        let wanted = decode_snapshot(&data)?;

        let stale: Vec<Vec<u8>> = self
            .store
            .keys()
            .filter(|key| !wanted.contains_key(*key))
            .map(<[u8]>::to_vec)
            .collect();
        for key in stale {
            self.store.delete(&key)?;
        }
        for (key, value) in &wanted {
            if self.store.get(key)?.as_deref() != Some(value.as_slice()) {
                self.store.put(key, value)?;
            }
        }

        self.store.sync()?;
        self.applied = index;
        write_applied(self.store.dir(), index)
    }
}

/// The store's live contents as a snapshot: one record per key, in key
/// order, so that two identical stores produce identical snapshots.
///
/// Built in memory, which is the price of simplicity here: taking or
/// restoring a snapshot needs room for the store's whole contents at once.
fn encode_snapshot(store: &Store) -> Result<Vec<u8>> {
    let mut keys: Vec<&[u8]> = store.keys().collect();
    keys.sort_unstable();
    let mut out = Vec::new();
    for key in keys {
        let value = store.get(key)?.ok_or(Error::Corrupt {
            file_id: 0,
            offset: 0,
            detail: "a key vanished while the snapshot was being taken",
        })?;
        // A timestamp of zero, so the bytes depend on the contents alone.
        out.extend_from_slice(&record::encode(key, Some(&value), 0)?);
    }
    Ok(out)
}

fn decode_snapshot(data: &[u8]) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
    let corrupt = |offset: usize, detail| Error::Corrupt {
        file_id: 0,
        offset: offset as u64,
        detail,
    };
    let mut out = HashMap::new();
    let mut at = 0usize;
    while at < data.len() {
        if data.len() - at < HEADER_LEN {
            return Err(corrupt(at, "snapshot ends inside a record header"));
        }
        let header = Header::decode(
            data[at..at + HEADER_LEN]
                .try_into()
                .expect("checked length"),
        );
        let Ok(len) = usize::try_from(header.record_len()) else {
            return Err(corrupt(at, "snapshot record is too large"));
        };
        if len > data.len() - at {
            return Err(corrupt(at, "snapshot ends inside a record"));
        }
        let key_end = at + HEADER_LEN + header.key_len as usize;
        let key = &data[at + HEADER_LEN..key_end];
        let value = &data[key_end..at + len];
        if !header.verify(key, value) {
            return Err(corrupt(at, "checksum mismatch in a snapshot record"));
        }
        if header.is_tombstone() {
            return Err(corrupt(at, "a snapshot holds live keys, never deletions"));
        }
        out.insert(key.to_vec(), value.to_vec());
        at += len;
    }
    Ok(out)
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

        let decoded = decode_snapshot(&encode_snapshot(&store).unwrap()).unwrap();
        let mut pairs: Vec<_> = decoded.into_iter().collect();
        pairs.sort();
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
        use crate::raft::{Config, MemStorage, SnapshotMeta, Storage};
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

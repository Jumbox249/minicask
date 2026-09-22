//! The replicated log, and the state Raft requires to survive a restart.
//!
//! Raft's safety argument rests on three things being on disk before a node
//! answers any RPC: the current term, the vote it cast in that term, and the
//! log entries it has acknowledged. A node that forgets its vote can vote
//! twice in one term and elect two leaders. A node that forgets an entry it
//! acknowledged can let a committed entry disappear.
//!
//! So persistence is a trait rather than a detail hidden inside the node.
//! [`MemStorage`] keeps it in memory, which is enough for the tests to model
//! a crash (drop the node, keep the storage). [`DiskStorage`] is what a
//! real node uses.
//!
//! A log does not have to start at index 1. Once a prefix of it has been
//! folded into a snapshot it is discarded, and the log begins just after
//! the snapshot, whose last index and term stand in for the entries that
//! are gone. With no snapshot that stand-in is index 0 at term 0, which is
//! the same sentinel the very first `AppendEntries` matches against, so the
//! two cases are one case.
//!
//! [`DiskStorage`]: super::DiskStorage

use crate::error::Result;

/// Which node in the cluster. Small integers, stable across restarts.
pub type NodeId = u64;

/// What a log entry carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// The blank entry a leader appends when it takes office.
    ///
    /// A leader may not count an entry from an earlier term as committed
    /// just because it sits on a majority of logs; the paper's figure 8
    /// shows how that loses data. Committing one entry from its own term
    /// carries every earlier entry with it, so a new leader appends this
    /// immediately and the backlog becomes committable at once.
    Noop,
    /// An opaque command for the state machine on top. The consensus layer
    /// never looks inside it.
    Data(Vec<u8>),
}

/// One entry in the replicated log. The index is stored alongside the term
/// so an entry is self-describing once it has been handed to a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub command: Command,
}

impl Entry {
    /// Bytes this entry takes up inside an `AppendEntries` frame: term,
    /// index, a tag and a length, then the command. Used to keep one
    /// message under a size budget.
    pub fn encoded_len(&self) -> usize {
        let payload = match &self.command {
            Command::Noop => 0,
            Command::Data(bytes) => bytes.len(),
        };
        ENTRY_OVERHEAD + payload
    }
}

/// Fixed bytes per entry on the wire: term, index, tag, length.
pub const ENTRY_OVERHEAD: usize = 8 + 8 + 1 + 8;

/// The part of a node's state that must reach disk before it replies to
/// anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HardState {
    pub term: u64,
    pub voted_for: Option<NodeId>,
}

/// What a snapshot covers: the index and term of the last entry folded
/// into it. The log resumes at `index + 1`.
///
/// The default, zero and zero, means there is no snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotMeta {
    pub index: u64,
    pub term: u64,
}

/// Durable storage for one node's consensus state.
///
/// Indices are 1-based, and everything at or below the snapshot's index has
/// been discarded except the snapshot's own index and term.
pub trait Storage {
    fn hard_state(&self) -> HardState;

    /// Must not return until the state is durable.
    fn save_hard_state(&mut self, state: HardState) -> Result<()>;

    /// The highest index in the log. When nothing follows the snapshot,
    /// that is the snapshot's index, and 0 when there is neither.
    fn last_index(&self) -> u64;

    /// The term of the entry at `index`. The snapshot's index answers with
    /// the snapshot's term; anything below it has been discarded and
    /// answers `None`, as does anything past the end.
    fn term_at(&self, index: u64) -> Option<u64>;

    /// An entry still held in the log. Never one folded into a snapshot.
    fn entry(&self, index: u64) -> Option<&Entry>;

    /// Every entry still held from `index` onwards.
    fn entries_from(&self, index: u64) -> Vec<Entry>;

    /// Append entries to the end of the log. Must not return until they are
    /// durable.
    fn append(&mut self, entries: &[Entry]) -> Result<()>;

    /// Discard the entry at `index` and everything after it. Only ever
    /// called on entries that conflict with the leader's log, which by
    /// Raft's rules cannot have been committed, and so cannot be in a
    /// snapshot either.
    fn truncate_from(&mut self, index: u64) -> Result<()>;

    /// What the current snapshot covers.
    fn snapshot_meta(&self) -> SnapshotMeta;

    /// The size of the current snapshot's data in bytes.
    fn snapshot_len(&self) -> u64;

    /// Up to `len` bytes of the snapshot's data, starting at `offset`.
    fn read_snapshot(&self, offset: u64, len: usize) -> Result<Vec<u8>>;

    /// Replace the snapshot with a newer one and drop the entries it
    /// covers. Must not return until durable.
    ///
    /// If the log holds the snapshot's last entry, at the same term, the
    /// entries after it are kept: they agree with whoever produced the
    /// snapshot. Otherwise the whole log is discarded, because it has
    /// diverged from the snapshot and none of it can be trusted.
    ///
    /// A snapshot no newer than the current one changes nothing.
    fn save_snapshot(&mut self, meta: SnapshotMeta, data: &[u8]) -> Result<()>;
}

/// Convenience queries that every `Storage` gets for free.
pub trait StorageExt: Storage {
    /// The first index still held as an entry.
    fn first_index(&self) -> u64 {
        self.snapshot_meta().index + 1
    }

    fn last_term(&self) -> u64 {
        self.term_at(self.last_index()).unwrap_or(0)
    }

    /// Whether a candidate with this log is at least as up to date as ours,
    /// which is the test that decides a vote: a longer log loses to one
    /// that ends in a later term.
    fn is_up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        let my_term = self.last_term();
        last_term > my_term || (last_term == my_term && last_index >= self.last_index())
    }

    /// The first index still held that belongs to `term`, used to tell a
    /// leader how far back to rewind after a mismatch.
    ///
    /// A term that began inside the snapshot reports the first entry after
    /// it. That can only be too late, never too early, and a hint that is
    /// too late costs a round trip rather than correctness.
    fn first_index_of_term(&self, term: u64) -> u64 {
        let first = self.first_index();
        (first..=self.last_index())
            .find(|&i| self.term_at(i) == Some(term))
            .unwrap_or(first)
    }

    /// Entries from `index` onwards, stopping before the total would pass
    /// `max_bytes`.
    ///
    /// At least one entry is always returned when there is one, however
    /// large, or a single entry bigger than the budget would stall
    /// replication for good.
    fn entries_within(&self, index: u64, max_bytes: usize) -> Vec<Entry> {
        let mut out = Vec::new();
        let mut total = 0usize;
        for i in index.max(self.first_index())..=self.last_index() {
            let Some(entry) = self.entry(i) else { break };
            let len = entry.encoded_len();
            if !out.is_empty() && total + len > max_bytes {
                break;
            }
            total += len;
            out.push(entry.clone());
        }
        out
    }

    /// The last index belonging to `term`, or `None` if the log has no
    /// entry from it. The snapshot's own index counts.
    fn last_index_of_term(&self, term: u64) -> Option<u64> {
        (self.snapshot_meta().index..=self.last_index())
            .rev()
            .find(|&i| i > 0 && self.term_at(i) == Some(term))
    }
}

impl<S: Storage + ?Sized> StorageExt for S {}

/// The in-memory part of a log: the entries after the snapshot, and the
/// snapshot's index and term standing in for everything before them.
///
/// Both storages keep one of these. Keeping the index arithmetic in one
/// place is the point: an off-by-one between two copies of it is exactly
/// the kind of bug a consensus log cannot afford.
#[derive(Debug, Clone, Default)]
pub(crate) struct EntryLog {
    base: SnapshotMeta,
    entries: Vec<Entry>,
}

impl EntryLog {
    pub(crate) fn new(base: SnapshotMeta, entries: Vec<Entry>) -> EntryLog {
        debug_assert!(
            entries
                .iter()
                .enumerate()
                .all(|(i, e)| e.index == base.index + 1 + i as u64),
            "entries must follow the snapshot with no gaps"
        );
        EntryLog { base, entries }
    }

    pub(crate) fn base(&self) -> SnapshotMeta {
        self.base
    }

    pub(crate) fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub(crate) fn last_index(&self) -> u64 {
        self.entries.last().map_or(self.base.index, |e| e.index)
    }

    /// Where `index` sits in `entries`, if it is held at all.
    pub(crate) fn position(&self, index: u64) -> Option<usize> {
        if index <= self.base.index || index > self.last_index() {
            return None;
        }
        Some((index - self.base.index - 1) as usize)
    }

    pub(crate) fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.base.index {
            return Some(self.base.term);
        }
        self.entry(index).map(|e| e.term)
    }

    pub(crate) fn entry(&self, index: u64) -> Option<&Entry> {
        self.position(index).map(|p| &self.entries[p])
    }

    pub(crate) fn entries_from(&self, index: u64) -> Vec<Entry> {
        let start = index.max(self.base.index + 1);
        match self.position(start) {
            Some(p) => self.entries[p..].to_vec(),
            None => Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, entry: Entry) {
        debug_assert_eq!(
            entry.index,
            self.last_index() + 1,
            "entries must be appended in order with no gaps"
        );
        self.entries.push(entry);
    }

    /// Drop `index` and everything after it. Returns how many entries are
    /// left, which is also the position the next entry will occupy.
    pub(crate) fn truncate_from(&mut self, index: u64) -> usize {
        let keep = if index <= self.base.index {
            0
        } else {
            ((index - self.base.index - 1) as usize).min(self.entries.len())
        };
        self.entries.truncate(keep);
        keep
    }

    /// Fold everything up to `meta.index` into a snapshot, keeping what
    /// follows only if it agrees with the snapshot. See
    /// [`Storage::save_snapshot`]. Returns false when `meta` is no newer
    /// than what is already here.
    pub(crate) fn compact(&mut self, meta: SnapshotMeta) -> bool {
        if meta.index <= self.base.index {
            return false;
        }
        let kept = if self.term_at(meta.index) == Some(meta.term) {
            let from = (meta.index - self.base.index) as usize;
            self.entries.split_off(from)
        } else {
            Vec::new()
        };
        self.base = meta;
        self.entries = kept;
        true
    }
}

/// An in-memory `Storage`. Surviving a process restart is not something it
/// can do, but surviving a *node* restart is: the test harness drops the
/// node and keeps the storage, which is exactly what a real crash leaves
/// behind.
#[derive(Debug, Clone, Default)]
pub struct MemStorage {
    hard_state: HardState,
    log: EntryLog,
    snapshot: Vec<u8>,
}

impl MemStorage {
    pub fn new() -> MemStorage {
        MemStorage::default()
    }
}

impl Storage for MemStorage {
    fn hard_state(&self) -> HardState {
        self.hard_state
    }

    fn save_hard_state(&mut self, state: HardState) -> Result<()> {
        self.hard_state = state;
        Ok(())
    }

    fn last_index(&self) -> u64 {
        self.log.last_index()
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        self.log.term_at(index)
    }

    fn entry(&self, index: u64) -> Option<&Entry> {
        self.log.entry(index)
    }

    fn entries_from(&self, index: u64) -> Vec<Entry> {
        self.log.entries_from(index)
    }

    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        for entry in entries {
            self.log.push(entry.clone());
        }
        Ok(())
    }

    fn truncate_from(&mut self, index: u64) -> Result<()> {
        self.log.truncate_from(index);
        Ok(())
    }

    fn snapshot_meta(&self) -> SnapshotMeta {
        self.log.base()
    }

    fn snapshot_len(&self) -> u64 {
        self.snapshot.len() as u64
    }

    fn read_snapshot(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let start = (offset as usize).min(self.snapshot.len());
        let end = start.saturating_add(len).min(self.snapshot.len());
        Ok(self.snapshot[start..end].to_vec())
    }

    fn save_snapshot(&mut self, meta: SnapshotMeta, data: &[u8]) -> Result<()> {
        if self.log.compact(meta) {
            self.snapshot = data.to_vec();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, term: u64) -> Entry {
        Entry {
            term,
            index,
            command: Command::Noop,
        }
    }

    fn filled() -> MemStorage {
        let mut s = MemStorage::new();
        // terms:        1  1  2  3  3
        let entries: Vec<Entry> = [1, 1, 2, 3, 3]
            .iter()
            .enumerate()
            .map(|(i, &t)| entry(i as u64 + 1, t))
            .collect();
        s.append(&entries).unwrap();
        s
    }

    #[test]
    fn index_zero_is_a_sentinel_at_term_zero() {
        let s = MemStorage::new();
        assert_eq!(s.last_index(), 0);
        assert_eq!(s.term_at(0), Some(0));
        assert_eq!(s.entry(0), None);
        assert_eq!(s.term_at(1), None);
    }

    #[test]
    fn appends_and_reads_back() {
        let s = filled();
        assert_eq!(s.last_index(), 5);
        assert_eq!(s.last_term(), 3);
        assert_eq!(s.term_at(3), Some(2));
        assert_eq!(s.entries_from(4).len(), 2);
        assert_eq!(s.entries_from(6), vec![]);
    }

    #[test]
    fn truncation_drops_the_index_and_everything_after() {
        let mut s = filled();
        s.truncate_from(4).unwrap();
        assert_eq!(s.last_index(), 3);
        assert_eq!(s.last_term(), 2);
        s.truncate_from(9).unwrap();
        assert_eq!(s.last_index(), 3, "truncating past the end changes nothing");
    }

    #[test]
    fn term_boundaries() {
        let s = filled();
        assert_eq!(s.first_index_of_term(3), 4);
        assert_eq!(s.first_index_of_term(1), 1);
        assert_eq!(s.last_index_of_term(1), Some(2));
        assert_eq!(s.last_index_of_term(3), Some(5));
        assert_eq!(s.last_index_of_term(9), None);
    }

    fn data(index: u64, len: usize) -> Entry {
        Entry {
            term: 1,
            index,
            command: Command::Data(vec![b'x'; len]),
        }
    }

    #[test]
    fn a_budgeted_read_stops_at_the_budget() {
        let mut s = MemStorage::new();
        let entries: Vec<Entry> = (1..=10).map(|i| data(i, 100)).collect();
        s.append(&entries).unwrap();
        let each = entries[0].encoded_len();

        let batch = s.entries_within(1, each * 3);
        assert_eq!(batch.len(), 3, "exactly three fit");
        assert_eq!(batch[0].index, 1);

        let batch = s.entries_within(9, each * 3);
        assert_eq!(batch.len(), 2, "only two remain");

        assert!(s.entries_within(11, each * 3).is_empty());
    }

    #[test]
    fn an_entry_bigger_than_the_budget_is_still_sent_alone() {
        let mut s = MemStorage::new();
        s.append(&[data(1, 5000), data(2, 10)]).unwrap();
        let batch = s.entries_within(1, 100);
        assert_eq!(batch.len(), 1, "one oversized entry, and nothing after it");
        assert_eq!(batch[0].index, 1);
    }

    #[test]
    fn a_later_final_term_beats_a_longer_log() {
        let s = filled(); // five entries, ending in term 3
        assert!(s.is_up_to_date(5, 3), "an identical log qualifies");
        assert!(s.is_up_to_date(6, 3), "longer at the same term qualifies");
        assert!(s.is_up_to_date(1, 4), "a later term wins even when shorter");
        assert!(!s.is_up_to_date(4, 3), "shorter at the same term does not");
        assert!(
            !s.is_up_to_date(99, 2),
            "an earlier term loses at any length"
        );
    }

    // -- snapshots ------------------------------------------------------

    fn meta(index: u64, term: u64) -> SnapshotMeta {
        SnapshotMeta { index, term }
    }

    #[test]
    fn compaction_drops_the_prefix_and_keeps_its_last_term() {
        let mut s = filled();
        s.save_snapshot(meta(3, 2), b"state").unwrap();

        assert_eq!(s.snapshot_meta(), meta(3, 2));
        assert_eq!(s.first_index(), 4);
        assert_eq!(s.last_index(), 5, "the tail is untouched");
        assert_eq!(s.term_at(3), Some(2), "the snapshot answers for its index");
        assert_eq!(s.term_at(2), None, "below the snapshot is gone");
        assert_eq!(s.entry(3), None, "the snapshot's index is not an entry");
        assert_eq!(s.entry(4).map(|e| e.index), Some(4));
        assert_eq!(s.read_snapshot(0, 100).unwrap(), b"state");
    }

    #[test]
    fn a_log_after_a_snapshot_still_appends_and_truncates() {
        let mut s = filled();
        s.save_snapshot(meta(3, 2), b"").unwrap();
        s.append(&[entry(6, 4)]).unwrap();
        assert_eq!(s.last_index(), 6);
        s.truncate_from(5).unwrap();
        assert_eq!(s.last_index(), 4);
        assert_eq!(s.entries_from(1).len(), 1, "only index 4 is left");
    }

    #[test]
    fn compacting_everything_leaves_the_snapshot_as_the_log_end() {
        let mut s = filled();
        s.save_snapshot(meta(5, 3), b"").unwrap();
        assert_eq!(s.last_index(), 5);
        assert_eq!(s.last_term(), 3, "votes still see the right last term");
        assert!(s.entries_from(1).is_empty());
        s.append(&[entry(6, 3)]).unwrap();
        assert_eq!(s.first_index(), 6);
    }

    #[test]
    fn a_snapshot_that_disagrees_with_the_log_discards_all_of_it() {
        let mut s = filled();
        // Index 4 here is term 3; a snapshot says it was term 7.
        s.save_snapshot(meta(4, 7), b"theirs").unwrap();
        assert_eq!(s.last_index(), 4);
        assert_eq!(s.term_at(4), Some(7));
        assert_eq!(
            s.entry(5),
            None,
            "an entry after a mismatch cannot be trusted"
        );
    }

    #[test]
    fn a_snapshot_past_the_end_of_the_log_replaces_it() {
        let mut s = filled();
        s.save_snapshot(meta(40, 9), b"far ahead").unwrap();
        assert_eq!(s.last_index(), 40);
        assert_eq!(s.first_index(), 41);
        assert!(s.entries_from(1).is_empty());
    }

    #[test]
    fn an_older_snapshot_changes_nothing() {
        let mut s = filled();
        s.save_snapshot(meta(4, 3), b"new").unwrap();
        s.save_snapshot(meta(2, 1), b"old").unwrap();
        assert_eq!(s.snapshot_meta(), meta(4, 3));
        assert_eq!(s.read_snapshot(0, 10).unwrap(), b"new");
        assert_eq!(s.last_index(), 5, "and the log is untouched");
    }

    #[test]
    fn a_snapshot_reads_back_in_pieces() {
        let mut s = filled();
        s.save_snapshot(meta(2, 1), b"0123456789").unwrap();
        assert_eq!(s.snapshot_len(), 10);
        assert_eq!(s.read_snapshot(0, 4).unwrap(), b"0123");
        assert_eq!(s.read_snapshot(4, 4).unwrap(), b"4567");
        assert_eq!(s.read_snapshot(8, 4).unwrap(), b"89");
        assert!(s.read_snapshot(10, 4).unwrap().is_empty());
    }

    #[test]
    fn term_hints_stay_inside_what_is_held() {
        let mut s = filled();
        s.save_snapshot(meta(3, 2), b"").unwrap();
        assert_eq!(
            s.first_index_of_term(1),
            4,
            "a term inside the snapshot reports the first held entry"
        );
        assert_eq!(
            s.last_index_of_term(2),
            Some(3),
            "the snapshot's term counts"
        );
        assert_eq!(s.last_index_of_term(1), None);
    }
}

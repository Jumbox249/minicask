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
//! a crash (drop the node, keep the storage) and enough to run a cluster.
//! A disk-backed implementation over the store's own append-only files is
//! the obvious next one.

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

/// Durable storage for one node's consensus state.
///
/// Indices are 1-based. Index 0 is a sentinel that always exists and always
/// has term 0, which is what makes the very first `AppendEntries` match
/// without a special case.
pub trait Storage {
    fn hard_state(&self) -> HardState;

    /// Must not return until the state is durable.
    fn save_hard_state(&mut self, state: HardState) -> Result<()>;

    /// The highest index in the log, or 0 when it is empty.
    fn last_index(&self) -> u64;

    /// The term of the entry at `index`, or `None` if there is no such
    /// entry. Index 0 is term 0.
    fn term_at(&self, index: u64) -> Option<u64>;

    fn entry(&self, index: u64) -> Option<&Entry>;

    /// Every entry from `index` onwards.
    fn entries_from(&self, index: u64) -> Vec<Entry>;

    /// Append entries to the end of the log. Must not return until they are
    /// durable.
    fn append(&mut self, entries: &[Entry]) -> Result<()>;

    /// Discard the entry at `index` and everything after it. Only ever
    /// called on entries that conflict with the leader's log, which by
    /// Raft's rules cannot have been committed.
    fn truncate_from(&mut self, index: u64) -> Result<()>;
}

/// Convenience queries that every `Storage` gets for free.
pub trait StorageExt: Storage {
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

    /// The first index belonging to `term`, used to tell a leader how far
    /// back to rewind after a mismatch.
    fn first_index_of_term(&self, term: u64) -> u64 {
        let mut index = 1;
        for i in 1..=self.last_index() {
            if self.term_at(i) == Some(term) {
                index = i;
                break;
            }
        }
        index
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
        for i in index.max(1)..=self.last_index() {
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
    /// entry from it.
    fn last_index_of_term(&self, term: u64) -> Option<u64> {
        (1..=self.last_index())
            .rev()
            .find(|&i| self.term_at(i) == Some(term))
    }
}

impl<S: Storage + ?Sized> StorageExt for S {}

/// An in-memory `Storage`. Surviving a process restart is not something it
/// can do, but surviving a *node* restart is: the test harness drops the
/// node and keeps the storage, which is exactly what a real crash leaves
/// behind.
#[derive(Debug, Clone, Default)]
pub struct MemStorage {
    hard_state: HardState,
    entries: Vec<Entry>,
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
        self.entries.last().map_or(0, |e| e.index)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.entry(index).map(|e| e.term)
    }

    fn entry(&self, index: u64) -> Option<&Entry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    fn entries_from(&self, index: u64) -> Vec<Entry> {
        if index == 0 || index > self.last_index() {
            return Vec::new();
        }
        self.entries[(index - 1) as usize..].to_vec()
    }

    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        for entry in entries {
            debug_assert_eq!(
                entry.index,
                self.last_index() + 1,
                "entries must be appended in order with no gaps"
            );
            self.entries.push(entry.clone());
        }
        Ok(())
    }

    fn truncate_from(&mut self, index: u64) -> Result<()> {
        if index == 0 {
            self.entries.clear();
        } else if index <= self.last_index() {
            self.entries.truncate((index - 1) as usize);
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
}

//! The consensus state machine.
//!
//! A `Node` has no clock, no sockets and no threads. Time arrives as
//! [`tick`](Node::tick) and the network arrives as [`step`](Node::step);
//! both return the messages the node wants sent. Everything that happens is
//! a function of the state and the input, which is what lets the test
//! harness partition a cluster and kill leaders without a single sleep.

use super::log::{Command, Entry, HardState, NodeId, Storage, StorageExt};
use super::message::{Action, Message};
use crate::error::Result;
use std::collections::{HashMap, HashSet};

/// Which of the three states a node is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Timer settings, counted in ticks rather than milliseconds so that the
/// caller decides what a tick is worth.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Ticks between a leader's heartbeats.
    pub heartbeat_ticks: u64,
    /// A follower waits between these two before standing for election.
    ///
    /// The spread is what breaks split votes: two nodes that time out
    /// together split the vote and try again, so their timeouts have to
    /// differ often enough for one to get in first. The range should
    /// comfortably exceed `heartbeat_ticks` or followers will unseat a
    /// perfectly healthy leader.
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            heartbeat_ticks: 2,
            election_timeout_min: 10,
            election_timeout_max: 20,
        }
    }
}

/// A command the leader has appended to its log, and the messages that
/// offer it to the followers. It is not committed yet: watch
/// [`commit_index`](Node::commit_index) reach `index`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accepted {
    pub index: u64,
    pub actions: Vec<Action>,
}

/// Why a command could not be accepted.
#[derive(Debug)]
pub enum ProposeError {
    /// Only a leader may accept commands. Carries who to try instead, when
    /// this node has heard from a leader.
    NotLeader { leader: Option<NodeId> },
    /// The entry could not be made durable, so it was not accepted.
    Storage(crate::Error),
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposeError::NotLeader { leader: Some(id) } => {
                write!(f, "not the leader, try node {id}")
            }
            ProposeError::NotLeader { leader: None } => write!(f, "not the leader"),
            ProposeError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ProposeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProposeError::Storage(e) => Some(e),
            ProposeError::NotLeader { .. } => None,
        }
    }
}

impl From<crate::Error> for ProposeError {
    fn from(e: crate::Error) -> Self {
        ProposeError::Storage(e)
    }
}

/// A deterministic xorshift, so that randomised election timeouts are
/// reproducible. Seeded from the node id, which is enough for peers to
/// time out at different moments.
#[derive(Debug, Clone)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        // Scramble the seed so that ids 1, 2, 3 do not produce similar
        // first values, which would defeat the point.
        Rng(seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407)
            | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

pub struct Node<S: Storage> {
    id: NodeId,
    peers: Vec<NodeId>,
    config: Config,
    storage: S,

    role: Role,
    leader_id: Option<NodeId>,

    /// The highest index known to be committed, and the highest handed to
    /// the state machine. Both are volatile: a restarted node relearns its
    /// commit index from the leader, and never needs to know more than
    /// that, because committed entries are by definition already durable on
    /// a majority.
    commit_index: u64,
    last_applied: u64,

    votes: HashSet<NodeId>,
    /// The index of the no-op this node appended on taking office. Until
    /// that commits, a leader knows the log but not which of it is
    /// committed, so it is not yet fit to answer reads.
    leader_start: u64,
    /// Peers heard from since the last quorum check. A leader that cannot
    /// find a majority in here has been cut off and stands down.
    active: HashSet<NodeId>,
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,

    election_elapsed: u64,
    heartbeat_elapsed: u64,
    election_timeout: u64,
    rng: Rng,
}

impl<S: Storage> Node<S> {
    /// Start, or restart, a node on top of its durable state.
    ///
    /// A node always comes up as a follower. It keeps the term and the vote
    /// it had before, which is what stops a restart from being a second
    /// vote in the same term.
    pub fn new(id: NodeId, peers: Vec<NodeId>, config: Config, storage: S) -> Node<S> {
        let mut rng = Rng::new(id);
        let election_timeout = random_timeout(&mut rng, &config);
        Node {
            id,
            peers: peers.into_iter().filter(|&p| p != id).collect(),
            config,
            storage,
            role: Role::Follower,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            votes: HashSet::new(),
            leader_start: 0,
            active: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            election_timeout,
            rng,
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    pub fn term(&self) -> u64 {
        self.storage.hard_state().term
    }

    /// Who this node currently believes is leading, if anyone.
    pub fn leader(&self) -> Option<NodeId> {
        self.leader_id
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// Whether this node is a leader that may answer a read.
    ///
    /// Winning an election is not enough. A new leader holds every
    /// committed entry, by the up-to-date rule, but it does not yet know
    /// which of them are committed: a follower learns that from the next
    /// `AppendEntries`, and the old leader may have died before sending
    /// one. Reading in that window can miss a write that was acknowledged.
    ///
    /// Committing the no-op from its own term settles it, because
    /// everything earlier commits along with it.
    pub fn ready_to_serve(&self) -> bool {
        self.role == Role::Leader && self.commit_index >= self.leader_start
    }

    pub fn last_index(&self) -> u64 {
        self.storage.last_index()
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Hand back the durable state. Dropping the node and keeping this is
    /// what a crash looks like from the outside.
    pub fn into_storage(self) -> S {
        self.storage
    }

    /// One unit of time has passed.
    pub fn tick(&mut self) -> Result<Vec<Action>> {
        let mut actions = Vec::new();
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.config.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append(&mut actions)?;
                }

                // Check quorum. Raft on its own never tells a leader it has
                // been cut off: it keeps the title until it hears a later
                // term, which it cannot hear from the wrong side of a
                // partition. A leader that believes it still leads will
                // happily answer reads, and those reads are already stale,
                // because the majority has moved on without it.
                //
                // So a leader that cannot account for a majority within one
                // election timeout stands itself down. The term is kept,
                // since nothing has been decided; only the office is given
                // up.
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    self.active.insert(self.id);
                    let reachable = self.active.len();
                    self.active.clear();
                    if !self.has_majority(reachable) {
                        self.become_follower(self.term(), None)?;
                    } else {
                        self.election_elapsed = 0;
                    }
                }
            }
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    // Either nobody is leading, or the last election was
                    // split. Both are answered the same way.
                    self.become_candidate(&mut actions)?;
                }
            }
        }
        Ok(actions)
    }

    /// Stand for election now instead of waiting for the timeout.
    ///
    /// Useful for bringing a fresh cluster up without waiting one out, and
    /// for handing leadership somewhere deliberately. It is always safe: a
    /// node that should not win still will not, because the voters decide.
    pub fn campaign(&mut self) -> Result<Vec<Action>> {
        let mut actions = Vec::new();
        self.become_candidate(&mut actions)?;
        Ok(actions)
    }

    /// A message arrived from `from`.
    pub fn step(&mut self, from: NodeId, message: Message) -> Result<Vec<Action>> {
        let mut actions = Vec::new();
        let term = message.term();

        if term > self.term() {
            // Someone is in a later term, so whatever this node thought it
            // was, it is a follower now. The vote is cleared because it
            // belonged to the term being left behind.
            self.become_follower(term, None)?;
        } else if term < self.term() && message.is_request() {
            // A stale leader or candidate. Telling it our term is what
            // makes it step down.
            actions.push(self.reply_stale(from, &message));
            return Ok(actions);
        }

        match message {
            Message::RequestVote {
                last_log_index,
                last_log_term,
                ..
            } => self.handle_request_vote(from, last_log_index, last_log_term, &mut actions)?,
            Message::RequestVoteReply { term, granted } => {
                self.handle_vote_reply(from, term, granted, &mut actions)?
            }
            Message::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                ..
            } => self.handle_append_entries(
                from,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                &mut actions,
            )?,
            Message::AppendEntriesReply {
                term,
                success,
                match_index,
                conflict_index,
                conflict_term,
            } => self.handle_append_reply(
                from,
                term,
                success,
                match_index,
                conflict_index,
                conflict_term,
                &mut actions,
            )?,
        }
        Ok(actions)
    }

    /// Offer a command to the cluster. Only a leader can accept one.
    pub fn propose(&mut self, command: Vec<u8>) -> std::result::Result<Accepted, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let index = self.append_local(Command::Data(command))?;
        // Send it now rather than waiting for the next heartbeat. A
        // follower that is behind gets caught up by one either way.
        let mut actions = Vec::new();
        self.broadcast_append(&mut actions)?;
        Ok(Accepted { index, actions })
    }

    /// Entries that have been committed and not yet handed over. Applying
    /// them in this order on every node is what makes the state machines
    /// agree.
    pub fn take_committed(&mut self) -> Vec<Entry> {
        let mut out = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.storage.entry(self.last_applied) {
                out.push(entry.clone());
            }
        }
        out
    }

    // -- elections ------------------------------------------------------

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) -> Result<()> {
        let previous = self.storage.hard_state();
        self.role = Role::Follower;
        self.leader_id = leader;
        self.votes.clear();
        self.active.clear();
        if term != previous.term {
            self.storage.save_hard_state(HardState {
                term,
                voted_for: None,
            })?;
        }
        self.reset_election_timer();
        Ok(())
    }

    fn become_candidate(&mut self, actions: &mut Vec<Action>) -> Result<()> {
        let term = self.term() + 1;
        self.role = Role::Candidate;
        self.leader_id = None;
        // A candidate votes for itself, and that vote is as durable as any
        // other: forgetting it across a restart would allow a second vote.
        self.storage.save_hard_state(HardState {
            term,
            voted_for: Some(self.id),
        })?;
        self.votes.clear();
        self.votes.insert(self.id);
        self.reset_election_timer();

        if self.has_majority(self.votes.len()) {
            // A single-node cluster. It is already decided.
            return self.become_leader(actions);
        }

        let message = Message::RequestVote {
            term,
            last_log_index: self.storage.last_index(),
            last_log_term: self.storage.last_term(),
        };
        for &peer in &self.peers {
            actions.push(Action::Send {
                to: peer,
                message: message.clone(),
            });
        }
        Ok(())
    }

    fn become_leader(&mut self, actions: &mut Vec<Action>) -> Result<()> {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        self.active.clear();

        let next = self.storage.last_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for &peer in &self.peers {
            // Optimistically assume every follower matches, and find out
            // otherwise from the first rejection.
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
        }

        // See `Command::Noop`: this is what makes entries from earlier
        // terms committable, and what tells this node when it has caught
        // up enough to be trusted with a read.
        self.leader_start = self.append_local(Command::Noop)?;
        self.broadcast_append(actions)
    }

    fn handle_request_vote(
        &mut self,
        from: NodeId,
        last_log_index: u64,
        last_log_term: u64,
        actions: &mut Vec<Action>,
    ) -> Result<()> {
        let state = self.storage.hard_state();
        let free_to_vote = match state.voted_for {
            None => true,
            // Granting the same candidate the same vote again is safe, and
            // necessary when a reply was lost.
            Some(v) => v == from,
        };
        let granted = free_to_vote && self.storage.is_up_to_date(last_log_index, last_log_term);

        if granted {
            self.storage.save_hard_state(HardState {
                term: state.term,
                voted_for: Some(from),
            })?;
            // Only a granted vote earns the candidate more time. A node
            // that refuses keeps counting down, so that a cluster stuck
            // behind an unelectable candidate still makes progress.
            self.reset_election_timer();
        }

        actions.push(Action::Send {
            to: from,
            message: Message::RequestVoteReply {
                term: self.term(),
                granted,
            },
        });
        Ok(())
    }

    fn handle_vote_reply(
        &mut self,
        from: NodeId,
        term: u64,
        granted: bool,
        actions: &mut Vec<Action>,
    ) -> Result<()> {
        // A reply from an election this node has already left says nothing.
        if self.role != Role::Candidate || term != self.term() {
            return Ok(());
        }
        if granted {
            self.votes.insert(from);
            if self.has_majority(self.votes.len()) {
                self.become_leader(actions)?;
            }
        }
        Ok(())
    }

    // -- replication ----------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    fn handle_append_entries(
        &mut self,
        from: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
        actions: &mut Vec<Action>,
    ) -> Result<()> {
        // The term was checked in `step`, so this leader is current. A
        // candidate that hears from one concedes.
        self.role = Role::Follower;
        self.leader_id = Some(from);
        self.reset_election_timer();

        let term = self.term();
        let local_term = self.storage.term_at(prev_log_index);

        if local_term != Some(prev_log_term) {
            // The logs diverge at or before `prev_log_index`. Say where, so
            // the leader can rewind by a whole term rather than one entry.
            let (conflict_index, conflict_term) = match local_term {
                // Nothing there at all: the log is simply too short.
                None => (self.storage.last_index() + 1, None),
                Some(t) => (self.storage.first_index_of_term(t), Some(t)),
            };
            actions.push(Action::Send {
                to: from,
                message: Message::AppendEntriesReply {
                    term,
                    success: false,
                    match_index: 0,
                    conflict_index,
                    conflict_term,
                },
            });
            return Ok(());
        }

        // The logs match up to `prev_log_index`, so anything the leader
        // sends from here is authoritative.
        let mut append_from = 0;
        for (offset, entry) in entries.iter().enumerate() {
            let index = prev_log_index + 1 + offset as u64;
            match self.storage.term_at(index) {
                Some(t) if t == entry.term => append_from = offset + 1,
                Some(_) => {
                    // A genuine conflict. Raft guarantees this entry was
                    // never committed, so discarding it is safe.
                    self.storage.truncate_from(index)?;
                    break;
                }
                None => break,
            }
        }
        if append_from < entries.len() {
            self.storage.append(&entries[append_from..])?;
        }

        // Note the index of the last entry *this message* covers, not the
        // log's end. A delayed duplicate must not drag the commit index
        // past what its own leader knew.
        let last_covered = prev_log_index + entries.len() as u64;
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(last_covered);
        }

        actions.push(Action::Send {
            to: from,
            message: Message::AppendEntriesReply {
                term,
                success: true,
                match_index: last_covered,
                conflict_index: 0,
                conflict_term: None,
            },
        });
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_append_reply(
        &mut self,
        from: NodeId,
        term: u64,
        success: bool,
        match_index: u64,
        conflict_index: u64,
        conflict_term: Option<u64>,
        actions: &mut Vec<Action>,
    ) -> Result<()> {
        if self.role != Role::Leader || term != self.term() {
            return Ok(());
        }
        // A reply of either kind proves this peer is reachable and still
        // accepts us, which is what the quorum check counts.
        self.active.insert(from);

        if success {
            // Replies can arrive out of order, so never move a follower's
            // progress backwards.
            let current = self.match_index.get(&from).copied().unwrap_or(0);
            if match_index > current {
                self.match_index.insert(from, match_index);
                self.next_index.insert(from, match_index + 1);
                self.maybe_commit();
            }
            return Ok(());
        }

        // Rewind and try again. Jumping to a term boundary rather than
        // stepping back one entry keeps this to a few round trips even
        // when a follower is far behind.
        let next = match conflict_term {
            Some(t) => match self.storage.last_index_of_term(t) {
                // The leader has that term too, so resume just after it.
                Some(i) => i + 1,
                // It does not, so the follower's whole run of that term is
                // wrong and can be skipped in one go.
                None => conflict_index,
            },
            None => conflict_index,
        };
        self.next_index.insert(from, next.max(1));
        actions.push(self.append_message_for(from));
        Ok(())
    }

    fn maybe_commit(&mut self) {
        // The median of the followers' progress, counting this node's own
        // log, is the highest index a majority holds.
        let mut indices: Vec<u64> = self
            .peers
            .iter()
            .map(|p| self.match_index.get(p).copied().unwrap_or(0))
            .collect();
        indices.push(self.storage.last_index());
        indices.sort_unstable_by(|a, b| b.cmp(a));
        let majority = indices[self.quorum() - 1];

        // Only an entry from the leader's own term may be committed by
        // counting replicas; earlier ones ride along behind it. Without
        // this a later leader can still overwrite a "committed" entry.
        if majority > self.commit_index && self.storage.term_at(majority) == Some(self.term()) {
            self.commit_index = majority;
        }
    }

    fn broadcast_append(&mut self, actions: &mut Vec<Action>) -> Result<()> {
        let peers = self.peers.clone();
        for peer in peers {
            actions.push(self.append_message_for(peer));
        }
        Ok(())
    }

    fn append_message_for(&self, peer: NodeId) -> Action {
        let next = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or_else(|| self.storage.last_index() + 1);
        let prev_log_index = next.saturating_sub(1);
        Action::Send {
            to: peer,
            message: Message::AppendEntries {
                term: self.term(),
                prev_log_index,
                // A `None` here means the leader has itself discarded that
                // entry, which cannot happen without snapshots.
                prev_log_term: self.storage.term_at(prev_log_index).unwrap_or(0),
                entries: self.storage.entries_from(next),
                leader_commit: self.commit_index,
            },
        }
    }

    fn append_local(&mut self, command: Command) -> Result<u64> {
        let index = self.storage.last_index() + 1;
        self.storage.append(&[Entry {
            term: self.term(),
            index,
            command,
        }])?;
        // A one-node cluster commits the moment it appends, since it is
        // its own majority.
        self.maybe_commit();
        Ok(index)
    }

    // -- odds and ends --------------------------------------------------

    fn reply_stale(&self, from: NodeId, message: &Message) -> Action {
        let term = self.term();
        Action::Send {
            to: from,
            message: match message {
                Message::RequestVote { .. } => Message::RequestVoteReply {
                    term,
                    granted: false,
                },
                _ => Message::AppendEntriesReply {
                    term,
                    success: false,
                    match_index: 0,
                    conflict_index: 0,
                    conflict_term: None,
                },
            },
        }
    }

    fn quorum(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    fn has_majority(&self, votes: usize) -> bool {
        votes >= self.quorum()
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = random_timeout(&mut self.rng, &self.config);
    }
}

#[cfg(test)]
impl<S: Storage> Node<S> {
    /// Put the node in office with a given view of its followers, without
    /// running an election or appending the usual no-op.
    ///
    /// This exists to test [`maybe_commit`](Node::maybe_commit) in
    /// isolation. Through the ordinary path the no-op makes the
    /// current-term rule unreachable, since every `AppendEntries` a leader
    /// sends includes it, so the only way to exercise the rule is to build
    /// the state it guards against directly.
    fn leader_with(&mut self, followers: &[(NodeId, u64)]) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        for &(id, matched) in followers {
            self.match_index.insert(id, matched);
            self.next_index.insert(id, matched + 1);
        }
    }
}

fn random_timeout(rng: &mut Rng, config: &Config) -> u64 {
    let span = config
        .election_timeout_max
        .saturating_sub(config.election_timeout_min);
    config.election_timeout_min
        + if span == 0 {
            0
        } else {
            rng.next() % (span + 1)
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::MemStorage;

    fn entries(spec: &[(u64, u64)]) -> Vec<Entry> {
        spec.iter()
            .map(|&(index, term)| Entry {
                term,
                index,
                command: Command::Noop,
            })
            .collect()
    }

    /// A follower in `term` whose log holds `spec`, as (index, term) pairs.
    fn follower(term: u64, spec: &[(u64, u64)]) -> Node<MemStorage> {
        let mut storage = MemStorage::new();
        storage
            .save_hard_state(HardState {
                term,
                voted_for: None,
            })
            .unwrap();
        storage.append(&entries(spec)).unwrap();
        Node::new(1, vec![1, 2, 3], Config::default(), storage)
    }

    fn granted(actions: &[Action]) -> bool {
        match actions {
            [Action::Send {
                message: Message::RequestVoteReply { granted, .. },
                ..
            }] => *granted,
            other => panic!("expected one vote reply, got {other:?}"),
        }
    }

    fn accepted(actions: &[Action]) -> bool {
        match actions {
            [Action::Send {
                message: Message::AppendEntriesReply { success, .. },
                ..
            }] => *success,
            other => panic!("expected one append reply, got {other:?}"),
        }
    }

    fn vote_request(term: u64, last_log_index: u64, last_log_term: u64) -> Message {
        Message::RequestVote {
            term,
            last_log_index,
            last_log_term,
        }
    }

    fn append(term: u64, prev: (u64, u64), new: &[(u64, u64)], commit: u64) -> Message {
        Message::AppendEntries {
            term,
            prev_log_index: prev.0,
            prev_log_term: prev.1,
            entries: entries(new),
            leader_commit: commit,
        }
    }

    // -- voting ---------------------------------------------------------

    #[test]
    fn a_candidate_whose_log_ends_earlier_is_refused() {
        // Our log ends at term 5, the candidate's at term 3. Electing it
        // would let it overwrite entries a majority already holds.
        let mut node = follower(5, &[(1, 3), (2, 5)]);
        let actions = node.step(2, vote_request(6, 2, 3)).unwrap();
        assert!(!granted(&actions), "a stale candidate was given a vote");
    }

    #[test]
    fn a_longer_log_does_not_beat_a_later_term() {
        let mut node = follower(5, &[(1, 5)]);
        let actions = node.step(2, vote_request(6, 99, 4)).unwrap();
        assert!(!granted(&actions), "length was allowed to beat recency");
    }

    #[test]
    fn an_up_to_date_candidate_is_granted() {
        let mut node = follower(5, &[(1, 3), (2, 5)]);
        assert!(granted(&node.step(2, vote_request(6, 2, 5)).unwrap()));
        assert_eq!(node.storage().hard_state().voted_for, Some(2));
    }

    #[test]
    fn only_one_vote_per_term() {
        let mut node = follower(5, &[(1, 5)]);
        assert!(granted(&node.step(2, vote_request(6, 1, 5)).unwrap()));
        // The same term, an equally good log, a different candidate.
        assert!(
            !granted(&node.step(3, vote_request(6, 1, 5)).unwrap()),
            "a node voted twice in one term"
        );
        // Repeating the same request is fine, since a reply can be lost.
        assert!(granted(&node.step(2, vote_request(6, 1, 5)).unwrap()));
    }

    #[test]
    fn a_vote_survives_a_restart() {
        let mut node = follower(5, &[(1, 5)]);
        assert!(granted(&node.step(2, vote_request(6, 1, 5)).unwrap()));

        let storage = node.into_storage();
        let mut node = Node::new(1, vec![1, 2, 3], Config::default(), storage);
        assert!(
            !granted(&node.step(3, vote_request(6, 1, 5)).unwrap()),
            "a restart let the node vote a second time in one term"
        );
    }

    // -- the log --------------------------------------------------------

    #[test]
    fn a_duplicate_append_does_not_shorten_the_log() {
        let mut node = follower(1, &[]);
        let first = append(1, (0, 0), &[(1, 1), (2, 1)], 0);
        assert!(accepted(&node.step(2, first.clone()).unwrap()));
        assert_eq!(node.last_index(), 2);

        // The same message again, as a retransmission would be.
        assert!(accepted(&node.step(2, first).unwrap()));
        assert_eq!(node.last_index(), 2, "a duplicate truncated the log");
    }

    #[test]
    fn a_stale_append_does_not_shorten_the_log() {
        let mut node = follower(1, &[]);
        node.step(2, append(1, (0, 0), &[(1, 1), (2, 1), (3, 1)], 0))
            .unwrap();
        assert_eq!(node.last_index(), 3);

        // An older message from the same leader, arriving late. It agrees
        // with what we hold, so it must change nothing.
        assert!(accepted(
            &node.step(2, append(1, (0, 0), &[(1, 1)], 0)).unwrap()
        ));
        assert_eq!(
            node.last_index(),
            3,
            "a delayed message deleted entries it simply did not mention"
        );
    }

    #[test]
    fn a_genuine_conflict_is_truncated_and_replaced() {
        let mut node = follower(1, &[]);
        node.step(2, append(1, (0, 0), &[(1, 1), (2, 1), (3, 1)], 0))
            .unwrap();

        // A new leader in term 2 disagrees from index 2 onwards.
        assert!(accepted(
            &node.step(3, append(2, (1, 1), &[(2, 2)], 0)).unwrap()
        ));
        assert_eq!(node.last_index(), 2);
        assert_eq!(node.storage().term_at(2), Some(2));
        assert_eq!(node.storage().term_at(3), None, "the tail should be gone");
    }

    #[test]
    fn a_mismatched_prev_entry_is_rejected_with_a_hint() {
        let mut node = follower(2, &[(1, 1), (2, 1), (3, 2)]);
        let actions = node.step(2, append(2, (3, 9), &[], 0)).unwrap();
        match actions.as_slice() {
            [Action::Send {
                message:
                    Message::AppendEntriesReply {
                        success,
                        conflict_index,
                        conflict_term,
                        ..
                    },
                ..
            }] => {
                assert!(!success);
                assert_eq!(*conflict_term, Some(2));
                assert_eq!(*conflict_index, 3, "should point at the term boundary");
            }
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn a_short_log_is_rejected_with_its_own_length() {
        let mut node = follower(2, &[(1, 1)]);
        let actions = node.step(2, append(2, (5, 2), &[], 0)).unwrap();
        match actions.as_slice() {
            [Action::Send {
                message:
                    Message::AppendEntriesReply {
                        success,
                        conflict_index,
                        conflict_term,
                        ..
                    },
                ..
            }] => {
                assert!(!success);
                assert_eq!(*conflict_term, None);
                assert_eq!(*conflict_index, 2, "resume just past what we have");
            }
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    // -- commit ---------------------------------------------------------

    /// Figure 8 of the paper. A leader has an entry from an earlier term
    /// sitting on a majority of logs. Counting replicas alone would call it
    /// committed, and a later leader could still overwrite it, because
    /// nothing about those replicas came from a term this leader can speak
    /// for.
    #[test]
    fn an_entry_from_an_earlier_term_is_not_committed_by_counting_replicas() {
        let mut node = follower(5, &[(1, 1), (2, 1)]);
        node.leader_with(&[(2, 2), (3, 2)]);
        assert_eq!(node.term(), 5, "the log is entirely from earlier terms");

        node.maybe_commit();
        assert_eq!(
            node.commit_index(),
            0,
            "committed an earlier term's entry on a replica count alone"
        );
    }

    /// The other half of the rule: once one entry from the leader's own
    /// term commits, everything behind it commits with it. That is what
    /// the no-op on taking office is for.
    #[test]
    fn committing_the_current_term_carries_the_backlog_with_it() {
        let mut node = follower(5, &[(1, 1), (2, 1), (3, 5)]);
        node.leader_with(&[(2, 3), (3, 3)]);

        node.maybe_commit();
        assert_eq!(
            node.commit_index(),
            3,
            "an entry from this term should commit, and carry the rest"
        );
    }

    /// And a majority really is required, not a plurality.
    #[test]
    fn a_minority_of_replicas_commits_nothing() {
        let mut node = follower(5, &[(1, 5), (2, 5)]);
        // Five nodes, so two matching followers plus the leader is three
        // of five, a majority; one plus the leader is not.
        node.peers = vec![2, 3, 4, 5];
        node.leader_with(&[(2, 2), (3, 0), (4, 0), (5, 0)]);

        node.maybe_commit();
        assert_eq!(node.commit_index(), 0, "committed without a majority");

        node.leader_with(&[(3, 2)]);
        node.maybe_commit();
        assert_eq!(node.commit_index(), 2, "a majority should commit");
    }

    #[test]
    fn a_delayed_message_cannot_drag_the_commit_index_forward() {
        let mut node = follower(1, &[]);
        node.step(2, append(1, (0, 0), &[(1, 1), (2, 1), (3, 1)], 0))
            .unwrap();
        assert_eq!(node.commit_index(), 0);

        // A leader that knew of three commits, in a message covering only
        // the first entry. It may commit no further than it sent.
        node.step(2, append(1, (0, 0), &[(1, 1)], 3)).unwrap();
        assert_eq!(
            node.commit_index(),
            1,
            "committed past the end of what the message covered"
        );
    }

    #[test]
    fn a_stale_term_is_rejected_out_of_hand() {
        let mut node = follower(5, &[(1, 5)]);
        let actions = node.step(2, append(3, (0, 0), &[(1, 3)], 0)).unwrap();
        assert!(!accepted(&actions), "an old leader was obeyed");
        assert_eq!(node.last_index(), 1);
        assert_eq!(node.storage().term_at(1), Some(5), "our entry survived");
    }

    #[test]
    fn a_later_term_makes_a_leader_stand_down() {
        let mut storage = MemStorage::new();
        storage
            .save_hard_state(HardState {
                term: 2,
                voted_for: Some(1),
            })
            .unwrap();
        let mut node = Node::new(1, vec![1, 2, 3], Config::default(), storage);
        node.campaign().unwrap();
        node.step(
            2,
            Message::RequestVoteReply {
                term: 3,
                granted: true,
            },
        )
        .unwrap();
        assert!(node.is_leader(), "should have won its own election");

        node.step(3, append(9, (0, 0), &[], 0)).unwrap();
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.term(), 9);
        assert_eq!(node.leader(), Some(3));
    }
}

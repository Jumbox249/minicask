//! The consensus state machine.
//!
//! A `Node` has no clock, no sockets and no threads. Time arrives as
//! [`tick`](Node::tick) and the network arrives as [`step`](Node::step);
//! both return the messages the node wants sent. Everything that happens is
//! a function of the state and the input, which is what lets the test
//! harness partition a cluster and kill leaders without a single sleep.

use super::log::{
    Command, Entry, HardState, NodeId, SnapshotMeta, SnapshotSink, Storage, StorageExt,
};
use super::message::{Action, Message};
use crate::error::Result;
use std::collections::{HashMap, HashSet};
use std::io::Write;

/// Which of the three states a node is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    /// Asking whether an election would be worth holding. A pre-candidate
    /// has not raised its term and has not voted for itself on disk, so
    /// this state costs nothing and can be abandoned without trace.
    PreCandidate,
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
    /// Roughly how many bytes of entries one `AppendEntries` may carry.
    ///
    /// Without a limit a follower that has fallen behind is sent its whole
    /// backlog in one message, again on every heartbeat until it answers,
    /// and a backlog past the transport's frame limit can never be sent
    /// at all. A message always carries at least one entry, so this is a
    /// target rather than a hard cap; `max_entry_bytes` is the hard cap.
    pub max_append_bytes: usize,
    /// The largest command `propose` will accept, so that any one entry
    /// fits in a message.
    pub max_entry_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            heartbeat_ticks: 2,
            election_timeout_min: 10,
            election_timeout_max: 20,
            max_append_bytes: 1024 * 1024,
            max_entry_bytes: 16 * 1024 * 1024,
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
    /// The command is bigger than `Config::max_entry_bytes`, so it could
    /// never be replicated.
    TooLarge { len: usize, max: usize },
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
            ProposeError::TooLarge { len, max } => {
                write!(
                    f,
                    "command of {len} bytes exceeds the {max} byte entry limit"
                )
            }
            ProposeError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ProposeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProposeError::Storage(e) => Some(e),
            ProposeError::NotLeader { .. } | ProposeError::TooLarge { .. } => None,
        }
    }
}

impl From<crate::Error> for ProposeError {
    fn from(e: crate::Error) -> Self {
        ProposeError::Storage(e)
    }
}

/// A read that has been started with [`Node::read_index`] and is waiting to
/// be allowed. Opaque; ask [`Node::read_state`] about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadRequest(ReadKind);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadKind {
    /// Taken on the leader, and allowed once a majority has answered round
    /// `seq` of `term`.
    Local { term: u64, seq: u64, index: u64 },
    /// Asked of `leader`, whose answer arrives as a message.
    Forwarded { id: u64, leader: NodeId },
    /// There was nobody to ask.
    Refused,
}

/// Where a read stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadState {
    /// Not confirmed yet.
    Pending,
    /// Confirmed. The read may be answered from the state machine once it
    /// has applied at least this index, and will then see every write that
    /// was acknowledged before the read began.
    Ready(u64),
    /// It never will be: leadership changed, or there was no leader to
    /// ask. Try again, probably somewhere else.
    Failed,
}

/// What a follower has heard back about a read it forwarded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    Waiting,
    Ready(u64),
    Refused,
}

/// A read a follower asked this leader for, waiting on a round.
#[derive(Debug, Clone, Copy)]
struct RemoteRead {
    from: NodeId,
    id: u64,
    seq: u64,
    index: u64,
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
    /// Leader only: for each follower being sent a snapshot, which snapshot
    /// (by its last index) and how far into it the follower has got.
    snapshot_progress: HashMap<NodeId, (u64, u64)>,
    /// Follower only: a snapshot arriving in pieces, and who is sending it.
    /// Pieces from two leaders are never spliced, even for the same index:
    /// both describe the same state, but nothing promises the same bytes.
    /// The pieces go straight to storage as they arrive.
    incoming: Option<(NodeId, S::Sink)>,
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,

    /// Leader only: the current round. Every `AppendEntries` and
    /// `InstallSnapshot` carries it and every reply echoes it, and a read
    /// opens a new one. Kept across terms, so it never repeats.
    seq: u64,
    /// Leader only: the latest round each follower has answered this term.
    acked: HashMap<NodeId, u64>,
    /// Leader only: reads followers have asked for, waiting on a round.
    remote_reads: Vec<RemoteRead>,
    /// Follower only: reads forwarded to a leader, by id, and the answers.
    forwarded: HashMap<u64, Answer>,
    next_read: u64,

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
        let snapshot = storage.snapshot_meta().index;
        let mut rng = Rng::new(id);
        let election_timeout = random_timeout(&mut rng, &config);
        Node {
            id,
            peers: peers.into_iter().filter(|&p| p != id).collect(),
            config,
            storage,
            role: Role::Follower,
            leader_id: None,
            // A snapshot only ever holds applied state, and applied state
            // is committed, so a restart can start from there rather than
            // from nothing.
            commit_index: snapshot,
            last_applied: snapshot,
            votes: HashSet::new(),
            leader_start: 0,
            active: HashSet::new(),
            snapshot_progress: HashMap::new(),
            incoming: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            seq: 0,
            acked: HashMap::new(),
            remote_reads: Vec::new(),
            forwarded: HashMap::new(),
            next_read: 0,
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

    /// Start a read that must see every write acknowledged before it.
    ///
    /// A read served straight from the leader's state machine is fast and
    /// usually right, but not always: a leader cut off from the others
    /// goes on believing it leads until it notices, and in the meantime a
    /// new leader can accept writes it knows nothing about. So a read
    /// first records the commit index, then waits for a majority to answer
    /// a round of heartbeats sent after that. If they do, no other leader
    /// can have been elected in between, and a state machine applied up to
    /// the recorded index holds every acknowledged write. That is the read
    /// index from section 6.4 of the Raft dissertation, and it costs one
    /// round trip and nothing in the log.
    ///
    /// On a follower the question goes to the leader instead, and once
    /// the answer comes back the follower serves the read from its own
    /// state machine, which is how reads get spread across a cluster
    /// rather than all landing on the leader.
    ///
    /// Poll [`read_state`](Node::read_state) after each `tick` or `step`,
    /// and hand the request to [`forget_read`](Node::forget_read) when done
    /// with it.
    pub fn read_index(&mut self) -> (ReadRequest, Vec<Action>) {
        let mut actions = Vec::new();
        let request = if self.role == Role::Leader {
            let (seq, index) = self.open_read_round(&mut actions);
            ReadKind::Local {
                term: self.term(),
                seq,
                index,
            }
        } else if let Some(leader) = self.leader_id {
            self.next_read += 1;
            let id = self.next_read;
            self.forwarded.insert(id, Answer::Waiting);
            actions.push(Action::Send {
                to: leader,
                message: Message::ReadIndex {
                    term: self.term(),
                    id,
                },
            });
            ReadKind::Forwarded { id, leader }
        } else {
            ReadKind::Refused
        };
        (ReadRequest(request), actions)
    }

    pub fn read_state(&self, request: &ReadRequest) -> ReadState {
        match request.0 {
            ReadKind::Local { term, seq, index } => {
                if self.role != Role::Leader || self.term() != term {
                    // Whatever the round says now, it says it about a term
                    // this node no longer leads.
                    ReadState::Failed
                } else if self.round_confirmed(seq) {
                    ReadState::Ready(index)
                } else {
                    ReadState::Pending
                }
            }
            ReadKind::Forwarded { id, leader } => match self.forwarded.get(&id) {
                Some(Answer::Ready(index)) => ReadState::Ready(*index),
                // The question went to a leader this node has since stopped
                // following, so an answer may never come.
                Some(Answer::Waiting) if self.leader_id == Some(leader) => ReadState::Pending,
                _ => ReadState::Failed,
            },
            ReadKind::Refused => ReadState::Failed,
        }
    }

    /// Let go of a read, answered or not.
    pub fn forget_read(&mut self, request: &ReadRequest) {
        if let ReadKind::Forwarded { id, .. } = request.0 {
            self.forwarded.remove(&id);
        }
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
            Role::Follower | Role::PreCandidate | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    // Nobody is leading, or the last attempt was split, or
                    // the question went unanswered. All three are answered
                    // by asking again before running.
                    self.become_pre_candidate(&mut actions)?;
                }
            }
        }
        Ok(actions)
    }

    /// Fold the log up to `index` into a snapshot, and discard the entries
    /// it covers.
    ///
    /// `data` is the caller's state machine as of `index`, which is why
    /// only applied state can be snapshotted: that is the only state the
    /// caller can describe. Returns whether anything was compacted; asking
    /// for an index that is not applied yet, or not past the current
    /// snapshot, does nothing.
    pub fn compact(&mut self, index: u64, data: &[u8]) -> Result<bool> {
        let Some(mut sink) = self.begin_compaction(index)? else {
            return Ok(false);
        };
        sink.write_all(data)?;
        self.finish_compaction(sink)
    }

    /// The first half of [`compact`](Node::compact), for a snapshot too
    /// large to build in memory: a sink to write the state as of `index`
    /// into, or `None` if `index` cannot be snapshotted.
    ///
    /// The node carries on while the sink is being written, so the caller
    /// can write it without holding up consensus, provided what it writes
    /// is the state as of `index` and not whatever the state has become
    /// since.
    pub fn begin_compaction(&mut self, index: u64) -> Result<Option<S::Sink>> {
        if index <= self.storage.snapshot_meta().index || index > self.last_applied {
            return Ok(None);
        }
        let Some(term) = self.storage.term_at(index) else {
            return Ok(None);
        };
        Ok(Some(
            self.storage.new_snapshot(SnapshotMeta { index, term })?,
        ))
    }

    /// Install a snapshot written from [`begin_compaction`](Node::begin_compaction)
    /// and discard the log it covers. Returns false, and discards the
    /// snapshot instead, if a newer one was installed in the meantime,
    /// which happens when a leader sent one while this one was being
    /// written.
    pub fn finish_compaction(&mut self, sink: S::Sink) -> Result<bool> {
        if sink.meta().index <= self.storage.snapshot_meta().index {
            return Ok(false);
        }
        self.storage.install_snapshot(sink)?;
        Ok(true)
    }

    /// Stand for election now, skipping the pre-vote.
    ///
    /// This raises the term whether or not anyone would have voted for it,
    /// so it is for handing leadership over deliberately and for bringing
    /// a fresh cluster up without waiting one out. It is always safe: a
    /// node that should not win still will not, because the voters decide.
    /// An ordinary timeout asks first, via [`Role::PreCandidate`].
    pub fn campaign(&mut self) -> Result<Vec<Action>> {
        let mut actions = Vec::new();
        self.become_candidate(&mut actions)?;
        Ok(actions)
    }

    /// Ask the cluster whether an election would be worth holding, the way
    /// an ordinary timeout does.
    ///
    /// Unlike [`campaign`](Node::campaign) this raises no term and writes
    /// nothing, so a node that would not win leaves no trace of having
    /// asked.
    pub fn pre_vote(&mut self) -> Result<Vec<Action>> {
        let mut actions = Vec::new();
        self.become_pre_candidate(&mut actions)?;
        Ok(actions)
    }

    /// Run the election timer out without acting on it, so that this node
    /// no longer counts a sitting leader as recently heard from.
    ///
    /// Only the tests need this, to put a node in the state a real one
    /// reaches by waiting.
    #[doc(hidden)]
    pub fn expire_election_timer(&mut self) {
        self.election_elapsed = self.election_timeout;
    }

    /// A message arrived from `from`.
    pub fn step(&mut self, from: NodeId, message: Message) -> Result<Vec<Action>> {
        let mut actions = Vec::new();
        let term = message.term();

        // A node that is still hearing from a leader owes it the rest of
        // its lease, and says no to anyone canvassing. Without this a node
        // returning from a partition unseats a leader that is doing its
        // job perfectly well, purely by asking.
        if message.is_vote_request()
            && self.leader_id.is_some()
            && self.election_elapsed < self.election_timeout
        {
            actions.push(self.refuse_vote(from, &message));
            return Ok(actions);
        }

        // Pre-vote traffic never moves anyone's term, in either direction.
        // That is the point of asking first, so it is handled ahead of the
        // rule that would.
        match message {
            Message::PreVote {
                term,
                last_log_index,
                last_log_term,
            } => {
                self.handle_pre_vote(from, term, last_log_index, last_log_term, &mut actions);
                return Ok(actions);
            }
            Message::PreVoteReply { term, granted } => {
                self.handle_pre_vote_reply(from, term, granted, &mut actions)?;
                return Ok(actions);
            }
            _ => {}
        }

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
            // Both were answered above, before anything could touch a term.
            Message::PreVote { .. } | Message::PreVoteReply { .. } => {
                unreachable!("pre-vote traffic returns before reaching here")
            }
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
                seq,
                ..
            } => self.handle_append_entries(
                from,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                seq,
                &mut actions,
            )?,
            Message::AppendEntriesReply {
                term,
                success,
                match_index,
                conflict_index,
                conflict_term,
                seq,
            } => {
                if self.heard_round(from, term, seq, &mut actions) {
                    self.handle_append_reply(
                        from,
                        success,
                        match_index,
                        conflict_index,
                        conflict_term,
                        &mut actions,
                    )?
                }
            }
            Message::InstallSnapshot {
                last_index,
                last_term,
                offset,
                data,
                done,
                seq,
                ..
            } => self.handle_install_snapshot(
                from,
                SnapshotMeta {
                    index: last_index,
                    term: last_term,
                },
                offset,
                data,
                done,
                seq,
                &mut actions,
            )?,
            Message::InstallSnapshotReply {
                term,
                last_index,
                next_offset,
                done,
                seq,
            } => {
                if self.heard_round(from, term, seq, &mut actions) {
                    self.handle_snapshot_reply(from, last_index, next_offset, done, &mut actions)?
                }
            }
            Message::ReadIndex { id, .. } => self.handle_read_index(from, id, &mut actions),
            Message::ReadIndexReply { id, index, .. } => {
                // An answer from an earlier term is still a good one: that
                // leader confirmed it held office after the question
                // arrived, so the index covers every write acknowledged
                // before it was asked.
                if let Some(answer @ Answer::Waiting) = self.forwarded.get_mut(&id) {
                    *answer = index.map_or(Answer::Refused, Answer::Ready);
                }
            }
        }
        Ok(actions)
    }

    /// Offer several commands at once, as consecutive entries.
    ///
    /// They are appended to the log in one write, so one fsync covers them
    /// all, and offered to the followers in one message each. That is the
    /// whole of group commit on the consensus side: a server gathers the
    /// proposals that arrive while one is being appended and sends them
    /// here together.
    ///
    /// Each command gets its own answer, its index or why it was refused;
    /// the whole batch is refused only when this node cannot accept
    /// anything at all.
    #[allow(clippy::type_complexity)]
    pub fn propose_batch(
        &mut self,
        commands: Vec<Vec<u8>>,
    ) -> std::result::Result<(Vec<std::result::Result<u64, ProposeError>>, Vec<Action>), ProposeError>
    {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let term = self.term();
        let mut next = self.storage.last_index() + 1;
        let mut entries = Vec::with_capacity(commands.len());
        let mut answers = Vec::with_capacity(commands.len());
        for command in commands {
            let len = command.len() + super::log::ENTRY_OVERHEAD;
            if len > self.config.max_entry_bytes {
                answers.push(Err(ProposeError::TooLarge {
                    len,
                    max: self.config.max_entry_bytes,
                }));
                continue;
            }
            entries.push(Entry {
                term,
                index: next,
                command: Command::Data(command),
            });
            answers.push(Ok(next));
            next += 1;
        }
        let mut actions = Vec::new();
        if !entries.is_empty() {
            self.storage.append(&entries)?;
            self.maybe_commit();
            self.broadcast_append(&mut actions)?;
        }
        Ok((answers, actions))
    }

    /// Offer a command to the cluster. Only a leader can accept one.
    pub fn propose(&mut self, command: Vec<u8>) -> std::result::Result<Accepted, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let len = command.len() + super::log::ENTRY_OVERHEAD;
        if len > self.config.max_entry_bytes {
            // Accepting it would wedge the log: every later entry sits
            // behind one that no message can carry.
            return Err(ProposeError::TooLarge {
                len,
                max: self.config.max_entry_bytes,
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
        // Reads asked of this node as leader go unanswered, and the
        // followers who asked give up on them once they see it has gone.
        self.remote_reads.clear();
        if term != previous.term {
            self.storage.save_hard_state(HardState {
                term,
                voted_for: None,
            })?;
        }
        self.reset_election_timer();
        Ok(())
    }

    /// Ask the cluster whether an election would be worth holding.
    ///
    /// Nothing is written to disk and the term does not move, so a
    /// pre-candidate that hears nothing back has cost the cluster nothing
    /// and is free to ask again later.
    fn become_pre_candidate(&mut self, actions: &mut Vec<Action>) -> Result<()> {
        self.role = Role::PreCandidate;
        self.leader_id = None;
        self.remote_reads.clear();
        self.votes.clear();
        self.votes.insert(self.id);
        self.reset_election_timer();

        if self.has_majority(self.votes.len()) {
            // A single-node cluster needs nobody's permission.
            return self.become_candidate(actions);
        }

        let message = Message::PreVote {
            term: self.term() + 1,
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

    /// Answer the hypothetical. No vote is recorded, because none was
    /// cast: this node stays free to vote for whoever actually stands.
    fn handle_pre_vote(
        &mut self,
        from: NodeId,
        proposed_term: u64,
        last_log_index: u64,
        last_log_term: u64,
        actions: &mut Vec<Action>,
    ) {
        let worth_running = proposed_term > self.term();
        let granted = worth_running && self.storage.is_up_to_date(last_log_index, last_log_term);

        actions.push(Action::Send {
            to: from,
            message: Message::PreVoteReply {
                // Echoing the term that was asked about, on a yes, lets the
                // asker tell this round's replies from the last one's. A no
                // carries our own term instead, which is how a node that
                // has fallen behind finds out.
                term: if granted { proposed_term } else { self.term() },
                granted,
            },
        });
    }

    fn handle_pre_vote_reply(
        &mut self,
        from: NodeId,
        term: u64,
        granted: bool,
        actions: &mut Vec<Action>,
    ) -> Result<()> {
        if granted {
            // Only this round's answers count, and only while still asking.
            if self.role == Role::PreCandidate && term == self.term() + 1 {
                self.votes.insert(from);
                if self.has_majority(self.votes.len()) {
                    // The cluster would have us. Now the term is worth it.
                    self.become_candidate(actions)?;
                }
            }
        } else if term > self.term() {
            // Refused by someone further ahead, which answers a different
            // question: we are the ones who are behind.
            self.become_follower(term, None)?;
        }
        Ok(())
    }

    fn become_candidate(&mut self, actions: &mut Vec<Action>) -> Result<()> {
        let term = self.term() + 1;
        self.role = Role::Candidate;
        self.leader_id = None;
        self.remote_reads.clear();
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
        self.snapshot_progress.clear();
        self.acked.clear();
        self.remote_reads.clear();

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
        seq: u64,
        actions: &mut Vec<Action>,
    ) -> Result<()> {
        // The term was checked in `step`, so this leader is current. A
        // candidate that hears from one concedes.
        self.role = Role::Follower;
        self.leader_id = Some(from);
        self.reset_election_timer();

        let term = self.term();

        // Part of what this message covers may already be folded into a
        // snapshot here. Everything up to the snapshot is committed, so it
        // agrees with any current leader: skip that part of the message and
        // carry on from the snapshot's end, rather than rejecting it and
        // costing the leader a round trip to find that out.
        let snapshot = self.storage.snapshot_meta();
        let (prev_log_index, prev_log_term, entries) = if prev_log_index < snapshot.index {
            let skip = (snapshot.index - prev_log_index) as usize;
            if skip >= entries.len() {
                actions.push(Action::Send {
                    to: from,
                    message: Message::AppendEntriesReply {
                        term,
                        success: true,
                        match_index: snapshot.index,
                        conflict_index: 0,
                        conflict_term: None,
                        seq,
                    },
                });
                return Ok(());
            }
            let mut entries = entries;
            entries.drain(..skip);
            (snapshot.index, snapshot.term, entries)
        } else {
            (prev_log_index, prev_log_term, entries)
        };

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
                    seq,
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
                seq,
            },
        });
        Ok(())
    }

    /// A reply from a follower, of either kind, in this leader's term. It
    /// proves the follower is reachable and still accepts this leader,
    /// which is what the quorum check counts, and it answers a round, which
    /// may be what a read is waiting on. Returns false for a reply that
    /// belongs to another term or reached a node no longer leading.
    fn heard_round(
        &mut self,
        from: NodeId,
        term: u64,
        seq: u64,
        actions: &mut Vec<Action>,
    ) -> bool {
        if self.role != Role::Leader || term != self.term() {
            return false;
        }
        self.active.insert(from);
        let acked = self.acked.entry(from).or_insert(0);
        if seq > *acked {
            *acked = seq;
            self.release_reads(actions);
        }
        true
    }

    fn handle_append_reply(
        &mut self,
        from: NodeId,
        success: bool,
        match_index: u64,
        conflict_index: u64,
        conflict_term: Option<u64>,
        actions: &mut Vec<Action>,
    ) -> Result<()> {
        if success {
            // Replies can arrive out of order, so never move a follower's
            // progress backwards.
            let current = self.match_index.get(&from).copied().unwrap_or(0);
            if match_index > current {
                self.match_index.insert(from, match_index);
                self.next_index.insert(from, match_index + 1);
                self.maybe_commit();
                // More to send: carry straight on rather than waiting for
                // the next heartbeat, so a follower catching up moves at a
                // batch per round trip. Only on real progress, so a stale
                // or duplicate reply cannot start a second stream.
                if match_index < self.storage.last_index() {
                    actions.push(self.append_message_for(from)?);
                }
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
        actions.push(self.append_message_for(from)?);
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
            actions.push(self.append_message_for(peer)?);
        }
        Ok(())
    }

    /// What to send `peer` next: entries if the leader still has the ones
    /// it needs, or the next piece of the snapshot if it does not.
    fn append_message_for(&self, peer: NodeId) -> Result<Action> {
        let next = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or_else(|| self.storage.last_index() + 1);
        let snapshot = self.storage.snapshot_meta();

        if next <= snapshot.index {
            // The entries this follower needs are folded into the snapshot,
            // and `prev_log_index` would name one this leader no longer has
            // a term for. Send the state instead.
            let offset = match self.snapshot_progress.get(&peer) {
                Some(&(index, offset)) if index == snapshot.index => offset,
                // A newer snapshot than the one it was receiving: start over.
                _ => 0,
            };
            let data = self
                .storage
                .read_snapshot(offset, self.config.max_append_bytes.max(1))?;
            let done = offset + data.len() as u64 >= self.storage.snapshot_len();
            return Ok(Action::Send {
                to: peer,
                message: Message::InstallSnapshot {
                    term: self.term(),
                    last_index: snapshot.index,
                    last_term: snapshot.term,
                    offset,
                    data,
                    done,
                    seq: self.seq,
                },
            });
        }

        let prev_log_index = next - 1;
        Ok(Action::Send {
            to: peer,
            message: Message::AppendEntries {
                term: self.term(),
                prev_log_index,
                // Always known: `next` is past the snapshot, so the entry
                // before it is either held or is the snapshot's own index.
                prev_log_term: self.storage.term_at(prev_log_index).unwrap_or(0),
                entries: self
                    .storage
                    .entries_within(next, self.config.max_append_bytes),
                leader_commit: self.commit_index,
                seq: self.seq,
            },
        })
    }

    // -- reads ----------------------------------------------------------

    /// Record a read and start the round that will confirm it. Returns the
    /// round and the index the read must wait for.
    ///
    /// The index is the commit index, except on a leader that has not yet
    /// committed its no-op. Such a leader holds every entry committed
    /// before it, by the election rule, but does not yet know which of its
    /// entries those are; the no-op's own index is a safe bound, because
    /// everything committed before this term sits below it.
    fn open_read_round(&mut self, actions: &mut Vec<Action>) -> (u64, u64) {
        self.seq += 1;
        let index = self.commit_index.max(self.leader_start);
        let snapshot = self.storage.snapshot_meta().index;
        let peers = self.peers.clone();
        for peer in peers {
            let next = self
                .next_index
                .get(&peer)
                .copied()
                .unwrap_or_else(|| self.storage.last_index() + 1);
            // A follower being sent the snapshot answers the round with its
            // next reply, since the piece that prompts it carries it.
            if next <= snapshot {
                continue;
            }
            // A heartbeat with nothing in it: the round is the point, and
            // the entries go out as they always do.
            let prev_log_index = next - 1;
            actions.push(Action::Send {
                to: peer,
                message: Message::AppendEntries {
                    term: self.term(),
                    prev_log_index,
                    prev_log_term: self.storage.term_at(prev_log_index).unwrap_or(0),
                    entries: Vec::new(),
                    leader_commit: self.commit_index,
                    seq: self.seq,
                },
            });
        }
        (self.seq, index)
    }

    /// Whether a majority, counting this node, has answered `seq` or later.
    fn round_confirmed(&self, seq: u64) -> bool {
        let answered = self
            .peers
            .iter()
            .filter(|p| self.acked.get(p).is_some_and(|&a| a >= seq))
            .count();
        self.has_majority(answered + 1)
    }

    fn handle_read_index(&mut self, from: NodeId, id: u64, actions: &mut Vec<Action>) {
        if self.role != Role::Leader {
            actions.push(Action::Send {
                to: from,
                message: Message::ReadIndexReply {
                    term: self.term(),
                    id,
                    index: None,
                },
            });
            return;
        }
        let (seq, index) = self.open_read_round(actions);
        self.remote_reads.push(RemoteRead {
            from,
            id,
            seq,
            index,
        });
    }

    /// Answer every follower whose read's round a majority has now seen.
    fn release_reads(&mut self, actions: &mut Vec<Action>) {
        if self.remote_reads.is_empty() {
            return;
        }
        let term = self.term();
        let waiting = std::mem::take(&mut self.remote_reads);
        for read in waiting {
            if self.round_confirmed(read.seq) {
                actions.push(Action::Send {
                    to: read.from,
                    message: Message::ReadIndexReply {
                        term,
                        id: read.id,
                        index: Some(read.index),
                    },
                });
            } else {
                self.remote_reads.push(read);
            }
        }
    }

    // -- snapshots ------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    fn handle_install_snapshot(
        &mut self,
        from: NodeId,
        meta: SnapshotMeta,
        offset: u64,
        data: Vec<u8>,
        done: bool,
        seq: u64,
        actions: &mut Vec<Action>,
    ) -> Result<()> {
        // As with `AppendEntries`: the term was checked in `step`, so this
        // is the current leader, and hearing from it is a heartbeat.
        self.role = Role::Follower;
        self.leader_id = Some(from);
        self.reset_election_timer();

        let term = self.term();
        let reply = |next_offset: u64, done: bool| Action::Send {
            to: from,
            message: Message::InstallSnapshotReply {
                term,
                last_index: meta.index,
                next_offset,
                done,
                seq,
            },
        };

        // Everything it covers is already committed here, which means this
        // node already holds it, in its log or in its own snapshot.
        // Installing it would only move backwards.
        if meta.index <= self.commit_index {
            self.incoming = None;
            actions.push(reply(0, true));
            return Ok(());
        }

        let continuing = matches!(
            &self.incoming,
            Some((sender, sink)) if *sender == from && sink.meta() == meta
        );
        if !continuing {
            if offset != 0 {
                // A piece from the middle of a snapshot this node has no
                // start for. Ask for the beginning.
                actions.push(reply(0, false));
                return Ok(());
            }
            // Replacing an unfinished one drops it, and its part with it.
            self.incoming = Some((from, self.storage.new_snapshot(meta)?));
        }

        let (_, sink) = self.incoming.as_mut().expect("just ensured");
        let held = sink.written();
        if offset != held {
            // A gap, or a piece already seen. Either way, say where this
            // node has got to and let the leader carry on from there.
            actions.push(reply(held, false));
            return Ok(());
        }
        sink.write_all(&data)?;
        let held = sink.written();
        if !done {
            actions.push(reply(held, false));
            return Ok(());
        }

        let (_, sink) = self.incoming.take().expect("just used");
        self.storage.install_snapshot(sink)?;
        // The snapshot is applied state, so the state machine must pick it
        // up rather than wait for entries that no longer exist.
        self.commit_index = self.commit_index.max(meta.index);
        self.last_applied = self.last_applied.max(meta.index);
        actions.push(reply(held, true));
        Ok(())
    }

    fn handle_snapshot_reply(
        &mut self,
        from: NodeId,
        last_index: u64,
        next_offset: u64,
        done: bool,
        actions: &mut Vec<Action>,
    ) -> Result<()> {
        let current = self.storage.snapshot_meta().index;

        if done {
            self.snapshot_progress.remove(&from);
            let matched = self.match_index.get(&from).copied().unwrap_or(0);
            if last_index > matched {
                self.match_index.insert(from, last_index);
                self.next_index.insert(from, last_index + 1);
                self.maybe_commit();
            }
            // Whatever follows the snapshot goes out as ordinary entries,
            // or as a newer snapshot if this leader has compacted since.
            if self.next_index.get(&from).copied().unwrap_or(0) <= self.storage.last_index() {
                actions.push(self.append_message_for(from)?);
            }
            return Ok(());
        }

        let offset = if last_index == current {
            next_offset
        } else {
            // About a snapshot since replaced. Start the current one.
            0
        };
        self.snapshot_progress.insert(from, (current, offset));
        actions.push(self.append_message_for(from)?);
        Ok(())
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

    /// Say no to a canvasser without adopting its term or its premise.
    fn refuse_vote(&self, from: NodeId, message: &Message) -> Action {
        Action::Send {
            to: from,
            message: match message {
                Message::PreVote { .. } => Message::PreVoteReply {
                    term: self.term(),
                    granted: false,
                },
                _ => Message::RequestVoteReply {
                    term: self.term(),
                    granted: false,
                },
            },
        }
    }

    fn reply_stale(&self, from: NodeId, message: &Message) -> Action {
        let term = self.term();
        Action::Send {
            to: from,
            message: match message {
                Message::PreVote { .. } => Message::PreVoteReply {
                    term,
                    granted: false,
                },
                Message::RequestVote { .. } => Message::RequestVoteReply {
                    term,
                    granted: false,
                },
                Message::InstallSnapshot { last_index, .. } => Message::InstallSnapshotReply {
                    term,
                    last_index: *last_index,
                    next_offset: 0,
                    done: false,
                    seq: 0,
                },
                Message::ReadIndex { id, .. } => Message::ReadIndexReply {
                    term,
                    id: *id,
                    index: None,
                },
                _ => Message::AppendEntriesReply {
                    term,
                    success: false,
                    match_index: 0,
                    conflict_index: 0,
                    conflict_term: None,
                    seq: 0,
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
            seq: 0,
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

    // -- pre-vote -------------------------------------------------------

    fn pre_vote_request(term: u64, last_log_index: u64, last_log_term: u64) -> Message {
        Message::PreVote {
            term,
            last_log_index,
            last_log_term,
        }
    }

    fn pre_granted(actions: &[Action]) -> bool {
        match actions {
            [Action::Send {
                message: Message::PreVoteReply { granted, .. },
                ..
            }] => *granted,
            other => panic!("expected one pre-vote reply, got {other:?}"),
        }
    }

    fn pre_reply_term(actions: &[Action]) -> u64 {
        match actions {
            [Action::Send {
                message: Message::PreVoteReply { term, .. },
                ..
            }] => *term,
            other => panic!("expected one pre-vote reply, got {other:?}"),
        }
    }

    /// The property the whole mechanism rests on: answering the question
    /// costs the answerer nothing.
    #[test]
    fn answering_a_pre_vote_changes_nothing() {
        let mut node = follower(5, &[(1, 5)]);
        let before = node.storage().hard_state();

        // A proposed term far beyond ours, which a real RequestVote would
        // force us to adopt.
        let actions = node.step(2, pre_vote_request(99, 1, 5)).unwrap();
        assert!(pre_granted(&actions));

        assert_eq!(
            node.storage().hard_state(),
            before,
            "answering a hypothetical must not move the term or spend the vote"
        );
        assert_eq!(node.role(), Role::Follower);
    }

    /// And asking costs the asker nothing either, until the answer is yes.
    #[test]
    fn asking_does_not_raise_the_term() {
        let mut node = follower(5, &[(1, 5)]);
        node.pre_vote().unwrap();

        assert_eq!(node.role(), Role::PreCandidate);
        assert_eq!(node.term(), 5, "asking is not standing");
        assert_eq!(
            node.storage().hard_state().voted_for,
            None,
            "a pre-candidate has not voted for itself"
        );
    }

    #[test]
    fn a_pre_candidate_with_a_stale_log_is_refused() {
        let mut node = follower(5, &[(1, 3), (2, 5)]);
        assert!(
            !pre_granted(&node.step(2, pre_vote_request(6, 2, 3)).unwrap()),
            "a stale log must not be told an election is worth holding"
        );
        assert!(
            !pre_granted(&node.step(2, pre_vote_request(6, 1, 5)).unwrap()),
            "a shorter log at the same term must not either"
        );
        assert!(pre_granted(
            &node.step(2, pre_vote_request(6, 2, 5)).unwrap()
        ));
    }

    #[test]
    fn a_node_that_is_behind_is_refused_and_told_so() {
        let mut node = follower(9, &[(1, 9)]);
        // Its term is 5, so it proposes 6, which is still behind ours.
        let actions = node.step(2, pre_vote_request(6, 1, 9)).unwrap();
        assert!(!pre_granted(&actions));
        assert_eq!(
            pre_reply_term(&actions),
            9,
            "a refusal carries our term, which is how the asker learns"
        );
    }

    #[test]
    fn a_granted_reply_echoes_the_term_that_was_asked_about() {
        let mut node = follower(5, &[(1, 5)]);
        let actions = node.step(2, pre_vote_request(6, 1, 5)).unwrap();
        assert!(pre_granted(&actions));
        assert_eq!(
            pre_reply_term(&actions),
            6,
            "a yes echoes the proposed term so rounds can be told apart"
        );
    }

    /// A late yes from a previous round must not help win this one.
    #[test]
    fn a_reply_from_an_earlier_round_does_not_count() {
        let mut node = follower(5, &[(1, 5)]);
        node.pre_vote().unwrap();

        // Answers to a question this node is no longer asking.
        node.step(
            2,
            Message::PreVoteReply {
                term: 5,
                granted: true,
            },
        )
        .unwrap();
        node.step(
            3,
            Message::PreVoteReply {
                term: 5,
                granted: true,
            },
        )
        .unwrap();
        assert_eq!(
            node.role(),
            Role::PreCandidate,
            "stale answers carried a node into an election"
        );
        assert_eq!(node.term(), 5);
    }

    /// Enough answers to this round, and only then is the term worth
    /// spending.
    #[test]
    fn a_majority_of_yeses_starts_a_real_election() {
        let mut node = follower(5, &[(1, 5)]);
        node.pre_vote().unwrap();
        let actions = node
            .step(
                2,
                Message::PreVoteReply {
                    term: 6,
                    granted: true,
                },
            )
            .unwrap();

        assert_eq!(node.role(), Role::Candidate);
        assert_eq!(node.term(), 6, "now the term moves");
        assert_eq!(
            node.storage().hard_state().voted_for,
            Some(1),
            "and now it votes for itself, durably"
        );
        assert!(
            actions
                .iter()
                .any(|Action::Send { message, .. }| matches!(message, Message::RequestVote { .. })),
            "it should be canvassing for real now"
        );
    }

    /// A no from someone further ahead is how a node that has been away
    /// discovers it is the one that is behind.
    #[test]
    fn a_refusal_from_a_later_term_makes_a_pre_candidate_stand_down() {
        let mut node = follower(5, &[(1, 5)]);
        node.pre_vote().unwrap();
        node.step(
            2,
            Message::PreVoteReply {
                term: 20,
                granted: false,
            },
        )
        .unwrap();

        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.term(), 20, "it caught up to the term it was told");
    }

    /// While a leader is being heard from, nobody canvassing gets a
    /// hearing, whether the question is hypothetical or not.
    #[test]
    fn a_node_hearing_from_a_leader_refuses_everyone() {
        let mut node = follower(5, &[(1, 5)]);
        // A heartbeat, which makes node 2 the leader and starts the lease.
        node.step(2, append(5, (1, 5), &[], 0)).unwrap();
        assert_eq!(node.leader(), Some(2));

        assert!(
            !pre_granted(&node.step(3, pre_vote_request(6, 1, 5)).unwrap()),
            "a pre-vote was granted while a leader was live"
        );
        assert!(
            !granted(&node.step(3, vote_request(6, 1, 5)).unwrap()),
            "a vote was granted while a leader was live"
        );
        assert_eq!(
            node.term(),
            5,
            "and neither request was allowed to move the term"
        );
    }

    /// The lease has to run out, or a cluster whose leader has died would
    /// never replace it.
    #[test]
    fn loyalty_ends_once_the_leader_goes_quiet() {
        let mut node = follower(5, &[(1, 5)]);
        node.step(2, append(5, (1, 5), &[], 0)).unwrap();
        assert!(!pre_granted(
            &node.step(3, pre_vote_request(6, 1, 5)).unwrap()
        ));

        node.expire_election_timer();
        assert!(
            pre_granted(&node.step(3, pre_vote_request(6, 1, 5)).unwrap()),
            "a node held its loyalty past any evidence the leader was alive"
        );
    }

    // -- snapshots ------------------------------------------------------

    fn snap(index: u64, term: u64) -> SnapshotMeta {
        SnapshotMeta { index, term }
    }

    /// A follower whose log is `spec`, folded into a snapshot up to
    /// `through`.
    fn compacted_follower(
        term: u64,
        spec: &[(u64, u64)],
        through: SnapshotMeta,
    ) -> Node<MemStorage> {
        let mut node = follower(term, spec);
        node.storage.save_snapshot(through, b"state").unwrap();
        node
    }

    fn install(term: u64, meta: SnapshotMeta, offset: u64, data: &[u8], done: bool) -> Message {
        Message::InstallSnapshot {
            term,
            last_index: meta.index,
            last_term: meta.term,
            offset,
            data: data.to_vec(),
            done,
            seq: 0,
        }
    }

    /// (next_offset, done) from the single reply a node sent.
    fn snapshot_reply(actions: &[Action]) -> (u64, bool) {
        match actions {
            [Action::Send {
                message:
                    Message::InstallSnapshotReply {
                        next_offset, done, ..
                    },
                ..
            }] => (*next_offset, *done),
            other => panic!("expected one snapshot reply, got {other:?}"),
        }
    }

    #[test]
    fn append_entries_that_start_inside_the_snapshot_are_accepted() {
        let mut node = compacted_follower(1, &[(1, 1), (2, 1), (3, 1)], snap(3, 1));
        // The leader thinks this follower needs everything from 2.
        let message = append(1, (1, 1), &[(2, 1), (3, 1), (4, 1), (5, 1)], 0);
        assert!(
            accepted(&node.step(2, message).unwrap()),
            "the overlap with the snapshot should be skipped, not refused"
        );
        assert_eq!(node.last_index(), 5);
        assert_eq!(node.storage().first_index(), 4);
    }

    #[test]
    fn append_entries_entirely_inside_the_snapshot_report_the_snapshot() {
        let mut node = compacted_follower(1, &[(1, 1), (2, 1), (3, 1)], snap(3, 1));
        let actions = node
            .step(2, append(1, (0, 0), &[(1, 1), (2, 1)], 0))
            .unwrap();
        match actions.as_slice() {
            [Action::Send {
                message:
                    Message::AppendEntriesReply {
                        success,
                        match_index,
                        ..
                    },
                ..
            }] => {
                assert!(success);
                assert_eq!(*match_index, 3, "everything to the snapshot is held");
            }
            other => panic!("expected a success, got {other:?}"),
        }
    }

    #[test]
    fn a_snapshot_arriving_in_pieces_is_installed_whole() {
        let mut node = follower(2, &[]);
        let meta = snap(40, 2);

        assert_eq!(
            snapshot_reply(&node.step(9, install(2, meta, 0, b"abc", false)).unwrap()),
            (3, false)
        );
        assert_eq!(
            snapshot_reply(&node.step(9, install(2, meta, 3, b"def", false)).unwrap()),
            (6, false)
        );
        assert_eq!(
            node.storage().snapshot_meta(),
            SnapshotMeta::default(),
            "not until the last piece"
        );
        assert_eq!(
            snapshot_reply(&node.step(9, install(2, meta, 6, b"g", true)).unwrap()),
            (7, true)
        );

        assert_eq!(node.storage().snapshot_meta(), meta);
        assert_eq!(node.storage().read_snapshot(0, 100).unwrap(), b"abcdefg");
        assert_eq!(node.commit_index(), 40, "a snapshot is committed state");
        assert!(
            node.take_committed().is_empty(),
            "and there are no entries to apply"
        );
        assert_eq!(node.last_index(), 40);
    }

    #[test]
    fn a_missed_piece_is_asked_for_again() {
        let mut node = follower(2, &[]);
        let meta = snap(40, 2);
        node.step(9, install(2, meta, 0, b"abc", false)).unwrap();
        // The piece at 3 went missing; the one at 6 arrives.
        assert_eq!(
            snapshot_reply(&node.step(9, install(2, meta, 6, b"ghi", false)).unwrap()),
            (3, false),
            "the follower should say where it actually is"
        );
        node.step(9, install(2, meta, 3, b"def", true)).unwrap();
        assert_eq!(node.storage().read_snapshot(0, 100).unwrap(), b"abcdef");
    }

    #[test]
    fn a_repeated_piece_is_not_appended_twice() {
        let mut node = follower(2, &[]);
        let meta = snap(40, 2);
        node.step(9, install(2, meta, 0, b"abc", false)).unwrap();
        assert_eq!(
            snapshot_reply(&node.step(9, install(2, meta, 0, b"abc", false)).unwrap()),
            (3, false)
        );
        node.step(9, install(2, meta, 3, b"d", true)).unwrap();
        assert_eq!(node.storage().read_snapshot(0, 100).unwrap(), b"abcd");
    }

    #[test]
    fn pieces_from_two_leaders_are_never_spliced() {
        let mut node = follower(2, &[]);
        let meta = snap(40, 2);
        node.step(8, install(2, meta, 0, b"from-8:", false))
            .unwrap();
        // A different leader, the same snapshot, but it starts from its own
        // beginning. Continuing node 8's buffer with node 9's bytes would
        // produce a snapshot neither of them sent.
        assert_eq!(
            snapshot_reply(&node.step(9, install(3, meta, 7, b"tail", true)).unwrap()),
            (0, false),
            "a mid-snapshot piece from someone new must restart the transfer"
        );
        node.step(9, install(3, meta, 0, b"from-9", true)).unwrap();
        assert_eq!(node.storage().read_snapshot(0, 100).unwrap(), b"from-9");
    }

    #[test]
    fn a_snapshot_behind_what_is_committed_is_not_installed() {
        let mut node = follower(2, &[(1, 1), (2, 1), (3, 2)]);
        node.step(9, append(2, (3, 2), &[], 3)).unwrap();
        assert_eq!(node.commit_index(), 3);

        assert_eq!(
            snapshot_reply(
                &node
                    .step(9, install(2, snap(2, 1), 0, b"old", true))
                    .unwrap()
            ),
            (0, true),
            "it already has all of that"
        );
        assert_eq!(node.storage().snapshot_meta(), SnapshotMeta::default());
        assert_eq!(node.last_index(), 3, "and nothing was thrown away");
    }

    #[test]
    fn installing_over_a_log_that_agrees_keeps_its_tail() {
        let mut node = follower(2, &[(1, 1), (2, 1), (3, 2), (4, 2)]);
        node.step(9, install(2, snap(3, 2), 0, b"s", true)).unwrap();
        assert_eq!(
            node.last_index(),
            4,
            "entry 4 agrees with the snapshot, so it stays"
        );
    }

    #[test]
    fn installing_over_a_log_that_disagrees_discards_it() {
        let mut node = follower(3, &[(1, 1), (2, 1), (3, 1), (4, 1)]);
        node.step(9, install(3, snap(3, 3), 0, b"s", true)).unwrap();
        assert_eq!(node.last_index(), 3);
        assert_eq!(node.storage().entry(4), None);
    }

    #[test]
    fn a_restart_starts_from_the_snapshot() {
        let node = compacted_follower(2, &[(1, 1), (2, 1), (3, 2)], snap(3, 2));
        let storage = node.into_storage();
        let mut node = Node::new(1, vec![1, 2, 3], Config::default(), storage);
        assert_eq!(node.commit_index(), 3);
        assert!(
            node.take_committed().is_empty(),
            "nothing folded away is handed out again"
        );
    }

    #[test]
    fn only_applied_state_can_be_compacted() {
        let mut node = follower(1, &[(1, 1), (2, 1), (3, 1)]);
        node.step(9, append(1, (3, 1), &[], 3)).unwrap();
        assert!(
            !node.compact(3, b"x").unwrap(),
            "committed but not handed out yet"
        );
        assert_eq!(node.take_committed().len(), 3);
        assert!(node.compact(2, b"x").unwrap());
        assert!(
            !node.compact(2, b"x").unwrap(),
            "no newer than what is there"
        );
        assert_eq!(node.storage().first_index(), 3);
    }

    // -- reads ----------------------------------------------------------

    fn ack(term: u64, seq: u64) -> Message {
        Message::AppendEntriesReply {
            term,
            success: true,
            match_index: 0,
            conflict_index: 0,
            conflict_term: None,
            seq,
        }
    }

    fn read_reply(actions: &[Action]) -> Option<(NodeId, u64, Option<u64>)> {
        actions.iter().find_map(|a| match a {
            Action::Send {
                to,
                message: Message::ReadIndexReply { id, index, .. },
            } => Some((*to, *id, *index)),
            _ => None,
        })
    }

    /// The whole of the read index rests on this. A reply proves the
    /// follower still recognised this leader when it sent it, so only a
    /// reply to a message sent after the read began says anything about
    /// the moment the read began.
    #[test]
    fn a_read_is_confirmed_only_by_answers_to_a_later_round() {
        let mut node = follower(2, &[(1, 2)]);
        node.leader_with(&[(2, 1), (3, 1)]);
        node.commit_index = 1;

        let (first, _) = node.read_index();
        let (second, actions) = node.read_index();
        let heartbeats = actions
            .iter()
            .filter(|a| matches!(a, Action::Send { message: Message::AppendEntries { entries, .. }, .. } if entries.is_empty()))
            .count();
        assert_eq!(heartbeats, 2, "a read sends a heartbeat to every follower");
        assert_eq!(node.read_state(&second), ReadState::Pending);

        // Both followers answer the first read's round.
        node.step(2, ack(2, 1)).unwrap();
        node.step(3, ack(2, 1)).unwrap();
        assert_eq!(node.read_state(&first), ReadState::Ready(1));
        assert_eq!(
            node.read_state(&second),
            ReadState::Pending,
            "answers to an earlier round confirmed a later read"
        );

        // One answer to the second round, with the leader, is a majority.
        node.step(3, ack(2, 2)).unwrap();
        assert_eq!(node.read_state(&second), ReadState::Ready(1));
    }

    /// A leader straight out of an election has every committed entry but
    /// does not yet know which ones they are. Its read has to wait for its
    /// own no-op, since everything committed before this term sits below
    /// it; its own commit index could be well short of that.
    #[test]
    fn a_read_on_a_new_leader_covers_everything_before_its_term() {
        let mut node = follower(1, &[(1, 1), (2, 1)]);
        node.campaign().unwrap();
        node.step(
            2,
            Message::RequestVoteReply {
                term: 2,
                granted: true,
            },
        )
        .unwrap();
        assert!(node.is_leader());
        assert_eq!(node.commit_index(), 0, "nothing known to be committed yet");

        let (read, _) = node.read_index();
        node.step(2, ack(2, node.seq)).unwrap();
        assert_eq!(
            node.read_state(&read),
            ReadState::Ready(3),
            "the read index stopped short of the new leader's no-op"
        );
    }

    #[test]
    fn a_leader_that_loses_office_fails_its_reads() {
        let mut node = follower(2, &[(1, 2)]);
        node.leader_with(&[(2, 1), (3, 1)]);
        let (read, _) = node.read_index();
        node.step(3, append(3, (1, 2), &[], 1)).unwrap();
        assert!(!node.is_leader());
        assert_eq!(node.read_state(&read), ReadState::Failed);
    }

    /// The leader's half of a follower's read: nothing is sent back until a
    /// round started after the question has been answered by a majority.
    #[test]
    fn a_follower_is_answered_once_its_round_is_confirmed() {
        let mut node = follower(2, &[(1, 2)]);
        node.leader_with(&[(2, 1), (3, 1)]);
        node.commit_index = 1;
        // An earlier round, before the question.
        let (_, _) = node.read_index();

        let actions = node
            .step(2, Message::ReadIndex { term: 2, id: 41 })
            .unwrap();
        assert_eq!(read_reply(&actions), None, "answered before confirming");
        let round = node.seq;

        let actions = node.step(3, ack(2, round - 1)).unwrap();
        assert_eq!(read_reply(&actions), None, "an older round confirmed it");
        let actions = node.step(3, ack(2, round)).unwrap();
        assert_eq!(read_reply(&actions), Some((2, 41, Some(1))));
    }

    #[test]
    fn a_node_that_is_not_leading_refuses_a_read() {
        let mut node = follower(2, &[(1, 2)]);
        let actions = node.step(2, Message::ReadIndex { term: 2, id: 7 }).unwrap();
        assert_eq!(read_reply(&actions), Some((2, 7, None)));
    }

    /// The follower's half: it asks the leader it follows, and takes the
    /// answer when it comes.
    #[test]
    fn a_forwarded_read_takes_the_leaders_answer() {
        let mut node = follower(1, &[(1, 1)]);
        node.step(2, append(1, (1, 1), &[], 1)).unwrap();
        assert_eq!(node.leader(), Some(2));

        let (read, actions) = node.read_index();
        let id = match actions.as_slice() {
            [Action::Send {
                to: 2,
                message: Message::ReadIndex { id, .. },
            }] => *id,
            other => panic!("expected one question to the leader, got {other:?}"),
        };
        assert_eq!(node.read_state(&read), ReadState::Pending);
        node.step(
            2,
            Message::ReadIndexReply {
                term: 1,
                id,
                index: Some(9),
            },
        )
        .unwrap();
        assert_eq!(node.read_state(&read), ReadState::Ready(9));

        // A refusal fails it, and so does the leader changing under it.
        let (refused, actions) = node.read_index();
        let Some(Action::Send {
            message: Message::ReadIndex { id, .. },
            ..
        }) = actions.first()
        else {
            panic!("no question asked")
        };
        node.step(
            2,
            Message::ReadIndexReply {
                term: 1,
                id: *id,
                index: None,
            },
        )
        .unwrap();
        assert_eq!(node.read_state(&refused), ReadState::Failed);

        let (orphaned, _) = node.read_index();
        node.step(3, append(2, (1, 1), &[], 1)).unwrap();
        assert_eq!(node.leader(), Some(3));
        assert_eq!(node.read_state(&orphaned), ReadState::Failed);

        node.forget_read(&read);
        assert_eq!(node.read_state(&read), ReadState::Failed, "forgotten");
    }

    #[test]
    fn a_node_with_no_leader_to_ask_fails_the_read_at_once() {
        let mut node = follower(1, &[]);
        let (read, actions) = node.read_index();
        assert!(actions.is_empty());
        assert_eq!(node.read_state(&read), ReadState::Failed);
    }

    /// A snapshot is written while the node carries on, so a leader can
    /// install a newer one in the meantime. The older one must not then
    /// replace it.
    #[test]
    fn a_compaction_overtaken_by_an_installed_snapshot_is_dropped() {
        let mut node = follower(1, &[(1, 1), (2, 1), (3, 1)]);
        node.step(9, append(1, (3, 1), &[], 3)).unwrap();
        node.take_committed();

        let mut ours = node
            .begin_compaction(2)
            .unwrap()
            .expect("index 2 is applied");
        ours.write_all(b"ours, as of 2").unwrap();

        node.step(9, install(1, snap(5, 1), 0, b"theirs, as of 5", true))
            .unwrap();
        assert!(
            !node.finish_compaction(ours).unwrap(),
            "an older snapshot was reported as installed"
        );
        assert_eq!(node.storage().snapshot_meta(), snap(5, 1));
        assert_eq!(
            node.storage().read_snapshot(0, 100).unwrap(),
            b"theirs, as of 5"
        );
    }

    /// The leader's half: a follower that needs what the leader has folded
    /// away is sent the snapshot a budget's worth at a time, and resumes
    /// with ordinary entries once it has it.
    #[test]
    fn a_leader_sends_a_snapshot_in_pieces_then_carries_on_with_entries() {
        let config = Config {
            max_append_bytes: 100,
            ..Config::default()
        };
        let mut storage = MemStorage::new();
        storage
            .save_hard_state(HardState {
                term: 2,
                voted_for: Some(1),
            })
            .unwrap();
        let log: Vec<Entry> = (1..=12)
            .map(|index| Entry {
                term: 2,
                index,
                command: Command::Noop,
            })
            .collect();
        storage.append(&log).unwrap();
        storage.save_snapshot(snap(10, 2), &[7u8; 250]).unwrap();
        let mut node = Node::new(1, vec![1, 2, 3], config, storage);
        node.last_applied = 12;
        node.leader_with(&[(2, 0), (3, 12)]);

        let mut offset = 0;
        let mut pieces = 0;
        let mut action = node.append_message_for(2).unwrap();
        loop {
            let Action::Send {
                message:
                    Message::InstallSnapshot {
                        offset: sent_at,
                        data,
                        done,
                        last_index,
                        ..
                    },
                ..
            } = action
            else {
                panic!("expected a snapshot piece, got {action:?}");
            };
            assert_eq!(sent_at, offset);
            assert!(data.len() <= 100, "a piece went over the budget");
            assert_eq!(last_index, 10);
            pieces += 1;
            offset += data.len() as u64;

            let reply = Message::InstallSnapshotReply {
                term: 2,
                last_index: 10,
                next_offset: offset,
                done,
                seq: 0,
            };
            let actions = node.step(2, reply).unwrap();
            if done {
                // Past the snapshot, the rest is ordinary log.
                match actions.as_slice() {
                    [Action::Send {
                        message:
                            Message::AppendEntries {
                                prev_log_index,
                                entries,
                                ..
                            },
                        ..
                    }] => {
                        assert_eq!(*prev_log_index, 10);
                        assert_eq!(
                            entries.iter().map(|e| e.index).collect::<Vec<_>>(),
                            vec![11, 12]
                        );
                    }
                    other => panic!("expected entries after the snapshot, got {other:?}"),
                }
                break;
            }
            action = actions.into_iter().next().expect("the next piece");
        }
        assert_eq!(pieces, 3, "250 bytes at 100 a piece");
        assert_eq!(node.match_index.get(&2), Some(&10));
    }

    /// A reply about a snapshot the leader has since replaced starts the
    /// new one from the beginning, rather than continuing it from an
    /// offset that belonged to different bytes.
    #[test]
    fn a_reply_about_an_older_snapshot_restarts_the_transfer() {
        let config = Config {
            max_append_bytes: 100,
            ..Config::default()
        };
        let mut storage = MemStorage::new();
        storage
            .save_hard_state(HardState {
                term: 2,
                voted_for: Some(1),
            })
            .unwrap();
        let log: Vec<Entry> = (1..=20)
            .map(|index| Entry {
                term: 2,
                index,
                command: Command::Noop,
            })
            .collect();
        storage.append(&log).unwrap();
        storage.save_snapshot(snap(20, 2), &[1u8; 300]).unwrap();
        let mut node = Node::new(1, vec![1, 2, 3], config, storage);
        node.leader_with(&[(2, 0), (3, 20)]);

        let actions = node
            .step(
                2,
                Message::InstallSnapshotReply {
                    term: 2,
                    last_index: 10,
                    next_offset: 200,
                    done: false,
                    seq: 0,
                },
            )
            .unwrap();
        match actions.as_slice() {
            [Action::Send {
                message:
                    Message::InstallSnapshot {
                        offset, last_index, ..
                    },
                ..
            }] => {
                assert_eq!(*last_index, 20);
                assert_eq!(
                    *offset, 0,
                    "an offset into the old snapshot means nothing in the new one"
                );
            }
            other => panic!("expected the new snapshot from the start, got {other:?}"),
        }
    }
}

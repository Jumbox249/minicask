//! The two RPCs Raft needs, and their replies.
//!
//! The sender's identity is not in the message. It travels with it, as the
//! `from` argument to [`Node::step`](super::Node::step) and the `to` field
//! of an [`Action`](super::Action), which keeps the transport free to
//! authenticate the peer however it likes rather than trusting a field.

use super::log::{Entry, NodeId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// A hypothetical question: *if* I stood in `term`, would you vote for
    /// me? Asking costs the asker nothing and changes nobody's term, which
    /// is the entire point.
    ///
    /// A node that has been cut off spends the partition timing out and
    /// standing for election. Without this it raises its term each time,
    /// and on returning it forces a healthy leader to stand down and the
    /// cluster to hold an election it cannot win. Asking first means its
    /// term never moves while it is away.
    PreVote {
        /// The term it *would* run in, which is one past its own. Nobody
        /// adopts this term; it is a question, not a claim.
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    /// A granted reply echoes the term that was asked about, so a late
    /// reply from an earlier round is not counted twice. A refusal instead
    /// carries the responder's own term, which is how a node that has
    /// fallen behind finds out.
    PreVoteReply {
        term: u64,
        granted: bool,
    },
    /// A candidate asking for a vote in `term`. The log position lets the
    /// receiver refuse a candidate whose log is behind its own, which is
    /// what stops a stale node from being elected and erasing entries.
    RequestVote {
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteReply {
        term: u64,
        granted: bool,
    },
    /// The leader's one message: log entries to append, a heartbeat when
    /// `entries` is empty, and the commit index rides along with both.
    ///
    /// `prev_log_index` and `prev_log_term` are the induction step. The
    /// follower only accepts if it has that exact entry, so accepting
    /// proves the two logs are identical up to that point.
    AppendEntries {
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    },
    /// The state a follower needs but the leader can no longer send as
    /// entries, because it has folded them into a snapshot. Sent in pieces
    /// of about `max_append_bytes`, each acknowledged before the next.
    InstallSnapshot {
        term: u64,
        /// The index and term of the last entry the snapshot covers.
        last_index: u64,
        last_term: u64,
        /// Where in the snapshot's data this piece begins.
        offset: u64,
        data: Vec<u8>,
        /// Whether this is the final piece.
        done: bool,
    },
    InstallSnapshotReply {
        term: u64,
        /// Echoed, so the leader can tell a reply about the snapshot it is
        /// sending from one about a snapshot it has since replaced.
        last_index: u64,
        /// How much of the snapshot the follower now holds, which is where
        /// the leader carries on from. A follower that missed a piece, or
        /// saw one twice, says so here rather than failing.
        next_offset: u64,
        /// The snapshot is installed.
        done: bool,
    },
    AppendEntriesReply {
        term: u64,
        success: bool,
        /// On success, the highest index the follower now has from this
        /// leader. Sent explicitly rather than inferred, because a reply
        /// may arrive out of order or be a duplicate.
        match_index: u64,
        /// On failure, where the leader should resume from. Without this a
        /// leader rewinds one index per round trip, which takes as many
        /// round trips as the logs differ by.
        conflict_index: u64,
        /// On failure, the term of the entry that did not match, if the
        /// follower had one there at all.
        conflict_term: Option<u64>,
    },
}

impl Message {
    /// Every message carries a term, and the first rule of the protocol is
    /// about comparing it to your own.
    pub fn term(&self) -> u64 {
        match self {
            Message::PreVote { term, .. }
            | Message::PreVoteReply { term, .. }
            | Message::RequestVote { term, .. }
            | Message::RequestVoteReply { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesReply { term, .. }
            | Message::InstallSnapshot { term, .. }
            | Message::InstallSnapshotReply { term, .. } => *term,
        }
    }

    /// Whether this asks for a vote, real or hypothetical. Both are
    /// refused by a node that is still hearing from a leader.
    pub fn is_vote_request(&self) -> bool {
        matches!(self, Message::PreVote { .. } | Message::RequestVote { .. })
    }

    /// True for the messages a peer sends unprompted. Replies to a
    /// stale request are not evidence that anyone is alive and leading.
    pub fn is_request(&self) -> bool {
        matches!(
            self,
            Message::PreVote { .. }
                | Message::RequestVote { .. }
                | Message::AppendEntries { .. }
                | Message::InstallSnapshot { .. }
        )
    }
}

/// What a node wants done after handling a tick or a message. The node
/// never touches a socket itself; it says what to send and the transport
/// decides how.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Send { to: NodeId, message: Message },
}

//! The two RPCs Raft needs, and their replies.
//!
//! The sender's identity is not in the message. It travels with it, as the
//! `from` argument to [`Node::step`](super::Node::step) and the `to` field
//! of an [`Action`](super::Action), which keeps the transport free to
//! authenticate the peer however it likes rather than trusting a field.

use super::log::{Entry, NodeId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
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
            Message::RequestVote { term, .. }
            | Message::RequestVoteReply { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesReply { term, .. } => *term,
        }
    }

    /// True for the two messages a peer sends unprompted. Replies to a
    /// stale request are not evidence that anyone is alive and leading.
    pub fn is_request(&self) -> bool {
        matches!(
            self,
            Message::RequestVote { .. } | Message::AppendEntries { .. }
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

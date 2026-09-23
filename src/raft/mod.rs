//! Raft: leader election and log replication, so that a set of nodes agree
//! on one ordered sequence of commands even when some of them are down or
//! cut off from the rest.
//!
//! The consensus layer here is a state machine and nothing else. It owns no
//! threads, opens no sockets and reads no clock. Time is delivered with
//! [`Node::tick`], the network with [`Node::step`], and both hand back a
//! list of [`Action`]s naming the messages to send. Everything is a pure
//! function of the state and the input.
//!
//! That is a deliberate shape rather than an aesthetic one. A consensus bug
//! is usually a timing bug, and timing bugs are only reproducible if the
//! timing is something the test controls. `tests/raft.rs` drives whole
//! clusters through partitions, leader kills and restarts without a single
//! sleep, and every run is identical.
//!
//! ```
//! use minicask::raft::{Config, MemStorage, Node, Role};
//!
//! // A cluster of one is its own majority, so it elects itself.
//! let mut node = Node::new(1, vec![1], Config::default(), MemStorage::new());
//! for _ in 0..25 {
//!     node.tick()?;
//! }
//! assert_eq!(node.role(), Role::Leader);
//!
//! let accepted = node.propose(b"set x 1".to_vec()).expect("leader accepts");
//! assert_eq!(node.commit_index(), accepted.index);
//! # Ok::<(), minicask::Error>(())
//! ```

mod log;
mod message;
mod node;
mod storage;
pub mod wire;

pub use log::{
    decode_members, encode_members, Command, Entry, HardState, MemSnapshot, MemStorage, Member,
    NodeId, SnapshotMeta, SnapshotSink, Storage, StorageExt,
};
pub use message::{Action, Message};
pub use node::{Accepted, Config, Node, ProposeError, ReadRequest, ReadState, Role};
pub use storage::{DiskSnapshot, DiskStorage};

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

use crate::error::{Error, Result};
use crate::raft::{Accepted, Action, Command, Message, Node, NodeId, ProposeError, Role, Storage};
use crate::record::{self, Header, HEADER_LEN};
use crate::store::Store;

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
}

impl<S: Storage> ReplicatedStore<S> {
    /// Join a consensus node to a store.
    ///
    /// The store may already hold the effects of entries in the log; a
    /// restart replays them, which is safe because applying the same
    /// ordered writes twice lands in the same place.
    pub fn new(node: Node<S>, store: Store) -> ReplicatedStore<S> {
        ReplicatedStore {
            node,
            store,
            applied: 0,
        }
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
        for entry in self.node.take_committed() {
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

//! `minicask` is a small embedded key/value store built the way Bitcask is:
//! every write appends to a log file, and an in-memory index remembers where
//! each key's newest record lives.
//!
//! That buys three things worth having. A read is one hash lookup and one
//! seek. A write is one append, so there is no read-modify-write cycle to be
//! interrupted. And recovery after a crash is a replay of the log rather than
//! a repair of it, because no byte on disk is ever overwritten.
//!
//! The price is that the index holds every key in memory, and that space from
//! overwritten or deleted records only comes back when you compact.
//!
//! ```no_run
//! use minicask::Store;
//!
//! let mut store = Store::open("./my-data")?;
//! store.put(b"language", b"rust")?;
//! assert_eq!(store.get(b"language")?, Some(b"rust".to_vec()));
//! store.delete(b"language")?;
//! # Ok::<(), minicask::Error>(())
//! ```

mod cluster;
mod crc;
mod error;
mod hint;
mod log;
pub mod raft;
mod record;
mod replicated;
pub mod resp;
mod server;
mod store;

pub use cluster::{ClusterConfig, ClusterNode, Peer};
pub use error::{Error, Result};
pub use log::SyncPolicy;
pub use replicated::{Op, ReplicatedStore, SnapshotJob, DEFAULT_SNAPSHOT_EVERY};
pub use server::Server;
pub use store::{CompactReport, Location, Options, Stats, Store};

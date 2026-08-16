#![forbid(unsafe_code)]
//! Python-compatible Agent Broker storage formats and deterministic recovery primitives.
//!
//! Filesystem durability/atomic-snapshot repository composition is layered above these codecs. The
//! codec/replay layer remains independent from provider runtimes and serializes only the logical
//! [`agent_broker_domain::BrokerCheckpoint`] contract.

mod error;
mod filesystem_durability;
mod journal_v1;
mod repository;
mod snapshot_v1;

pub use error::{RepositoryError, StorageError};
pub use journal_v1::{
    JournalMutation, apply_journal_mutation, decode_journal_mutation, encode_journal_mutation,
};
pub use repository::{
    BrokerStateRepository, JournalCompactionPolicy, JournaledBrokerStateRepository,
};
pub use snapshot_v1::{decode_snapshot, encode_snapshot};

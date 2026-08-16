use std::error::Error;
use std::fmt;
use std::io;

use agent_broker_domain::{CheckpointError, Revision, Term};

/// Typed storage-format/replay failure. Filesystem failures are added by the repository layer.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StorageError {
    InvalidFormat(String),
    UnsupportedSchemaVersion(u64),
    RevisionGap {
        expected: Revision,
        actual: Revision,
    },
    BackwardTerm {
        current: Term,
        incoming: Term,
    },
    EntityRevisionRollback {
        entity: &'static str,
    },
    Checkpoint(CheckpointError),
    Serialization(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormat(message) | Self::Serialization(message) => {
                formatter.write_str(message)
            }
            Self::UnsupportedSchemaVersion(version) => {
                write!(
                    formatter,
                    "unsupported Broker storage schema version {version}"
                )
            }
            Self::RevisionGap { expected, actual } => write!(
                formatter,
                "journal revision gap: expected {}, received {}",
                expected.get(),
                actual.get()
            ),
            Self::BackwardTerm { current, incoming } => write!(
                formatter,
                "journal term moved backward: current {}, incoming {}",
                current.get(),
                incoming.get()
            ),
            Self::EntityRevisionRollback { entity } => {
                write!(
                    formatter,
                    "{entity} local revision moved backward during replay"
                )
            }
            Self::Checkpoint(error) => write!(formatter, "invalid Broker checkpoint: {error}"),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Checkpoint(error) => Some(error),
            _ => None,
        }
    }
}

/// Filesystem/configuration failure around the durable repository boundary.
#[derive(Debug)]
pub enum RepositoryError {
    InvalidConfiguration(&'static str),
    Storage(StorageError),
    Io {
        operation: &'static str,
        source: io::Error,
    },
}

impl RepositoryError {
    pub(crate) fn io(operation: &'static str, source: io::Error) -> Self {
        Self::Io { operation, source }
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => formatter.write_str(message),
            Self::Storage(error) => error.fmt(formatter),
            Self::Io { operation, source } => write!(formatter, "{operation}: {source}"),
        }
    }
}

impl Error for RepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            Self::InvalidConfiguration(_) => None,
        }
    }
}

impl From<StorageError> for RepositoryError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<CheckpointError> for StorageError {
    fn from(error: CheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

use std::error::Error;
use std::fmt;
use std::io;

use agent_broker_protocol::ProtocolCodecError;

/// Process/runtime composition failures outside the typed Broker application boundary.
#[derive(Debug)]
pub enum RuntimeError {
    InvalidConfiguration(&'static str),
    StateAlreadyOwned,
    Io {
        operation: &'static str,
        source: io::Error,
    },
    StateOwnerSaturated,
    StateOwnerStopped,
    StateOwnerReplyDropped,
    ClockBeforeUnixEpoch,
    Protocol(ProtocolCodecError),
}

impl RuntimeError {
    pub(crate) fn io(operation: &'static str, source: io::Error) -> Self {
        Self::Io { operation, source }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => formatter.write_str(message),
            Self::StateAlreadyOwned => {
                formatter.write_str("Broker state is already owned by another process")
            }
            Self::Io { operation, source } => write!(formatter, "{operation}: {source}"),
            Self::StateOwnerSaturated => {
                formatter.write_str("Broker state owner queue is saturated")
            }
            Self::StateOwnerStopped => formatter.write_str("Broker state owner is not running"),
            Self::StateOwnerReplyDropped => {
                formatter.write_str("Broker state owner dropped a response")
            }
            Self::ClockBeforeUnixEpoch => {
                formatter.write_str("system clock is before the Unix epoch")
            }
            Self::Protocol(error) => error.fmt(formatter),
        }
    }
}

impl Error for RuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Protocol(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ProtocolCodecError> for RuntimeError {
    fn from(error: ProtocolCodecError) -> Self {
        Self::Protocol(error)
    }
}

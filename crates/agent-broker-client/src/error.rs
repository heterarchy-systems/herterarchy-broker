use std::error::Error;
use std::fmt;
use std::io;

use agent_broker_application::BrokerError;
use agent_broker_protocol::{Operation, ProtocolCodecError};

/// Stable synchronous client failure categories without automatic mutation retries.
#[derive(Debug)]
pub enum ClientError {
    Transport(io::Error),
    Protocol(ProtocolCodecError),
    Broker(BrokerError),
    UnexpectedPayload(Operation),
    RequestIdExhausted,
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "Broker transport failed: {error}"),
            Self::Protocol(error) => error.fmt(formatter),
            Self::Broker(error) => error.fmt(formatter),
            Self::UnexpectedPayload(operation) => {
                write!(
                    formatter,
                    "Broker response payload does not match {operation}"
                )
            }
            Self::RequestIdExhausted => formatter.write_str("Broker client request IDs exhausted"),
        }
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Protocol(error) => Some(error),
            Self::Broker(error) => Some(error),
            Self::UnexpectedPayload(_) | Self::RequestIdExhausted => None,
        }
    }
}

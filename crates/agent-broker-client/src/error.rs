use std::error::Error;
use std::fmt;
use std::io;

use agent_broker_application::BrokerError;
use agent_broker_protocol::{Operation, ProtocolCodecError};

use crate::session_store::ClientSessionStoreError;

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

/// Failure from an explicitly opted-in durable protocol-v3 execution/recovery operation.
#[derive(Debug)]
pub enum DurableExecutionError {
    Client(ClientError),
    SessionStore(ClientSessionStoreError),
}

impl fmt::Display for DurableExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => error.fmt(formatter),
            Self::SessionStore(error) => error.fmt(formatter),
        }
    }
}

impl Error for DurableExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Client(error) => Some(error),
            Self::SessionStore(error) => Some(error),
        }
    }
}

impl From<ClientError> for DurableExecutionError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl From<ClientSessionStoreError> for DurableExecutionError {
    fn from(error: ClientSessionStoreError) -> Self {
        Self::SessionStore(error)
    }
}

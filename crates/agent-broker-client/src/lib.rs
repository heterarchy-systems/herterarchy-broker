#![forbid(unsafe_code)]
//! Typed synchronous client for the Agent Broker protocol-v1 TCP boundary.

mod client;
mod error;

pub use client::{BrokerClient, BrokerClientConfig, ClaimInput, CompleteInput, RenewInput};
pub use error::ClientError;

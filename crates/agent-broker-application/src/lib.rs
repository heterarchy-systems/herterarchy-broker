#![forbid(unsafe_code)]
//! Provider-neutral Agent Broker application and consensus boundary.

mod consensus;
mod error;
mod service;

pub use consensus::ConsensusAdapter;
pub use error::{BrokerError, BrokerErrorCode};
pub use service::{
    BrokerApplicationService, BrokerHealth, ClaimTaskInput, CompleteTaskInput, RenewTaskLeaseInput,
};

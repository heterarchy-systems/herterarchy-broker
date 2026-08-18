#![forbid(unsafe_code)]
//! Provider-neutral Agent Broker application and consensus boundary.

mod command_identity;
mod consensus;
mod error;
mod service;

pub use command_identity::{
    CommandIdentity, CommandIdentityError, CommandSequence, CommandSessionId, SessionOwnerEpoch,
    SessionOwnerInstanceId,
};
pub use consensus::ConsensusAdapter;
pub use error::{BrokerError, BrokerErrorCode, BrokerErrorDisposition};
pub use service::{
    BrokerApplicationService, BrokerHealth, ClaimTaskInput, CompleteTaskInput, RenewTaskLeaseInput,
};

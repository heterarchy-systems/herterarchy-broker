#![forbid(unsafe_code)]
//! Standalone process/runtime composition for the Rust Agent Broker.

mod clock;
mod error;
mod standalone_maintenance;
mod state_owner;
mod state_process_lock;
mod tcp_server;

pub use error::RuntimeError;
pub use standalone_maintenance::{
    MaintenanceRunError, StandaloneMaintenancePolicy, StandaloneMaintenanceResult,
    StandaloneMaintenanceRunner,
};
pub use state_owner::StateOwnerHandle;
pub use state_process_lock::BrokerStateProcessLock;
pub use tcp_server::{BrokerServerConfig, TcpBrokerServer};

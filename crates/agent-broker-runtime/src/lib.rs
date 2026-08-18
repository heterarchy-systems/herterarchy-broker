#![forbid(unsafe_code)]
//! Standalone process/runtime composition for the Rust Agent Broker.

mod clock;
mod error;
mod operations;
mod standalone_maintenance;
mod state_owner;
mod state_process_lock;
mod tcp_server;

pub use error::RuntimeError;
pub use operations::{
    ClusterOperationsObserver, ClusterOperationsReason, ClusterOperationsSnapshot,
    OperationsBindPolicy, OperationsServer, OperationsServerConfig, StandaloneOperationsObserver,
};
pub use standalone_maintenance::{
    LeaderMaintenanceResult, LeaderMaintenanceRunner, MaintenanceRunError,
    StandaloneMaintenancePolicy, StandaloneMaintenanceResult, StandaloneMaintenanceRunner,
};
pub use state_owner::{StateOwnerHandle, StateOwnerLoad};
pub use state_process_lock::BrokerStateProcessLock;
pub use tcp_server::{
    BrokerBindPolicy, BrokerServerConfig, BrokerServerLoad, BrokerServerObserver, TcpBrokerServer,
};

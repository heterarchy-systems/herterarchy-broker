#![forbid(unsafe_code)]
//! Typed synchronous and asynchronous clients for Agent Broker protocol-v1/v2/v3 TCP boundaries.

#[cfg(feature = "async")]
mod async_client;
#[cfg(feature = "async")]
mod async_router;
#[cfg(feature = "async")]
mod async_session_store;
mod client;
mod error;
mod router;
mod session_store;

#[cfg(feature = "async")]
pub use async_client::AsyncBrokerClient;
#[cfg(feature = "async")]
pub use async_router::AsyncStaticClusterRouter;
#[cfg(feature = "async")]
pub use async_session_store::AsyncDurableClientSessionStore;
pub use client::{
    BrokerClient, BrokerClientConfig, ClaimInput, CompleteInput, DurableRetryPolicy, RenewInput,
};
pub use error::{ClientError, DurableExecutionError};
pub use router::{
    StaticClusterNode, StaticClusterRouter, StaticClusterRouterConfig, StaticClusterRoutingError,
    StaticClusterRoutingRetryPolicy,
};
pub use session_store::{
    ClientSessionStoreError, DurableClientSessionStore, DurableSessionOwner,
    PendingOwnerAcquisition, ReservedCommand,
};

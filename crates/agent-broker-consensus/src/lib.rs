#![forbid(unsafe_code)]
//! Consensus/commit implementations shared by standalone and future replicated Agent Broker modes.

mod cluster_raft;
mod cluster_tls;
mod cluster_transport;
mod one_node_network;
mod one_node_raft;
mod raft_log_storage;
mod raft_observation;
mod raft_persistence;
mod raft_state_machine;
mod raft_type_config;
mod replicated_command;
mod replicated_proposal;
mod replicated_response;
mod standalone;

pub use cluster_raft::{
    ClusterRaftConfig, ClusterRaftConsensusAdapter, ClusterRaftObserver, ClusterRaftProgress,
    ClusterRaftReadiness, ClusterRaftReadinessStatus,
};
pub use cluster_tls::ClusterRaftTlsConfig;
pub use one_node_raft::{OneNodeRaftConfig, OneNodeRaftConsensusAdapter, OneNodeRaftProgress};
pub use raft_type_config::{AgentBrokerRaftNodeId, AgentBrokerRaftTypeConfig};
pub use replicated_command::{ReplicatedBrokerCommandV1, ReplicatedCommandError};
pub use replicated_proposal::{ReplicatedBrokerProposalV1, ReplicatedCommandIdentityV1};
pub use replicated_response::{
    ReplicatedBrokerMutationResultV1, ReplicatedBrokerResponseV1, ReplicatedResponseError,
};
pub use standalone::StandaloneConsensusAdapter;

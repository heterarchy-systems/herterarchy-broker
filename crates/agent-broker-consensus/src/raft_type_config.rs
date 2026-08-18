use std::io::Cursor;

use crate::{ReplicatedBrokerProposalV1, ReplicatedBrokerResponseV1};

openraft::declare_raft_types!(
    /// OpenRaft type boundary for the Agent Broker replicated consensus path.
    pub AgentBrokerRaftTypeConfig:
        D = ReplicatedBrokerProposalV1,
        R = ReplicatedBrokerResponseV1,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<AgentBrokerRaftTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        Responder = openraft::impls::OneshotResponder<AgentBrokerRaftTypeConfig>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub type AgentBrokerRaftNodeId = u64;

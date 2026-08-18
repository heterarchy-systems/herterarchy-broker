use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::error::{Fatal, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{BasicNode, Vote};

use crate::raft_type_config::{AgentBrokerRaftNodeId, AgentBrokerRaftTypeConfig};

/// One-node-only network factory.
///
/// A valid single-node `OpenRaft` group never needs to emit an RPC to a peer. If `OpenRaft` ever asks
/// this factory for a client, the returned client fails every operation with `Unreachable` and
/// increments an observation counter. Tests assert the count stays at zero so one-node parity cannot
/// accidentally depend on an undeclared in-process transport.
#[derive(Debug, Clone, Default)]
pub(crate) struct OneNodeRaftNetworkFactory {
    remote_attempts: Arc<AtomicU64>,
}

impl OneNodeRaftNetworkFactory {
    pub(crate) fn remote_attempt_count(&self) -> u64 {
        self.remote_attempts.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OneNodeRaftNetwork {
    target: AgentBrokerRaftNodeId,
    remote_attempts: Arc<AtomicU64>,
}

impl RaftNetworkFactory<AgentBrokerRaftTypeConfig> for OneNodeRaftNetworkFactory {
    type Network = OneNodeRaftNetwork;

    async fn new_client(
        &mut self,
        target: AgentBrokerRaftNodeId,
        _node: &BasicNode,
    ) -> Self::Network {
        OneNodeRaftNetwork {
            target,
            remote_attempts: self.remote_attempts.clone(),
        }
    }
}

impl RaftNetwork<AgentBrokerRaftTypeConfig> for OneNodeRaftNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<AgentBrokerRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<AgentBrokerRaftNodeId>,
        RPCError<AgentBrokerRaftNodeId, BasicNode, RaftError<AgentBrokerRaftNodeId>>,
    > {
        self.remote_attempts.fetch_add(1, Ordering::Relaxed);
        Err(unreachable(self.target).into())
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<AgentBrokerRaftNodeId>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<AgentBrokerRaftNodeId>,
        RPCError<AgentBrokerRaftNodeId, BasicNode, RaftError<AgentBrokerRaftNodeId>>,
    > {
        self.remote_attempts.fetch_add(1, Ordering::Relaxed);
        Err(unreachable(self.target).into())
    }

    async fn full_snapshot(
        &mut self,
        _vote: Vote<AgentBrokerRaftNodeId>,
        _snapshot: Snapshot<AgentBrokerRaftTypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<
        SnapshotResponse<AgentBrokerRaftNodeId>,
        StreamingError<AgentBrokerRaftTypeConfig, Fatal<AgentBrokerRaftNodeId>>,
    > {
        self.remote_attempts.fetch_add(1, Ordering::Relaxed);
        Err(unreachable(self.target).into())
    }
}

fn unreachable(target: AgentBrokerRaftNodeId) -> Unreachable {
    let error = io::Error::new(
        io::ErrorKind::NotConnected,
        format!("one-node Raft has no remote transport for target node {target}"),
    );
    Unreachable::new(&error)
}

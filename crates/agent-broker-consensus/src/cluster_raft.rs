use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use agent_broker_application::{
    BrokerError, BrokerErrorCode, CommandIdentity, CommandSessionId, ConsensusAdapter,
    SessionOwnerEpoch, SessionOwnerInstanceId,
};
use agent_broker_domain::commands::{AdvanceTermCommand, BrokerCommand};
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{BrokerState, ConsumerGroupDirectory, Revision, Term};
use openraft::error::{CheckIsLeaderError, RaftError};
use openraft::raft::Raft;
use openraft::{BasicNode, Config, SnapshotPolicy};

use crate::ReplicatedBrokerCommandV1;
use crate::cluster_tls::ClusterRaftTlsConfig;
use crate::cluster_transport::{RaftRpcServerHandle, TcpRaftNetworkFactory, start_raft_rpc_server};
use crate::raft_log_storage::AgentBrokerRaftLogStorage;
use crate::raft_observation::RaftBrokerObservation;
use crate::raft_persistence::RedbRaftPersistence;
use crate::raft_state_machine::AgentBrokerRaftStateMachine;
use crate::raft_type_config::{AgentBrokerRaftNodeId, AgentBrokerRaftTypeConfig};

const CONTROL_QUEUE_CAPACITY: usize = 64;
const INITIAL_LEADER_WAIT: Duration = Duration::from_millis(750);
const CLUSTER_BOOTSTRAP_WAIT: Duration = Duration::from_secs(15);
const LEADER_POLL_INTERVAL: Duration = Duration::from_millis(20);
const DEFAULT_SNAPSHOT_LOG_INTERVAL: u64 = 256;
const DEFAULT_REPLICATION_LAG_THRESHOLD: u64 = 5_000;
const DEFAULT_MAX_IN_SNAPSHOT_LOG_TO_KEEP: u64 = 1_000;
const DEFAULT_IDENTIFIED_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_IDENTIFIED_WRITE_TIMEOUT: Duration = Duration::from_mins(1);
const DEFAULT_RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_ELECTION_TIMEOUT_MIN_MS: u64 = 1_000;
const DEFAULT_ELECTION_TIMEOUT_MAX_MS: u64 = 2_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 100;
const DEFAULT_READINESS_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const THREE_NODE_CLUSTER_SIZE: usize = 3;

/// Durable configuration for one member of the initial three-node Agent Broker Raft cluster.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClusterRaftConfig {
    node_id: AgentBrokerRaftNodeId,
    state_path: PathBuf,
    raft_bind_addr: SocketAddr,
    nodes: BTreeMap<AgentBrokerRaftNodeId, BasicNode>,
    tls: ClusterRaftTlsConfig,
    bootstrap: bool,
    snapshot_log_interval: u64,
    replication_lag_threshold: u64,
    max_in_snapshot_log_to_keep: u64,
    identified_write_timeout: Duration,
    rpc_connect_timeout: Duration,
    election_timeout_min_ms: u64,
    election_timeout_max_ms: u64,
    heartbeat_interval_ms: u64,
}

impl ClusterRaftConfig {
    /// Construct one node configuration from the complete initial three-node address map.
    ///
    /// `nodes` maps stable Raft node IDs to pre-resolved IP socket addresses.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when node IDs/addresses are invalid, the local node is missing, or
    /// the initial topology is not exactly three nodes.
    pub fn new(
        node_id: AgentBrokerRaftNodeId,
        state_path: impl Into<PathBuf>,
        raft_bind_addr: SocketAddr,
        nodes: BTreeMap<AgentBrokerRaftNodeId, String>,
        tls: ClusterRaftTlsConfig,
        bootstrap: bool,
    ) -> Result<Self, BrokerError> {
        let nodes = nodes
            .into_iter()
            .map(|(id, address)| (id, BasicNode::new(address)))
            .collect::<BTreeMap<_, _>>();
        let config = Self {
            node_id,
            state_path: state_path.into(),
            raft_bind_addr,
            nodes,
            tls,
            bootstrap,
            snapshot_log_interval: DEFAULT_SNAPSHOT_LOG_INTERVAL,
            replication_lag_threshold: DEFAULT_REPLICATION_LAG_THRESHOLD,
            max_in_snapshot_log_to_keep: DEFAULT_MAX_IN_SNAPSHOT_LOG_TO_KEEP,
            identified_write_timeout: DEFAULT_IDENTIFIED_WRITE_TIMEOUT,
            rpc_connect_timeout: DEFAULT_RPC_CONNECT_TIMEOUT,
            election_timeout_min_ms: DEFAULT_ELECTION_TIMEOUT_MIN_MS,
            election_timeout_max_ms: DEFAULT_ELECTION_TIMEOUT_MAX_MS,
            heartbeat_interval_ms: DEFAULT_HEARTBEAT_INTERVAL_MS,
        };
        config.validate()?;
        Ok(config)
    }

    /// Override the committed-log interval used by the snapshot policy.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when `interval` is zero or is not below the configured replication
    /// lag threshold.
    pub fn with_snapshot_log_interval(mut self, interval: u64) -> Result<Self, BrokerError> {
        self.snapshot_log_interval = interval;
        self.validate_snapshot_catch_up_policy()?;
        Ok(self)
    }

    /// Override the snapshot/lag-retention relationship used for deterministic follower catch-up.
    ///
    /// `replication_lag_threshold` must be greater than `snapshot_log_interval`, matching
    /// `OpenRaft`'s requirement that a snapshot can cover a follower classified as lagging.
    /// `max_in_snapshot_log_to_keep` may be zero when callers intentionally want all logs already
    /// represented by a durable snapshot to become purge-eligible.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when the snapshot interval is zero or the lag threshold is not
    /// strictly greater than that interval.
    pub fn with_snapshot_catch_up_policy(
        mut self,
        snapshot_log_interval: u64,
        replication_lag_threshold: u64,
        max_in_snapshot_log_to_keep: u64,
    ) -> Result<Self, BrokerError> {
        self.snapshot_log_interval = snapshot_log_interval;
        self.replication_lag_threshold = replication_lag_threshold;
        self.max_in_snapshot_log_to_keep = max_in_snapshot_log_to_keep;
        self.validate_snapshot_catch_up_policy()?;
        Ok(self)
    }

    /// Override the post-submit response deadline for identified mutations.
    ///
    /// This deadline begins only after `OpenRaft` has accepted the proposal into its core request
    /// channel. Expiry therefore means the commit outcome is unknown, not that the mutation failed.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when the timeout is zero or exceeds one minute.
    pub fn with_identified_write_timeout(mut self, timeout: Duration) -> Result<Self, BrokerError> {
        self.identified_write_timeout = timeout;
        self.validate_identified_write_timeout()?;
        Ok(self)
    }

    /// Override the outbound Raft TCP connect deadline.
    ///
    /// # Errors
    /// Returns [`BrokerError`] when the timeout is zero or exceeds 30 seconds.
    pub fn with_rpc_connect_timeout(mut self, timeout: Duration) -> Result<Self, BrokerError> {
        self.rpc_connect_timeout = timeout;
        self.validate_rpc_connect_timeout()?;
        Ok(self)
    }

    /// Override the `OpenRaft` election/heartbeat timing policy.
    ///
    /// Defaults remain 1000–2000 ms election timeouts and a 100 ms heartbeat interval. This is
    /// primarily useful for deterministic fault tests that must separate a client response
    /// deadline from leader-election timing.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when a duration is below one millisecond, cannot fit in `u64`
    /// milliseconds, the election range is inverted, or the minimum election timeout is not
    /// greater than the heartbeat interval.
    pub fn with_raft_timing(
        mut self,
        election_timeout_min: Duration,
        election_timeout_max: Duration,
        heartbeat_interval: Duration,
    ) -> Result<Self, BrokerError> {
        self.election_timeout_min_ms =
            duration_millis_u64(election_timeout_min, "cluster election_timeout_min")?;
        self.election_timeout_max_ms =
            duration_millis_u64(election_timeout_max, "cluster election_timeout_max")?;
        self.heartbeat_interval_ms =
            duration_millis_u64(heartbeat_interval, "cluster heartbeat_interval")?;
        self.validate_raft_timing()?;
        Ok(self)
    }

    #[must_use]
    pub const fn node_id(&self) -> AgentBrokerRaftNodeId {
        self.node_id
    }

    #[must_use]
    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    #[must_use]
    pub const fn raft_bind_addr(&self) -> SocketAddr {
        self.raft_bind_addr
    }

    #[must_use]
    pub const fn bootstrap(&self) -> bool {
        self.bootstrap
    }

    fn validate(&self) -> Result<(), BrokerError> {
        if self.node_id == 0 {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "cluster Raft node_id must be greater than zero",
            ));
        }
        if self.nodes.len() != THREE_NODE_CLUSTER_SIZE {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "initial Agent Broker cluster topology must contain exactly three nodes",
            ));
        }
        if !self.nodes.contains_key(&self.node_id) {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "cluster Raft topology does not contain the local node_id",
            ));
        }
        if self
            .nodes
            .iter()
            .any(|(id, node)| *id == 0 || node.addr.parse::<SocketAddr>().is_err())
        {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "cluster Raft node IDs must be positive and advertised addresses must be pre-resolved IP socket addresses",
            ));
        }
        self.validate_snapshot_catch_up_policy()?;
        self.validate_identified_write_timeout()?;
        self.validate_rpc_connect_timeout()?;
        self.tls.validate()?;
        self.validate_raft_timing()?;
        Ok(())
    }

    fn validate_snapshot_catch_up_policy(&self) -> Result<(), BrokerError> {
        if self.snapshot_log_interval == 0 {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "cluster Raft snapshot log interval must be greater than zero",
            ));
        }
        if self.replication_lag_threshold <= self.snapshot_log_interval {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "cluster Raft replication lag threshold must exceed the snapshot log interval",
            ));
        }
        Ok(())
    }

    fn validate_identified_write_timeout(&self) -> Result<(), BrokerError> {
        if self.identified_write_timeout.is_zero()
            || self.identified_write_timeout > MAX_IDENTIFIED_WRITE_TIMEOUT
        {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "cluster identified write timeout must be between 1ns and 60s",
            ));
        }
        Ok(())
    }

    fn validate_rpc_connect_timeout(&self) -> Result<(), BrokerError> {
        if self.rpc_connect_timeout.is_zero() || self.rpc_connect_timeout > MAX_RPC_CONNECT_TIMEOUT
        {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "cluster Raft RPC connect timeout must be between 1ns and 30s",
            ));
        }
        Ok(())
    }

    fn validate_raft_timing(&self) -> Result<(), BrokerError> {
        if self.election_timeout_min_ms >= self.election_timeout_max_ms {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "cluster election_timeout_min must be below election_timeout_max",
            ));
        }
        if self.election_timeout_min_ms <= self.heartbeat_interval_ms {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "cluster election_timeout_min must be greater than heartbeat_interval",
            ));
        }
        Ok(())
    }
}

fn duration_millis_u64(duration: Duration, label: &'static str) -> Result<u64, BrokerError> {
    let millis = u64::try_from(duration.as_millis()).map_err(|_| {
        BrokerError::new(
            BrokerErrorCode::InvalidRequest,
            format!("{label} exceeds u64 milliseconds"),
        )
    })?;
    if millis == 0 {
        return Err(BrokerError::new(
            BrokerErrorCode::InvalidRequest,
            format!("{label} must be at least one millisecond"),
        ));
    }
    Ok(millis)
}

/// Observable replicated progress for one Agent Broker cluster member.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClusterRaftProgress {
    pub node_id: AgentBrokerRaftNodeId,
    pub raft_rpc_addr: SocketAddr,
    pub raft_rpc_queued_connections: usize,
    pub raft_rpc_active_connections: usize,
    pub raft_term: u64,
    pub last_log_index: Option<u64>,
    pub committed_index: Option<u64>,
    pub applied_index: Option<u64>,
    pub snapshot_index: Option<u64>,
    pub purged_index: Option<u64>,
    pub broker_term: Term,
    pub broker_revision: Revision,
    pub current_leader: Option<AgentBrokerRaftNodeId>,
    pub voters: BTreeSet<AgentBrokerRaftNodeId>,
    pub learners: BTreeSet<AgentBrokerRaftNodeId>,
}

/// Machine-readable write-readiness state for one cluster member.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ClusterRaftReadinessStatus {
    /// The local node is current leader and `OpenRaft` confirmed current-term quorum authority.
    Ready,
    /// The local node observes another node as leader.
    Follower,
    /// The local node does not currently observe a leader.
    NoLeader,
    /// The local node still appeared to be leader locally, but quorum confirmation failed.
    QuorumUnavailable,
    /// The local consensus adapter entered its fail-stop state.
    ConsensusFailStopped,
    /// The consensus controller is no longer reachable.
    ConsensusUnavailable,
    /// The bounded consensus control queue could not accept an observation request.
    ObservationSaturated,
    /// The bounded readiness deadline elapsed before a definitive observation completed.
    ProbeTimedOut,
}

impl ClusterRaftReadinessStatus {
    /// Stable machine-readable reason string for operations surfaces.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Follower => "follower",
            Self::NoLeader => "no_leader",
            Self::QuorumUnavailable => "quorum_unavailable",
            Self::ConsensusFailStopped => "consensus_fail_stopped",
            Self::ConsensusUnavailable => "consensus_unavailable",
            Self::ObservationSaturated => "observation_saturated",
            Self::ProbeTimedOut => "probe_timed_out",
        }
    }
}

/// Bounded read-only consensus readiness observation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClusterRaftReadiness {
    pub status: ClusterRaftReadinessStatus,
    pub progress: Option<ClusterRaftProgress>,
}

impl ClusterRaftReadiness {
    fn new(status: ClusterRaftReadinessStatus, progress: Option<ClusterRaftProgress>) -> Self {
        Self { status, progress }
    }

    /// Whether this observation proves current leader write-readiness.
    #[must_use]
    pub const fn is_write_ready(&self) -> bool {
        matches!(self.status, ClusterRaftReadinessStatus::Ready)
    }
}

/// Cloneable read-only observation handle for one cluster consensus controller.
#[derive(Clone)]
pub struct ClusterRaftObserver {
    requests: SyncSender<ControllerRequest>,
    fail_stopped: Arc<AtomicBool>,
}

impl std::fmt::Debug for ClusterRaftObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterRaftObserver")
            .field("fail_stopped", &self.fail_stopped.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl ClusterRaftObserver {
    /// Perform one fail-closed bounded write-readiness observation.
    ///
    /// This never proposes Broker business state. Leaders are considered ready only after
    /// `OpenRaft::ensure_linearizable` confirms current-term quorum authority.
    #[must_use]
    pub fn readiness(&self) -> ClusterRaftReadiness {
        if self.fail_stopped.load(Ordering::Acquire) {
            return ClusterRaftReadiness::new(
                ClusterRaftReadinessStatus::ConsensusFailStopped,
                None,
            );
        }
        let (reply, receiver) = mpsc::sync_channel(1);
        let deadline = Instant::now() + DEFAULT_READINESS_PROBE_TIMEOUT;
        match self
            .requests
            .try_send(ControllerRequest::Readiness { deadline, reply })
        {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return ClusterRaftReadiness::new(
                    ClusterRaftReadinessStatus::ObservationSaturated,
                    None,
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                return ClusterRaftReadiness::new(
                    ClusterRaftReadinessStatus::ConsensusUnavailable,
                    None,
                );
            }
        }
        match receiver.recv_timeout(DEFAULT_READINESS_PROBE_TIMEOUT) {
            Ok(readiness) => {
                if readiness.status == ClusterRaftReadinessStatus::ConsensusFailStopped {
                    self.fail_stopped.store(true, Ordering::Release);
                }
                readiness
            }
            Err(RecvTimeoutError::Timeout) => {
                ClusterRaftReadiness::new(ClusterRaftReadinessStatus::ProbeTimedOut, None)
            }
            Err(RecvTimeoutError::Disconnected) => {
                ClusterRaftReadiness::new(ClusterRaftReadinessStatus::ConsensusUnavailable, None)
            }
        }
    }
}

/// Synchronous application-facing adapter backed by one member of a real TCP `OpenRaft` cluster.
pub struct ClusterRaftConsensusAdapter {
    node_id: AgentBrokerRaftNodeId,
    requests: SyncSender<ControllerRequest>,
    controller: Option<JoinHandle<()>>,
    observation: Arc<RaftBrokerObservation>,
    fail_stopped: Arc<AtomicBool>,
}

impl std::fmt::Debug for ClusterRaftConsensusAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterRaftConsensusAdapter")
            .field("node_id", &self.node_id)
            .field("term", &self.term())
            .field("revision", &self.revision())
            .field("fail_stopped", &self.fail_stopped.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl ClusterRaftConsensusAdapter {
    /// Open one cluster member, start its Raft RPC listener, and optionally converge bootstrap
    /// membership when this node is the designated bootstrap node.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] for invalid durable state, Raft startup, RPC listener, bootstrap, or
    /// controller-thread failures.
    pub fn open(config: ClusterRaftConfig) -> Result<Self, BrokerError> {
        ensure_parent_directory(config.state_path())?;
        let node_id = config.node_id();
        let observation = Arc::new(RaftBrokerObservation::new(&BrokerState::default(), None));
        let controller_observation = Arc::clone(&observation);
        let fail_stopped = Arc::new(AtomicBool::new(false));
        let (requests, receiver) = mpsc::sync_channel(CONTROL_QUEUE_CAPACITY);
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let controller = thread::Builder::new()
            .name(format!("agent-broker-cluster-raft-{node_id}"))
            .spawn(move || {
                run_controller(config, &receiver, &startup_sender, controller_observation);
            })
            .map_err(|error| {
                BrokerError::new(
                    BrokerErrorCode::InternalError,
                    format!("failed to start cluster Raft controller thread: {error}"),
                )
            })?;
        match startup_receiver.recv() {
            Ok(Ok(_progress)) => Ok(Self {
                node_id,
                requests,
                controller: Some(controller),
                observation,
                fail_stopped,
            }),
            Ok(Err(error)) => {
                let _join_result = controller.join();
                Err(error)
            }
            Err(error) => {
                let _join_result = controller.join();
                Err(BrokerError::new(
                    BrokerErrorCode::InternalError,
                    format!("cluster Raft controller exited during startup: {error}"),
                ))
            }
        }
    }

    /// Create a cloneable read-only operations observer before moving this adapter into the
    /// application state owner.
    #[must_use]
    pub fn observer(&self) -> ClusterRaftObserver {
        ClusterRaftObserver {
            requests: self.requests.clone(),
            fail_stopped: Arc::clone(&self.fail_stopped),
        }
    }

    /// Read Raft, Broker, and membership progress from the local cluster member.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when the adapter is fail-stopped or the controller cannot provide
    /// current Raft state.
    pub fn progress(&mut self) -> Result<ClusterRaftProgress, BrokerError> {
        self.ensure_available()?;
        self.request(|reply| ControllerRequest::Progress { reply })?
    }

    /// Shut down the Raft RPC listener, Raft engine, and controller thread.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when shutdown or controller joining fails.
    pub fn shutdown(mut self) -> Result<(), BrokerError> {
        let shutdown_result = if self.fail_stopped.load(Ordering::Acquire) {
            Ok(())
        } else {
            self.request(|reply| ControllerRequest::Shutdown { reply })?
        };
        let join_result = self.join_controller();
        shutdown_result.and(join_result)
    }

    fn ensure_available(&self) -> Result<(), BrokerError> {
        if self.fail_stopped.load(Ordering::Acquire) {
            return Err(BrokerError::new(
                BrokerErrorCode::PersistenceError,
                "cluster Raft consensus adapter is fail-stopped after an engine failure",
            ));
        }
        Ok(())
    }

    fn request<T>(
        &self,
        build: impl FnOnce(SyncSender<T>) -> ControllerRequest,
    ) -> Result<T, BrokerError> {
        let (reply, receiver) = mpsc::sync_channel(1);
        self.requests.send(build(reply)).map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::PersistenceError,
                format!("cluster Raft controller is unavailable: {error}"),
            )
        })?;
        receiver.recv().map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::PersistenceError,
                format!("cluster Raft controller dropped the response: {error}"),
            )
        })
    }

    fn join_controller(&mut self) -> Result<(), BrokerError> {
        let Some(controller) = self.controller.take() else {
            return Ok(());
        };
        controller.join().map_err(|_| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                "cluster Raft controller thread panicked",
            )
        })
    }
}

impl ConsensusAdapter for ClusterRaftConsensusAdapter {
    fn term(&self) -> Term {
        self.observation.validated_term().unwrap_or(Term::INITIAL)
    }

    fn revision(&self) -> Revision {
        self.observation.revision()
    }

    fn group_directory(&mut self) -> Result<ConsumerGroupDirectory, BrokerError> {
        self.ensure_available()?;
        let deadline = Instant::now() + DEFAULT_READINESS_PROBE_TIMEOUT;
        self.request(|reply| ControllerRequest::GroupDirectory { deadline, reply })?
    }

    fn maintenance_authority(&mut self) -> Result<bool, BrokerError> {
        let progress = self.progress()?;
        Ok(progress.current_leader == Some(self.node_id))
    }

    fn propose(&mut self, command: BrokerCommand) -> Result<BrokerMutationResult, BrokerError> {
        self.ensure_available()?;
        if matches!(command, BrokerCommand::AdvanceTerm(_)) {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "Broker term is owned by Raft in cluster consensus mode",
            ));
        }
        let reply = match self.request(|reply| ControllerRequest::Propose { command, reply }) {
            Ok(reply) => reply,
            Err(error) => {
                self.fail_stopped.store(true, Ordering::Release);
                return Err(error);
            }
        };
        if let Err(error) = &reply
            && should_poison(error)
        {
            self.fail_stopped.store(true, Ordering::Release);
        }
        reply
    }

    fn propose_identified(
        &mut self,
        identity: CommandIdentity,
        command: BrokerCommand,
    ) -> Result<BrokerMutationResult, BrokerError> {
        self.ensure_available()?;
        if matches!(command, BrokerCommand::AdvanceTerm(_)) {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "Broker term is owned by Raft in cluster consensus mode",
            ));
        }
        let reply = match self.request(|reply| ControllerRequest::ProposeIdentified {
            identity,
            command,
            reply,
        }) {
            Ok(reply) => reply,
            Err(error) => {
                self.fail_stopped.store(true, Ordering::Release);
                return Err(error);
            }
        };
        if let Err(error) = &reply
            && should_poison(error)
        {
            self.fail_stopped.store(true, Ordering::Release);
        }
        reply
    }

    fn acquire_command_session_owner(
        &mut self,
        session_id: CommandSessionId,
        expected_owner_epoch: SessionOwnerEpoch,
        owner_instance_id: SessionOwnerInstanceId,
    ) -> Result<SessionOwnerEpoch, BrokerError> {
        self.ensure_available()?;
        let reply = match self.request(|reply| ControllerRequest::AcquireCommandSessionOwner {
            session_id,
            expected_owner_epoch,
            owner_instance_id,
            reply,
        }) {
            Ok(reply) => reply,
            Err(error) => {
                self.fail_stopped.store(true, Ordering::Release);
                return Err(error);
            }
        };
        if let Err(error) = &reply
            && should_poison(error)
        {
            self.fail_stopped.store(true, Ordering::Release);
        }
        reply
    }
}

impl Drop for ClusterRaftConsensusAdapter {
    fn drop(&mut self) {}
}

fn should_poison(error: &BrokerError) -> bool {
    matches!(
        error.code(),
        BrokerErrorCode::PersistenceError | BrokerErrorCode::InternalError
    )
}

enum ControllerRequest {
    Propose {
        command: BrokerCommand,
        reply: SyncSender<Result<BrokerMutationResult, BrokerError>>,
    },
    ProposeIdentified {
        identity: CommandIdentity,
        command: BrokerCommand,
        reply: SyncSender<Result<BrokerMutationResult, BrokerError>>,
    },
    AcquireCommandSessionOwner {
        session_id: CommandSessionId,
        expected_owner_epoch: SessionOwnerEpoch,
        owner_instance_id: SessionOwnerInstanceId,
        reply: SyncSender<Result<SessionOwnerEpoch, BrokerError>>,
    },
    Progress {
        reply: SyncSender<Result<ClusterRaftProgress, BrokerError>>,
    },
    Readiness {
        deadline: Instant,
        reply: SyncSender<ClusterRaftReadiness>,
    },
    GroupDirectory {
        deadline: Instant,
        reply: SyncSender<Result<ConsumerGroupDirectory, BrokerError>>,
    },
    Shutdown {
        reply: SyncSender<Result<(), BrokerError>>,
    },
}

struct RunningClusterRaft {
    node_id: AgentBrokerRaftNodeId,
    raft: Raft<AgentBrokerRaftTypeConfig>,
    observation: Arc<RaftBrokerObservation>,
    rpc_server: Option<RaftRpcServerHandle>,
    identified_write_timeout: Duration,
}

fn run_controller(
    config: ClusterRaftConfig,
    receiver: &Receiver<ControllerRequest>,
    startup_sender: &SyncSender<Result<ClusterRaftProgress, BrokerError>>,
    observation: Arc<RaftBrokerObservation>,
) {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .thread_name(format!("agent-broker-cluster-runtime-{}", config.node_id()))
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _send_result = startup_sender.send(Err(BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("failed to build cluster Raft Tokio runtime: {error}"),
            )));
            return;
        }
    };
    let mut running = match runtime.block_on(RunningClusterRaft::open(config, observation)) {
        Ok(running) => running,
        Err(error) => {
            let _send_result = startup_sender.send(Err(error));
            return;
        }
    };
    let startup_progress = runtime.block_on(running.progress());
    if startup_sender.send(startup_progress).is_err() {
        let _shutdown_result = running.stop(&runtime);
        return;
    }
    while let Ok(request) = receiver.recv() {
        match request {
            ControllerRequest::Propose { command, reply } => {
                let result = runtime.block_on(running.propose(command));
                let _send_result = reply.send(result);
            }
            ControllerRequest::ProposeIdentified {
                identity,
                command,
                reply,
            } => {
                let result = runtime.block_on(running.propose_identified(identity, command));
                let _send_result = reply.send(result);
            }
            ControllerRequest::AcquireCommandSessionOwner {
                session_id,
                expected_owner_epoch,
                owner_instance_id,
                reply,
            } => {
                let result = runtime.block_on(running.acquire_command_session_owner(
                    session_id,
                    expected_owner_epoch,
                    owner_instance_id,
                ));
                let _send_result = reply.send(result);
            }
            ControllerRequest::Progress { reply } => {
                let result = runtime.block_on(running.progress());
                let _send_result = reply.send(result);
            }
            ControllerRequest::Readiness { deadline, reply } => {
                if Instant::now() >= deadline {
                    let _send_result = reply.send(ClusterRaftReadiness::new(
                        ClusterRaftReadinessStatus::ProbeTimedOut,
                        None,
                    ));
                    continue;
                }
                let result = runtime.block_on(running.readiness(deadline));
                let _send_result = reply.send(result);
            }
            ControllerRequest::GroupDirectory { deadline, reply } => {
                let result = runtime.block_on(running.group_directory(deadline));
                let _send_result = reply.send(result);
            }
            ControllerRequest::Shutdown { reply } => {
                let result = running.stop(&runtime);
                let _send_result = reply.send(result);
                return;
            }
        }
    }
    let _shutdown_result = running.stop(&runtime);
}

impl RunningClusterRaft {
    async fn open(
        config: ClusterRaftConfig,
        observation: Arc<RaftBrokerObservation>,
    ) -> Result<Self, BrokerError> {
        let persistence = RedbRaftPersistence::open(config.state_path()).map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::PersistenceError,
                format!("failed to open cluster Raft redb store: {error}"),
            )
        })?;
        let log_store = AgentBrokerRaftLogStorage::new(persistence.clone());
        let state_machine =
            AgentBrokerRaftStateMachine::load(persistence, Arc::clone(&observation))
                .await
                .map_err(raft_storage_error)?;
        let raft_config = Config {
            cluster_name: "agent-broker-cluster".to_owned(),
            election_timeout_min: config.election_timeout_min_ms,
            election_timeout_max: config.election_timeout_max_ms,
            heartbeat_interval: config.heartbeat_interval_ms,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_log_interval),
            replication_lag_threshold: config.replication_lag_threshold,
            max_in_snapshot_log_to_keep: config.max_in_snapshot_log_to_keep,
            ..Config::default()
        }
        .validate()
        .map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("invalid cluster Raft configuration: {error}"),
            )
        })?;
        let tls = Arc::new(
            config
                .tls
                .load(config.node_id, config.nodes.keys().copied())?,
        );
        let raft = Raft::new(
            config.node_id,
            Arc::new(raft_config),
            TcpRaftNetworkFactory::new(
                config.node_id,
                config.rpc_connect_timeout,
                Arc::clone(&tls),
            ),
            log_store,
            state_machine,
        )
        .await
        .map_err(raft_fatal_error)?;
        let rpc_server =
            start_raft_rpc_server(&raft, config.raft_bind_addr, &tls).map_err(|error| {
                BrokerError::new(
                    BrokerErrorCode::TransportError,
                    format!("failed to bind cluster Raft RPC listener: {error}"),
                )
            })?;
        let mut running = Self {
            node_id: config.node_id,
            raft,
            observation,
            rpc_server: Some(rpc_server),
            identified_write_timeout: config.identified_write_timeout,
        };
        if config.bootstrap {
            running.bootstrap(&config.nodes).await?;
        }
        running.wait_until_committed_is_applied().await?;
        if running.raft.current_leader().await == Some(running.node_id) {
            running.sync_broker_term_to_raft().await?;
        }
        Ok(running)
    }

    async fn bootstrap(
        &mut self,
        nodes: &BTreeMap<AgentBrokerRaftNodeId, BasicNode>,
    ) -> Result<(), BrokerError> {
        let desired_voters = nodes.keys().copied().collect::<BTreeSet<_>>();
        let initialized = self.raft.is_initialized().await.map_err(raft_fatal_error)?;
        if initialized && current_voters(&self.raft) == desired_voters {
            return Ok(());
        }
        if !initialized {
            let local_node = nodes.get(&self.node_id).cloned().ok_or_else(|| {
                BrokerError::new(
                    BrokerErrorCode::InvalidRequest,
                    "bootstrap topology is missing the local node",
                )
            })?;
            self.raft
                .initialize(BTreeMap::from([(self.node_id, local_node)]))
                .await
                .map_err(|error| raft_engine_error("initialize bootstrap node", error))?;
        }
        ensure_local_leader(&self.raft, self.node_id).await?;
        if current_voters(&self.raft) == desired_voters {
            return Ok(());
        }
        for (&node_id, node) in nodes {
            if node_id == self.node_id || current_members(&self.raft).contains(&node_id) {
                continue;
            }
            self.raft
                .add_learner(node_id, node.clone(), false)
                .await
                .map_err(|error| raft_engine_error("add cluster learner", error))?;
        }
        converge_voters(&self.raft, desired_voters).await
    }

    async fn propose(&self, command: BrokerCommand) -> Result<BrokerMutationResult, BrokerError> {
        let leader = self.raft.current_leader().await;
        if leader != Some(self.node_id) {
            return Err(not_leader_error(self.node_id, leader));
        }
        self.sync_broker_term_to_raft().await?;
        let data = ReplicatedBrokerCommandV1::try_from(command).map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("failed to encode Broker command for cluster Raft: {error}"),
            )
        })?;
        let proposal = crate::ReplicatedBrokerProposalV1::legacy(data);
        let response = self
            .raft
            .client_write::<tokio::sync::oneshot::error::RecvError>(proposal)
            .await
            .map_err(|error| raft_engine_error("client_write", error))?;
        response.data.into_application_result().map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("failed to decode committed cluster Raft response: {error}"),
            )
        })?
    }

    async fn propose_identified(
        &self,
        identity: CommandIdentity,
        command: BrokerCommand,
    ) -> Result<BrokerMutationResult, BrokerError> {
        let leader = self.raft.current_leader().await;
        if leader != Some(self.node_id) {
            return Err(not_leader_error(self.node_id, leader));
        }
        self.sync_broker_term_to_raft().await?;
        let data = ReplicatedBrokerCommandV1::try_from(command).map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("failed to encode identified Broker command for cluster Raft: {error}"),
            )
        })?;
        let proposal = crate::ReplicatedBrokerProposalV1::identified(&identity, data);
        let receiver = self
            .raft
            .client_write_ff(proposal)
            .await
            .map_err(raft_fatal_error)?;
        let response = match tokio::time::timeout(self.identified_write_timeout, receiver).await {
            Err(_) => {
                return Err(BrokerError::new(
                    BrokerErrorCode::CommitOutcomeUnknown,
                    format!(
                        "identified mutation response deadline elapsed after submission: session={} sequence={}",
                        identity.session_id(),
                        identity.sequence().get()
                    ),
                ));
            }
            Ok(Err(error)) => {
                return Err(raft_engine_error("identified client_write response", error));
            }
            Ok(Ok(result)) => {
                result.map_err(|error| raft_engine_error("identified client_write", error))?
            }
        };
        response.data.into_application_result().map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("failed to decode committed identified Raft response: {error}"),
            )
        })?
    }

    async fn acquire_command_session_owner(
        &self,
        session_id: CommandSessionId,
        expected_owner_epoch: SessionOwnerEpoch,
        owner_instance_id: SessionOwnerInstanceId,
    ) -> Result<SessionOwnerEpoch, BrokerError> {
        let leader = self.raft.current_leader().await;
        if leader != Some(self.node_id) {
            return Err(not_leader_error(self.node_id, leader));
        }
        self.sync_broker_term_to_raft().await?;
        let proposal = crate::ReplicatedBrokerProposalV1::legacy(
            ReplicatedBrokerCommandV1::AcquireCommandSessionOwner {
                session_id: session_id.as_str().to_owned(),
                expected_owner_epoch: expected_owner_epoch.get(),
                owner_instance_id: owner_instance_id.as_str().to_owned(),
            },
        );
        let response = self
            .raft
            .client_write::<tokio::sync::oneshot::error::RecvError>(proposal)
            .await
            .map_err(|error| raft_engine_error("command-session owner acquisition", error))?;
        match response.data {
            crate::ReplicatedBrokerResponseV1::SessionOwnerAcquired { owner_epoch } => {
                SessionOwnerEpoch::new(owner_epoch).map_err(|error| {
                    BrokerError::new(
                        BrokerErrorCode::InternalError,
                        format!("committed command-session owner epoch was invalid: {error}"),
                    )
                })
            }
            crate::ReplicatedBrokerResponseV1::ApplicationError(error) => Err(error.into()),
            other => Err(BrokerError::new(
                BrokerErrorCode::InternalError,
                format!(
                    "command-session owner acquisition returned unexpected response: {other:?}"
                ),
            )),
        }
    }

    async fn sync_broker_term_to_raft(&self) -> Result<(), BrokerError> {
        let raft_term = self.current_raft_term().await?;
        let broker_term = self
            .observation
            .validated_term()
            .map_err(|error| BrokerError::new(BrokerErrorCode::InternalError, error.to_string()))?;
        if raft_term < broker_term.get() {
            return Err(BrokerError::new(
                BrokerErrorCode::PersistenceError,
                format!(
                    "cluster Raft term {raft_term} is behind committed Broker term {}",
                    broker_term.get()
                ),
            ));
        }
        if raft_term == broker_term.get() {
            return Ok(());
        }
        let new_term = Term::new(raft_term).map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("cluster Raft produced invalid Broker fencing term: {error}"),
            )
        })?;
        let data =
            ReplicatedBrokerCommandV1::try_from(BrokerCommand::AdvanceTerm(AdvanceTermCommand {
                expected_term: broker_term,
                new_term,
            }))
            .map_err(|error| {
                BrokerError::new(
                    BrokerErrorCode::InternalError,
                    format!("failed to encode cluster term-advance command: {error}"),
                )
            })?;
        let proposal = crate::ReplicatedBrokerProposalV1::legacy(data);
        let response = self
            .raft
            .client_write::<tokio::sync::oneshot::error::RecvError>(proposal)
            .await
            .map_err(|error| raft_engine_error("term synchronization", error))?;
        match response.data.into_application_result().map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("failed to decode cluster term response: {error}"),
            )
        })? {
            Ok(BrokerMutationResult::TermAdvanced(_)) => Ok(()),
            Ok(other) => Err(BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("cluster term synchronization returned unexpected result: {other:?}"),
            )),
            Err(error) => Err(BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("cluster term synchronization was rejected: {error}"),
            )),
        }
    }

    async fn readiness(&self, deadline: Instant) -> ClusterRaftReadiness {
        let initial_progress = match self.progress().await {
            Ok(progress) => progress,
            Err(_error) => {
                return ClusterRaftReadiness::new(
                    ClusterRaftReadinessStatus::ConsensusFailStopped,
                    None,
                );
            }
        };
        if initial_progress.current_leader != Some(self.node_id) {
            let status = if initial_progress.current_leader.is_some() {
                ClusterRaftReadinessStatus::Follower
            } else {
                ClusterRaftReadinessStatus::NoLeader
            };
            return ClusterRaftReadiness::new(status, Some(initial_progress));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return ClusterRaftReadiness::new(
                ClusterRaftReadinessStatus::ProbeTimedOut,
                Some(initial_progress),
            );
        }
        let confirmation = tokio::time::timeout(remaining, self.raft.ensure_linearizable()).await;
        match confirmation {
            Err(_) => ClusterRaftReadiness::new(
                ClusterRaftReadinessStatus::ProbeTimedOut,
                Some(initial_progress),
            ),
            Ok(Ok(_read_log_id)) => match self.progress().await {
                Ok(progress) if progress.current_leader == Some(self.node_id) => {
                    ClusterRaftReadiness::new(ClusterRaftReadinessStatus::Ready, Some(progress))
                }
                Ok(progress) => {
                    let status = if progress.current_leader.is_some() {
                        ClusterRaftReadinessStatus::Follower
                    } else {
                        ClusterRaftReadinessStatus::NoLeader
                    };
                    ClusterRaftReadiness::new(status, Some(progress))
                }
                Err(_error) => ClusterRaftReadiness::new(
                    ClusterRaftReadinessStatus::ConsensusFailStopped,
                    None,
                ),
            },
            Ok(Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_)))) => {
                ClusterRaftReadiness::new(
                    ClusterRaftReadinessStatus::QuorumUnavailable,
                    Some(initial_progress),
                )
            }
            Ok(Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(_)))) => {
                match self.progress().await {
                    Ok(progress) => {
                        let status = if progress.current_leader.is_some() {
                            ClusterRaftReadinessStatus::Follower
                        } else {
                            ClusterRaftReadinessStatus::NoLeader
                        };
                        ClusterRaftReadiness::new(status, Some(progress))
                    }
                    Err(_error) => ClusterRaftReadiness::new(
                        ClusterRaftReadinessStatus::ConsensusFailStopped,
                        None,
                    ),
                }
            }
            Ok(Err(RaftError::Fatal(_fatal))) => ClusterRaftReadiness::new(
                ClusterRaftReadinessStatus::ConsensusFailStopped,
                Some(initial_progress),
            ),
        }
    }

    async fn group_directory(
        &self,
        deadline: Instant,
    ) -> Result<ConsumerGroupDirectory, BrokerError> {
        let readiness = self.readiness(deadline).await;
        if readiness.status == ClusterRaftReadinessStatus::Ready {
            return Ok(self.observation.group_directory());
        }
        Err(BrokerError::new(
            BrokerErrorCode::TransportError,
            format!(
                "Consumer Group directory read is unavailable: {}",
                readiness.status.as_str()
            ),
        ))
    }

    async fn progress(&self) -> Result<ClusterRaftProgress, BrokerError> {
        let metrics = self.raft.metrics().borrow().clone();
        let committed_index = self
            .raft
            .with_raft_state(|state| state.committed.map(|log_id| log_id.index))
            .await
            .map_err(raft_fatal_error)?;
        let membership = metrics.membership_config.membership();
        let broker_term = self
            .observation
            .validated_term()
            .map_err(|error| BrokerError::new(BrokerErrorCode::InternalError, error.to_string()))?;
        let raft_rpc_addr = self.rpc_server.as_ref().map_or_else(
            || {
                Err(BrokerError::new(
                    BrokerErrorCode::InternalError,
                    "cluster Raft RPC server is unavailable",
                ))
            },
            |server| Ok(server.local_addr()),
        )?;
        let (raft_rpc_queued_connections, raft_rpc_active_connections) =
            match self.rpc_server.as_ref() {
                Some(server) => (server.queued_connections(), server.active_connections()),
                None => (0, 0),
            };
        Ok(ClusterRaftProgress {
            node_id: self.node_id,
            raft_rpc_addr,
            raft_rpc_queued_connections,
            raft_rpc_active_connections,
            raft_term: metrics.current_term,
            last_log_index: metrics.last_log_index,
            committed_index,
            applied_index: self.observation.applied_index(),
            snapshot_index: metrics.snapshot.map(|log_id| log_id.index),
            purged_index: metrics.purged.map(|log_id| log_id.index),
            broker_term,
            broker_revision: self.observation.revision(),
            current_leader: metrics.current_leader,
            voters: membership.voter_ids().collect(),
            learners: membership.learner_ids().collect(),
        })
    }

    async fn current_raft_term(&self) -> Result<u64, BrokerError> {
        self.raft
            .with_raft_state(|state| state.vote_ref().leader_id.get_term())
            .await
            .map_err(raft_fatal_error)
    }

    async fn wait_until_committed_is_applied(&self) -> Result<(), BrokerError> {
        let committed_index = self
            .raft
            .with_raft_state(|state| state.committed.map(|log_id| log_id.index))
            .await
            .map_err(raft_fatal_error)?;
        let Some(committed_index) = committed_index else {
            return Ok(());
        };
        let deadline = tokio::time::Instant::now() + CLUSTER_BOOTSTRAP_WAIT;
        loop {
            if self
                .observation
                .applied_index()
                .is_some_and(|applied| applied >= committed_index)
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(BrokerError::new(
                    BrokerErrorCode::PersistenceError,
                    "cluster Raft committed log was not applied before the startup deadline",
                ));
            }
            tokio::time::sleep(LEADER_POLL_INTERVAL).await;
        }
    }

    fn stop(&mut self, runtime: &tokio::runtime::Runtime) -> Result<(), BrokerError> {
        if let Some(server) = self.rpc_server.take() {
            server.stop().map_err(|error| {
                BrokerError::new(
                    BrokerErrorCode::TransportError,
                    format!("cluster Raft RPC server shutdown failed: {error}"),
                )
            })?;
        }
        runtime.block_on(self.raft.shutdown()).map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("cluster Raft shutdown failed: {error}"),
            )
        })
    }
}

async fn ensure_local_leader(
    raft: &Raft<AgentBrokerRaftTypeConfig>,
    node_id: AgentBrokerRaftNodeId,
) -> Result<(), BrokerError> {
    if wait_for_leader(raft, node_id, INITIAL_LEADER_WAIT).await {
        return Ok(());
    }
    raft.trigger().elect().await.map_err(raft_fatal_error)?;
    if wait_for_leader(raft, node_id, CLUSTER_BOOTSTRAP_WAIT).await {
        return Ok(());
    }
    Err(BrokerError::new(
        BrokerErrorCode::TransportError,
        format!("cluster bootstrap node {node_id} did not become leader before the deadline"),
    ))
}

async fn wait_for_leader(
    raft: &Raft<AgentBrokerRaftTypeConfig>,
    node_id: AgentBrokerRaftNodeId,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if raft.current_leader().await == Some(node_id) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(LEADER_POLL_INTERVAL).await;
    }
}

async fn converge_voters(
    raft: &Raft<AgentBrokerRaftTypeConfig>,
    desired_voters: BTreeSet<AgentBrokerRaftNodeId>,
) -> Result<(), BrokerError> {
    let deadline = tokio::time::Instant::now() + CLUSTER_BOOTSTRAP_WAIT;
    loop {
        if current_voters(raft) == desired_voters {
            return Ok(());
        }
        match raft.change_membership(desired_voters.clone(), false).await {
            Ok(_) => {}
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(LEADER_POLL_INTERVAL).await;
            }
            Err(error) => return Err(raft_engine_error("change cluster membership", error)),
        }
        if tokio::time::Instant::now() >= deadline && current_voters(raft) != desired_voters {
            return Err(BrokerError::new(
                BrokerErrorCode::TransportError,
                "cluster membership did not converge to three voters before the deadline",
            ));
        }
    }
}

fn current_voters(raft: &Raft<AgentBrokerRaftTypeConfig>) -> BTreeSet<AgentBrokerRaftNodeId> {
    raft.metrics()
        .borrow()
        .membership_config
        .membership()
        .voter_ids()
        .collect()
}

fn current_members(raft: &Raft<AgentBrokerRaftTypeConfig>) -> BTreeSet<AgentBrokerRaftNodeId> {
    raft.metrics()
        .borrow()
        .membership_config
        .membership()
        .nodes()
        .map(|(node_id, _node)| *node_id)
        .collect()
}

fn not_leader_error(
    node_id: AgentBrokerRaftNodeId,
    leader: Option<AgentBrokerRaftNodeId>,
) -> BrokerError {
    BrokerError::new(
        BrokerErrorCode::TransportError,
        match leader {
            Some(leader) => {
                format!("cluster node {node_id} is not leader; current leader is {leader}")
            }
            None => format!("cluster node {node_id} is not leader and no current leader is known"),
        },
    )
}

fn ensure_parent_directory(path: &Path) -> Result<(), BrokerError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(parent).map_err(|error| {
        BrokerError::new(
            BrokerErrorCode::PersistenceError,
            format!("failed to create cluster Raft state directory: {error}"),
        )
    })
}

fn raft_storage_error(error: impl std::fmt::Display) -> BrokerError {
    BrokerError::new(
        BrokerErrorCode::PersistenceError,
        format!("cluster Raft storage failed: {error}"),
    )
}

fn raft_fatal_error(error: impl std::fmt::Display) -> BrokerError {
    BrokerError::new(
        BrokerErrorCode::PersistenceError,
        format!("cluster Raft engine failed: {error}"),
    )
}

fn raft_engine_error(operation: &'static str, error: impl std::fmt::Display) -> BrokerError {
    BrokerError::new(
        BrokerErrorCode::TransportError,
        format!("cluster Raft {operation} failed: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;

    use super::{CONTROL_QUEUE_CAPACITY, ClusterRaftObserver, ClusterRaftReadinessStatus};

    #[test]
    fn readiness_fails_closed_when_consensus_fail_stop_is_latched() {
        let (requests, _receiver) = mpsc::sync_channel(CONTROL_QUEUE_CAPACITY);
        let observer = ClusterRaftObserver {
            requests,
            fail_stopped: Arc::new(AtomicBool::new(true)),
        };

        let readiness = observer.readiness();
        assert_eq!(
            readiness.status,
            ClusterRaftReadinessStatus::ConsensusFailStopped
        );
        assert!(!readiness.is_write_ready());
        assert!(readiness.progress.is_none());
    }
}

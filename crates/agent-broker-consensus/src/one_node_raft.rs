use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use agent_broker_application::{BrokerError, BrokerErrorCode, ConsensusAdapter};
use agent_broker_domain::commands::{AdvanceTermCommand, BrokerCommand};
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{BrokerState, Revision, Term};
use openraft::raft::Raft;
use openraft::{BasicNode, Config, SnapshotPolicy};

use crate::ReplicatedBrokerCommandV1;
use crate::one_node_network::OneNodeRaftNetworkFactory;
use crate::raft_log_storage::AgentBrokerRaftLogStorage;
use crate::raft_observation::RaftBrokerObservation;
use crate::raft_persistence::RedbRaftPersistence;
use crate::raft_state_machine::AgentBrokerRaftStateMachine;
use crate::raft_type_config::{AgentBrokerRaftNodeId, AgentBrokerRaftTypeConfig};

const ONE_NODE_ID: AgentBrokerRaftNodeId = 1;
const CONTROL_QUEUE_CAPACITY: usize = 64;
const INITIAL_LEADER_WAIT: Duration = Duration::from_millis(500);
const FORCED_LEADER_WAIT: Duration = Duration::from_secs(3);
const LEADER_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_SNAPSHOT_LOG_INTERVAL: u64 = 256;

/// Local durable configuration for the one-node Raft equivalence path.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OneNodeRaftConfig {
    state_path: PathBuf,
    snapshot_log_interval: u64,
}

impl OneNodeRaftConfig {
    /// Use a durable redb file as the one-node Raft authority.
    #[must_use]
    pub fn new(state_path: impl Into<PathBuf>) -> Self {
        Self {
            state_path: state_path.into(),
            snapshot_log_interval: DEFAULT_SNAPSHOT_LOG_INTERVAL,
        }
    }

    /// Override the number of committed logs between automatic snapshots.
    pub fn with_snapshot_log_interval(mut self, interval: u64) -> Result<Self, BrokerError> {
        if interval == 0 {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "one-node Raft snapshot log interval must be greater than zero",
            ));
        }
        self.snapshot_log_interval = interval;
        Ok(self)
    }

    #[must_use]
    pub fn state_path(&self) -> &Path {
        &self.state_path
    }
}

/// Explicit Raft/Broker progress snapshot used by one-node equivalence tests and diagnostics.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OneNodeRaftProgress {
    pub raft_term: u64,
    pub last_log_index: Option<u64>,
    pub committed_index: Option<u64>,
    pub applied_index: Option<u64>,
    pub broker_term: Term,
    pub broker_revision: Revision,
    pub current_leader: Option<AgentBrokerRaftNodeId>,
    pub remote_attempt_count: u64,
}

/// Synchronous `ConsensusAdapter` facade backed by a real one-node OpenRaft engine.
///
/// OpenRaft and its Tokio runtime live on a dedicated controller thread. This preserves the
/// synchronous application boundary without ever nesting `Runtime::block_on` inside a caller's
/// async executor. The actual Broker state remains exclusively owned by `RaftStateMachine`.
pub struct OneNodeRaftConsensusAdapter {
    requests: SyncSender<ControllerRequest>,
    controller: Option<JoinHandle<()>>,
    term: Term,
    revision: Revision,
    poisoned: bool,
}

impl std::fmt::Debug for OneNodeRaftConsensusAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OneNodeRaftConsensusAdapter")
            .field("term", &self.term)
            .field("revision", &self.revision)
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl OneNodeRaftConsensusAdapter {
    /// Open or recover a durable one-node OpenRaft group and wait until node 1 is leader.
    pub fn open(config: OneNodeRaftConfig) -> Result<Self, BrokerError> {
        ensure_parent_directory(config.state_path())?;

        let (requests, receiver) = mpsc::sync_channel(CONTROL_QUEUE_CAPACITY);
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let controller = thread::Builder::new()
            .name("agent-broker-one-node-raft".to_owned())
            .spawn(move || run_controller(config, receiver, startup_sender))
            .map_err(|error| {
                BrokerError::new(
                    BrokerErrorCode::InternalError,
                    format!("failed to start one-node Raft controller thread: {error}"),
                )
            })?;

        let startup = startup_receiver.recv().map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("one-node Raft controller exited during startup: {error}"),
            )
        })?;
        let progress = match startup {
            Ok(progress) => progress,
            Err(error) => {
                let _join_result = controller.join();
                return Err(error);
            }
        };

        Ok(Self {
            requests,
            controller: Some(controller),
            term: progress.broker_term,
            revision: progress.broker_revision,
            poisoned: false,
        })
    }

    /// Read distinct Raft and Broker progress pointers without exposing mutable state.
    pub fn progress(&mut self) -> Result<OneNodeRaftProgress, BrokerError> {
        self.ensure_available()?;
        let reply = self.request(|sender| ControllerRequest::Progress { reply: sender })?;
        let progress = reply?;
        self.term = progress.broker_term;
        self.revision = progress.broker_revision;
        Ok(progress)
    }

    /// Trigger and wait for a durable snapshot. Intended for parity/recovery verification.
    pub fn trigger_snapshot(&mut self) -> Result<OneNodeRaftProgress, BrokerError> {
        self.ensure_available()?;
        let reply = self.request(|sender| ControllerRequest::TriggerSnapshot { reply: sender })?;
        let progress = reply?;
        self.term = progress.broker_term;
        self.revision = progress.broker_revision;
        Ok(progress)
    }

    /// Shut down OpenRaft and join its dedicated controller thread.
    pub fn shutdown(mut self) -> Result<(), BrokerError> {
        let shutdown_result = if self.poisoned {
            Ok(())
        } else {
            self.request(|sender| ControllerRequest::Shutdown { reply: sender })?
        };
        let join_result = self.join_controller();
        shutdown_result.and(join_result)
    }

    fn ensure_available(&self) -> Result<(), BrokerError> {
        if self.poisoned {
            return Err(BrokerError::new(
                BrokerErrorCode::PersistenceError,
                "one-node Raft consensus adapter is fail-stopped after an engine failure",
            ));
        }
        Ok(())
    }

    fn request<T>(
        &self,
        build: impl FnOnce(SyncSender<T>) -> ControllerRequest,
    ) -> Result<T, BrokerError> {
        let (reply_sender, reply_receiver) = mpsc::sync_channel(1);
        self.requests.send(build(reply_sender)).map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::PersistenceError,
                format!("one-node Raft controller is unavailable: {error}"),
            )
        })?;
        reply_receiver.recv().map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::PersistenceError,
                format!("one-node Raft controller dropped the response: {error}"),
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
                "one-node Raft controller thread panicked",
            )
        })
    }
}

impl ConsensusAdapter for OneNodeRaftConsensusAdapter {
    fn term(&self) -> Term {
        self.term
    }

    fn revision(&self) -> Revision {
        self.revision
    }

    fn propose(&mut self, command: BrokerCommand) -> Result<BrokerMutationResult, BrokerError> {
        self.ensure_available()?;
        if matches!(command, BrokerCommand::AdvanceTerm(_)) {
            return Err(BrokerError::new(
                BrokerErrorCode::InvalidRequest,
                "Broker term is owned by Raft in one-node consensus mode",
            ));
        }

        let reply = match self.request(|sender| ControllerRequest::Propose {
            command,
            reply: sender,
        }) {
            Ok(reply) => reply,
            Err(error) => {
                self.poisoned = true;
                return Err(error);
            }
        };
        match reply {
            Ok(reply) => {
                self.term = reply.progress.broker_term;
                self.revision = reply.progress.broker_revision;
                reply.result
            }
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
        }
    }
}

impl Drop for OneNodeRaftConsensusAdapter {
    fn drop(&mut self) {
        // Explicit `shutdown(self)` is the error-reporting path. Drop is deliberately non-blocking
        // with respect to application correctness: dropping the request sender lets the controller
        // observe disconnection, shut OpenRaft down, and exit. No mutation is acknowledged here.
    }
}

enum ControllerRequest {
    Propose {
        command: BrokerCommand,
        reply: SyncSender<Result<ControllerProposeReply, BrokerError>>,
    },
    Progress {
        reply: SyncSender<Result<OneNodeRaftProgress, BrokerError>>,
    },
    TriggerSnapshot {
        reply: SyncSender<Result<OneNodeRaftProgress, BrokerError>>,
    },
    Shutdown {
        reply: SyncSender<Result<(), BrokerError>>,
    },
}

struct ControllerProposeReply {
    result: Result<BrokerMutationResult, BrokerError>,
    progress: OneNodeRaftProgress,
}

struct RunningOneNodeRaft {
    raft: Raft<AgentBrokerRaftTypeConfig>,
    observation: Arc<RaftBrokerObservation>,
    network: OneNodeRaftNetworkFactory,
}

fn run_controller(
    config: OneNodeRaftConfig,
    receiver: Receiver<ControllerRequest>,
    startup_sender: SyncSender<Result<OneNodeRaftProgress, BrokerError>>,
) {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .thread_name("agent-broker-raft-runtime")
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _send_result = startup_sender.send(Err(BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("failed to build one-node Raft Tokio runtime: {error}"),
            )));
            return;
        }
    };

    let running = match runtime.block_on(RunningOneNodeRaft::open(config)) {
        Ok(running) => running,
        Err(error) => {
            let _send_result = startup_sender.send(Err(error));
            return;
        }
    };
    let startup_progress = runtime.block_on(running.progress());
    if startup_sender.send(startup_progress).is_err() {
        let _shutdown_result = runtime.block_on(running.raft.shutdown());
        return;
    }

    while let Ok(request) = receiver.recv() {
        match request {
            ControllerRequest::Propose { command, reply } => {
                let result = runtime.block_on(running.propose(command));
                let _send_result = reply.send(result);
            }
            ControllerRequest::Progress { reply } => {
                let result = runtime.block_on(running.progress());
                let _send_result = reply.send(result);
            }
            ControllerRequest::TriggerSnapshot { reply } => {
                let result = runtime.block_on(running.trigger_snapshot());
                let _send_result = reply.send(result);
            }
            ControllerRequest::Shutdown { reply } => {
                let result = runtime.block_on(running.raft.shutdown()).map_err(|error| {
                    BrokerError::new(
                        BrokerErrorCode::InternalError,
                        format!("one-node Raft shutdown join failed: {error}"),
                    )
                });
                let _send_result = reply.send(result);
                return;
            }
        }
    }

    let _shutdown_result = runtime.block_on(running.raft.shutdown());
}

impl RunningOneNodeRaft {
    async fn open(config: OneNodeRaftConfig) -> Result<Self, BrokerError> {
        let persistence = RedbRaftPersistence::open(config.state_path()).map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::PersistenceError,
                format!("failed to open one-node Raft redb store: {error}"),
            )
        })?;
        let observation = Arc::new(RaftBrokerObservation::new(&BrokerState::default(), None));
        let log_store = AgentBrokerRaftLogStorage::new(persistence.clone());
        let state_machine = AgentBrokerRaftStateMachine::load(persistence, observation.clone())
            .await
            .map_err(raft_storage_error)?;
        let network = OneNodeRaftNetworkFactory::default();
        let raft_config = Config {
            cluster_name: "agent-broker-one-node".to_owned(),
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_log_interval),
            ..Config::default()
        }
        .validate()
        .map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("invalid one-node Raft configuration: {error}"),
            )
        })?;

        let raft = Raft::new(
            ONE_NODE_ID,
            Arc::new(raft_config),
            network.clone(),
            log_store,
            state_machine,
        )
        .await
        .map_err(raft_fatal_error)?;

        if !raft.is_initialized().await.map_err(raft_fatal_error)? {
            raft.initialize(BTreeMap::from([(
                ONE_NODE_ID,
                BasicNode::new("agent-broker-one-node"),
            )]))
            .await
            .map_err(|error| raft_engine_error("initialize", error))?;
        }

        if !wait_for_local_leader(&raft, INITIAL_LEADER_WAIT).await {
            raft.trigger().elect().await.map_err(raft_fatal_error)?;
            if !wait_for_local_leader(&raft, FORCED_LEADER_WAIT).await {
                let _shutdown_result = raft.shutdown().await;
                return Err(BrokerError::new(
                    BrokerErrorCode::TransportError,
                    "one-node Raft did not elect node 1 as leader within the startup deadline",
                ));
            }
        }

        let running = Self {
            raft,
            observation,
            network,
        };
        running.wait_until_committed_is_applied().await?;
        running.sync_broker_term_to_raft().await?;
        if running.network.remote_attempt_count() != 0 {
            return Err(BrokerError::new(
                BrokerErrorCode::InternalError,
                "one-node Raft unexpectedly attempted a remote RPC during startup",
            ));
        }
        Ok(running)
    }

    async fn propose(&self, command: BrokerCommand) -> Result<ControllerProposeReply, BrokerError> {
        self.sync_broker_term_to_raft().await?;
        let data = ReplicatedBrokerCommandV1::try_from(command).map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("failed to encode Broker command for Raft: {error}"),
            )
        })?;
        let response = self
            .raft
            .client_write::<tokio::sync::oneshot::error::RecvError>(data)
            .await
            .map_err(|error| raft_engine_error("client_write", error))?;
        let result = response.data.into_application_result().map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("failed to decode committed Raft application response: {error}"),
            )
        })?;
        let progress = self.progress().await?;
        if progress.remote_attempt_count != 0 {
            return Err(BrokerError::new(
                BrokerErrorCode::InternalError,
                "one-node Raft unexpectedly attempted a remote RPC while committing a command",
            ));
        }
        Ok(ControllerProposeReply { result, progress })
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
                    "Raft term {} is behind committed Broker term {}",
                    raft_term,
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
                format!("Raft produced an invalid Broker fencing term: {error}"),
            )
        })?;
        let command = BrokerCommand::AdvanceTerm(AdvanceTermCommand {
            expected_term: broker_term,
            new_term,
        });
        let data = ReplicatedBrokerCommandV1::try_from(command).map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("failed to encode internal Raft term-advance command: {error}"),
            )
        })?;
        let response = self
            .raft
            .client_write::<tokio::sync::oneshot::error::RecvError>(data)
            .await
            .map_err(|error| raft_engine_error("term synchronization", error))?;
        match response.data.into_application_result().map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("failed to decode internal Raft term response: {error}"),
            )
        })? {
            Ok(BrokerMutationResult::TermAdvanced(_)) => Ok(()),
            Ok(other) => Err(BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("Raft term synchronization returned unexpected result: {other:?}"),
            )),
            Err(error) => Err(BrokerError::new(
                BrokerErrorCode::InternalError,
                format!("Raft term synchronization was rejected by Broker state: {error}"),
            )),
        }
    }

    async fn trigger_snapshot(&self) -> Result<OneNodeRaftProgress, BrokerError> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(raft_fatal_error)?;
        let target = self.observation.applied_index();
        let deadline = tokio::time::Instant::now() + FORCED_LEADER_WAIT;
        loop {
            let snapshot = self
                .raft
                .get_snapshot()
                .await
                .map_err(|error| raft_engine_error("read current snapshot", error))?;
            let snapshot_index = snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.meta.last_log_id)
                .map(|log_id| log_id.index);
            let reached_target = match target {
                Some(target_index) => snapshot_index.is_some_and(|index| index >= target_index),
                None => snapshot.is_some(),
            };
            if reached_target {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(BrokerError::new(
                    BrokerErrorCode::PersistenceError,
                    "one-node Raft snapshot did not reach the applied log before the deadline",
                ));
            }
            tokio::time::sleep(LEADER_POLL_INTERVAL).await;
        }
        self.progress().await
    }

    async fn progress(&self) -> Result<OneNodeRaftProgress, BrokerError> {
        let metrics = self.raft.metrics().borrow().clone();
        let committed_index = self
            .raft
            .with_raft_state(|state| state.committed.map(|log_id| log_id.index))
            .await
            .map_err(raft_fatal_error)?;
        let broker_term = self
            .observation
            .validated_term()
            .map_err(|error| BrokerError::new(BrokerErrorCode::InternalError, error.to_string()))?;
        Ok(OneNodeRaftProgress {
            raft_term: metrics.current_term,
            last_log_index: metrics.last_log_index,
            committed_index,
            applied_index: self.observation.applied_index(),
            broker_term,
            broker_revision: self.observation.revision(),
            current_leader: metrics.current_leader,
            remote_attempt_count: self.network.remote_attempt_count(),
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
        let deadline = tokio::time::Instant::now() + FORCED_LEADER_WAIT;
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
                    "one-node Raft committed log was not applied before the startup deadline",
                ));
            }
            tokio::time::sleep(LEADER_POLL_INTERVAL).await;
        }
    }
}

async fn wait_for_local_leader(raft: &Raft<AgentBrokerRaftTypeConfig>, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if raft.current_leader().await == Some(ONE_NODE_ID) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(LEADER_POLL_INTERVAL).await;
    }
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
            format!("failed to create one-node Raft state directory: {error}"),
        )
    })
}

fn raft_storage_error(error: impl std::fmt::Display) -> BrokerError {
    BrokerError::new(
        BrokerErrorCode::PersistenceError,
        format!("one-node Raft storage failed: {error}"),
    )
}

fn raft_fatal_error(error: impl std::fmt::Display) -> BrokerError {
    BrokerError::new(
        BrokerErrorCode::PersistenceError,
        format!("one-node Raft engine failed: {error}"),
    )
}

fn raft_engine_error(operation: &'static str, error: impl std::fmt::Display) -> BrokerError {
    BrokerError::new(
        BrokerErrorCode::TransportError,
        format!("one-node Raft {operation} failed: {error}"),
    )
}

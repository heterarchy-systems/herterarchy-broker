use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::{Duration, Instant};

use agent_broker_application::{
    BrokerApplicationService, BrokerError, BrokerErrorCode, BrokerErrorDisposition,
    CommandSessionId, ConsensusAdapter, SessionOwnerEpoch, SessionOwnerInstanceId,
};
use agent_broker_client::{
    BrokerClient, BrokerClientConfig, ClaimInput, ClientError, CompleteInput,
};
use agent_broker_consensus::StandaloneConsensusAdapter;
use agent_broker_domain::commands::BrokerCommand;
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{
    BrokerCapacityPolicy, ConsumerGroupId, ConsumerId, LeaseDurationMs, LeaseId, NamespaceId,
    TaskId, TaskObjective, TaskResult, TaskStatus, Term,
};
use agent_broker_protocol::{BrokerRequestDispatcher, DeclaredCapabilities};
use agent_broker_runtime::{
    BrokerBindPolicy, BrokerServerConfig, BrokerStateProcessLock, RuntimeError, StateOwnerHandle,
    TcpBrokerServer,
};
use agent_broker_storage::{JournalCompactionPolicy, JournaledBrokerStateRepository};
use tempfile::tempdir;

struct TestServer {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<Result<(), RuntimeError>>,
}

struct BlockingConsensus {
    inner: StandaloneConsensusAdapter<JournaledBrokerStateRepository>,
    entered: SyncSender<()>,
    release: Receiver<()>,
    block_first: bool,
}

impl ConsensusAdapter for BlockingConsensus {
    fn term(&self) -> Term {
        self.inner.term()
    }

    fn revision(&self) -> agent_broker_domain::Revision {
        self.inner.revision()
    }

    fn propose(&mut self, command: BrokerCommand) -> Result<BrokerMutationResult, BrokerError> {
        if self.block_first {
            self.block_first = false;
            self.entered.send(()).map_err(|_| {
                BrokerError::new(
                    BrokerErrorCode::InternalError,
                    "saturation test entry barrier disconnected",
                )
            })?;
            self.release.recv().map_err(|_| {
                BrokerError::new(
                    BrokerErrorCode::InternalError,
                    "saturation test release barrier disconnected",
                )
            })?;
        }
        self.inner.propose(command)
    }
}

impl TestServer {
    fn stop(self) -> Result<(), Box<dyn Error>> {
        self.stop.store(true, Ordering::Release);
        match self.handle.join() {
            Ok(result) => result.map_err(Into::into),
            Err(_) => Err("Broker server thread panicked".into()),
        }
    }
}

fn spawn_test_server(state_path: std::path::PathBuf) -> Result<TestServer, Box<dyn Error>> {
    let repository = JournaledBrokerStateRepository::new(
        state_path,
        None,
        JournalCompactionPolicy::new(10_000, 64 * 1024 * 1024)?,
    );
    let consensus = StandaloneConsensusAdapter::new(repository)?;
    let service = BrokerApplicationService::new(consensus, BrokerCapacityPolicy::default());
    let dispatcher = BrokerRequestDispatcher::new(service);
    let state_owner = StateOwnerHandle::spawn(dispatcher, 16)?;
    let server = TcpBrokerServer::bind(
        BrokerServerConfig {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            max_frame_bytes: 128 * 1024,
            max_connections: 16,
            connection_io_timeout: Duration::from_millis(250),
        },
        state_owner,
    )?;
    let address = server.local_addr()?;
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let handle = thread::Builder::new()
        .name("agent-broker-test-server".to_owned())
        .spawn(move || server.serve_until(thread_stop.as_ref()))?;
    Ok(TestServer {
        address,
        stop,
        handle,
    })
}

fn wait_for_state_owner_load(
    owner: &StateOwnerHandle,
    active_jobs: usize,
    queued_jobs: usize,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let load = owner.load();
        if load.active_jobs == active_jobs && load.queued_jobs == queued_jobs {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "state-owner load did not reach active={active_jobs} queued={queued_jobs}; last={load:?}"
            )
            .into());
        }
        thread::yield_now();
    }
}

fn assert_sustained_overload_rejected(
    client: &mut BrokerClient,
    owner: &StateOwnerHandle,
    session_id: &CommandSessionId,
    owner_instance_id: &SessionOwnerInstanceId,
    attempts: usize,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..attempts {
        let overloaded = client.acquire_command_session_owner(
            session_id.clone(),
            SessionOwnerEpoch::INITIAL,
            owner_instance_id.clone(),
        );
        let Err(ClientError::Broker(error)) = overloaded else {
            return Err(format!("expected typed saturation rejection, got {overloaded:?}").into());
        };
        assert_eq!(error.code(), BrokerErrorCode::CapacityExceeded);
        assert_eq!(error.disposition(), BrokerErrorDisposition::Rejected);
        let load = owner.load();
        assert_eq!(load.active_jobs, 1);
        assert_eq!(load.queued_jobs, 1);
        assert_eq!(load.capacity, 1);
    }
    Ok(())
}

fn join_client<T>(
    handle: thread::JoinHandle<Result<T, ClientError>>,
    label: &'static str,
) -> Result<T, Box<dyn Error>> {
    match handle.join() {
        Ok(result) => Ok(result?),
        Err(_) => Err(format!("{label} thread panicked").into()),
    }
}

#[test]
fn saturated_state_owner_rejects_v3_before_application_and_recovers_after_drain()
-> Result<(), Box<dyn Error>> {
    const OVERLOAD_ATTEMPTS: usize = 64;
    let directory = tempdir()?;
    let repository = JournaledBrokerStateRepository::new(
        directory.path().join("saturation-state.json"),
        None,
        JournalCompactionPolicy::new(10_000, 64 * 1024 * 1024)?,
    );
    let inner = StandaloneConsensusAdapter::new(repository)?;
    let (entered_sender, entered_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let service = BrokerApplicationService::new(
        BlockingConsensus {
            inner,
            entered: entered_sender,
            release: release_receiver,
            block_first: true,
        },
        BrokerCapacityPolicy::default(),
    );
    let owner = StateOwnerHandle::spawn(BrokerRequestDispatcher::new(service), 1)?;
    let server = TcpBrokerServer::bind(
        BrokerServerConfig {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            max_frame_bytes: 128 * 1024,
            max_connections: 8,
            connection_io_timeout: Duration::from_secs(2),
        },
        owner.clone(),
    )?;
    let address = server.local_addr()?;
    let stop = Arc::new(AtomicBool::new(false));
    let server_stop = Arc::clone(&stop);
    let server_thread = thread::Builder::new()
        .name("agent-broker-saturation-server".to_owned())
        .spawn(move || server.serve_until(server_stop.as_ref()))?;

    let first_namespace = NamespaceId::new("saturation-first")?;
    let first = thread::spawn(move || {
        let mut client = BrokerClient::new(BrokerClientConfig {
            address,
            timeout: Duration::from_secs(2),
            max_response_frame_bytes: 128 * 1024,
        })?;
        client.ensure_namespace(first_namespace)
    });
    entered_receiver.recv_timeout(Duration::from_secs(2))?;
    wait_for_state_owner_load(&owner, 1, 0)?;

    let second_namespace = NamespaceId::new("saturation-second")?;
    let second = thread::spawn(move || {
        let mut client = BrokerClient::new(BrokerClientConfig {
            address,
            timeout: Duration::from_secs(2),
            max_response_frame_bytes: 128 * 1024,
        })?;
        client.ensure_namespace(second_namespace)
    });
    wait_for_state_owner_load(&owner, 1, 1)?;

    let mut overload_client = BrokerClient::new(BrokerClientConfig {
        address,
        timeout: Duration::from_millis(250),
        max_response_frame_bytes: 128 * 1024,
    })?;
    let overload_session = CommandSessionId::new("saturation-v3-session")?;
    let overload_owner = SessionOwnerInstanceId::new("saturation-v3-owner")?;
    assert_sustained_overload_rejected(
        &mut overload_client,
        &owner,
        &overload_session,
        &overload_owner,
        OVERLOAD_ATTEMPTS,
    )?;

    release_sender.send(())?;
    let _ = join_client(first, "first saturation client")?;
    let _ = join_client(second, "second saturation client")?;
    wait_for_state_owner_load(&owner, 0, 0)?;

    overload_client.ensure_namespace(NamespaceId::new("saturation-after-drain")?)?;
    assert_eq!(owner.load().queued_jobs, 0);
    overload_client.close();
    stop.store(true, Ordering::Release);
    match server_thread.join() {
        Ok(result) => result?,
        Err(_) => return Err("saturation server thread panicked".into()),
    }
    Ok(())
}

#[test]
fn server_config_rejects_non_loopback_bind() {
    let config = BrokerServerConfig {
        address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8_811),
        max_frame_bytes: 128 * 1024,
        max_connections: 16,
        connection_io_timeout: Duration::from_secs(30),
    };
    assert!(config.validate().is_err());
    assert!(
        config
            .validate_with_policy(BrokerBindPolicy::ContainerBridge)
            .is_ok()
    );
    let mut zero_timeout = config;
    zero_timeout.connection_io_timeout = Duration::ZERO;
    assert!(
        zero_timeout
            .validate_with_policy(BrokerBindPolicy::ContainerBridge)
            .is_err()
    );
}

#[test]
fn state_process_lock_excludes_second_owner() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let state_path = directory.path().join("broker-state.json");
    let first = BrokerStateProcessLock::acquire(&state_path)?;
    let second = BrokerStateProcessLock::acquire(&state_path);
    assert!(matches!(second, Err(RuntimeError::StateAlreadyOwned)));
    drop(first);
    let third = BrokerStateProcessLock::acquire(&state_path)?;
    assert!(third.lock_path().ends_with("broker-state.json.lock"));
    Ok(())
}

#[test]
fn reusable_tcp_connection_runs_full_typed_lifecycle_through_durable_state_owner()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let state_path = directory.path().join("broker-state.json");
    let server = spawn_test_server(state_path)?;
    let mut client = BrokerClient::new(BrokerClientConfig {
        address: server.address,
        timeout: Duration::from_secs(2),
        max_response_frame_bytes: 128 * 1024,
    })?;

    let initial = client.health()?;
    assert_eq!(initial.term.get(), 1);
    assert_eq!(initial.revision.get(), 0);

    let namespace_id = NamespaceId::new("project-a")?;
    client.ensure_namespace(namespace_id.clone())?;
    let task_id = TaskId::new("task-1")?;
    client.publish_task(
        namespace_id.clone(),
        task_id.clone(),
        TaskObjective::new("runtime e2e")?,
    )?;
    let group_id = ConsumerGroupId::new("engineering")?;
    client.ensure_consumer_group(namespace_id, group_id.clone())?;
    let member_id = ConsumerId::new("worker-a")?;
    let joined = client.join_consumer_group(
        group_id.clone(),
        member_id.clone(),
        DeclaredCapabilities::new(["code", "review", "code"])?,
    )?;
    assert_eq!(joined.generation.get(), 1);

    let lease_id = LeaseId::new("lease-1")?;
    let claimed = client.claim_task(ClaimInput {
        group_id: group_id.clone(),
        member_id: member_id.clone(),
        expected_term: initial.term,
        expected_generation: joined.generation,
        lease_id: lease_id.clone(),
        lease_duration: LeaseDurationMs::new(60_000)?,
    })?;
    assert_eq!(claimed.task_id.as_ref(), Some(&task_id));
    let Some(lease_epoch) = claimed.lease_epoch else {
        return Err("claim must contain a lease epoch".into());
    };
    let completed = client.complete_task(CompleteInput {
        task_id,
        group_id,
        member_id,
        expected_term: initial.term,
        expected_generation: joined.generation,
        expected_lease_epoch: lease_epoch,
        lease_id,
        result: TaskResult::new("done")?,
    })?;
    assert_eq!(completed.status, TaskStatus::Completed);
    assert!(client.health()?.revision.get() >= 6);

    client.close();
    server.stop()?;
    Ok(())
}

#[test]
fn stale_fence_returns_typed_broker_error_without_breaking_connection() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let server = spawn_test_server(directory.path().join("broker-state.json"))?;
    let mut client = BrokerClient::new(BrokerClientConfig {
        address: server.address,
        timeout: Duration::from_secs(2),
        max_response_frame_bytes: 128 * 1024,
    })?;
    let namespace_id = NamespaceId::new("project-a")?;
    client.ensure_namespace(namespace_id.clone())?;
    let group_id = ConsumerGroupId::new("engineering")?;
    client.ensure_consumer_group(namespace_id, group_id.clone())?;
    let member_id = ConsumerId::new("worker-a")?;
    let joined = client.join_consumer_group(
        group_id.clone(),
        member_id.clone(),
        DeclaredCapabilities::new(["code"])?,
    )?;
    let result = client.claim_task(ClaimInput {
        group_id,
        member_id,
        expected_term: agent_broker_domain::Term::new(2)?,
        expected_generation: joined.generation,
        lease_id: LeaseId::new("lease-stale")?,
        lease_duration: LeaseDurationMs::new(60_000)?,
    });
    let Err(agent_broker_client::ClientError::Broker(error)) = result else {
        return Err("stale term must return a typed Broker error".into());
    };
    assert_eq!(error.code(), BrokerErrorCode::StaleFence);
    assert_eq!(client.health()?.term.get(), 1);

    client.close();
    server.stop()?;
    Ok(())
}

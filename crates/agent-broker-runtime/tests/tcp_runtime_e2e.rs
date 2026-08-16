use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use agent_broker_application::{BrokerApplicationService, BrokerErrorCode};
use agent_broker_client::{BrokerClient, BrokerClientConfig, ClaimInput, CompleteInput};
use agent_broker_consensus::StandaloneConsensusAdapter;
use agent_broker_domain::{
    BrokerCapacityPolicy, ConsumerGroupId, LeaseDurationMs, LeaseId, MemberId, NamespaceId, TaskId,
    TaskObjective, TaskResult, TaskStatus,
};
use agent_broker_protocol::{BrokerRequestDispatcher, DeclaredCapabilities};
use agent_broker_runtime::{
    BrokerServerConfig, BrokerStateProcessLock, RuntimeError, StateOwnerHandle, TcpBrokerServer,
};
use agent_broker_storage::{JournalCompactionPolicy, JournaledBrokerStateRepository};
use tempfile::tempdir;

struct TestServer {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<Result<(), RuntimeError>>,
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

#[test]
fn server_config_rejects_non_loopback_bind() {
    let config = BrokerServerConfig {
        address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8_811),
        max_frame_bytes: 128 * 1024,
        max_connections: 16,
    };
    assert!(config.validate().is_err());
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
    let member_id = MemberId::new("worker-a")?;
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
    let member_id = MemberId::new("worker-a")?;
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

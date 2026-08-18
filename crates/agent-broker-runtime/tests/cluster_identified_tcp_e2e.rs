use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Barrier};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use agent_broker_application::{
    BrokerApplicationService, BrokerError, BrokerErrorCode, BrokerErrorDisposition,
    CommandIdentity, CommandSequence, CommandSessionId, ConsensusAdapter, SessionOwnerEpoch,
    SessionOwnerInstanceId,
};
use agent_broker_client::{
    BrokerClient, BrokerClientConfig, ClientError, ClientSessionStoreError,
    DurableClientSessionStore, DurableExecutionError, DurableRetryPolicy,
};
use agent_broker_consensus::{
    ClusterRaftConfig, ClusterRaftConsensusAdapter, ClusterRaftTlsConfig,
};
use agent_broker_domain::commands::BrokerCommand;
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{
    BrokerCapacityPolicy, ConsumerGroupId, MemberId, NamespaceId, TaskId, TaskObjective, Term,
    TimestampMs,
};
use agent_broker_protocol::{
    BrokerRequest, BrokerRequestDispatcher, DeclaredCapabilities, EnsureNamespaceRequest,
    IdentifiedBrokerRequest, OwnerAcquisitionRequestV3, PublishTaskRequest, RequestId,
    encode_identified_request, encode_owner_acquisition_request_with_limit,
};
use agent_broker_runtime::{
    BrokerServerConfig, ClusterOperationsObserver, LeaderMaintenanceResult, OperationsServer,
    OperationsServerConfig, RuntimeError, StandaloneMaintenancePolicy, StateOwnerHandle,
    TcpBrokerServer,
};
use serde_json::Value;
use tempfile::{TempDir, tempdir};

#[path = "../../agent-broker-consensus/test_support/tls_fixture.rs"]
mod tls_fixture;

const CLUSTER_WAIT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(20);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_FRAME_BYTES: usize = 128 * 1024;
const DURABLE_CLIENT_CHILD_ENV: &str = "AGENT_BROKER_DURABLE_CLIENT_CHILD";
const DURABLE_CLIENT_BACKEND_ENV: &str = "AGENT_BROKER_DURABLE_CLIENT_BACKEND";
const DURABLE_CLIENT_PROXY_ENV: &str = "AGENT_BROKER_DURABLE_CLIENT_PROXY";
const DURABLE_CLIENT_STATE_ENV: &str = "AGENT_BROKER_DURABLE_CLIENT_STATE";
type ProxyThread = JoinHandle<std::io::Result<()>>;

struct BlockingConsensus<C> {
    inner: C,
    entered: SyncSender<()>,
    release: Receiver<()>,
    block_first: bool,
}

impl<C: ConsensusAdapter> ConsensusAdapter for BlockingConsensus<C> {
    fn term(&self) -> Term {
        self.inner.term()
    }

    fn revision(&self) -> agent_broker_domain::Revision {
        self.inner.revision()
    }

    fn maintenance_authority(&mut self) -> Result<bool, BrokerError> {
        self.inner.maintenance_authority()
    }

    fn propose(&mut self, command: BrokerCommand) -> Result<BrokerMutationResult, BrokerError> {
        if self.block_first {
            self.block_first = false;
            self.entered.send(()).map_err(|_| {
                BrokerError::new(
                    BrokerErrorCode::InternalError,
                    "cluster saturation entry barrier disconnected",
                )
            })?;
            self.release.recv().map_err(|_| {
                BrokerError::new(
                    BrokerErrorCode::InternalError,
                    "cluster saturation release barrier disconnected",
                )
            })?;
        }
        self.inner.propose(command)
    }
}

struct RunningTcpCluster {
    _directory: TempDir,
    state_owner: StateOwnerHandle,
    node_two: ClusterRaftConsensusAdapter,
    node_three: ClusterRaftConsensusAdapter,
    client_address: SocketAddr,
    operations_address: SocketAddr,
    stop: Arc<AtomicBool>,
    server_thread: Option<JoinHandle<Result<(), RuntimeError>>>,
    operations_thread: Option<JoinHandle<Result<(), RuntimeError>>>,
}

impl RunningTcpCluster {
    fn start() -> Result<Self, Box<dyn Error>> {
        Self::start_with(16, |node_one| node_one)
    }

    fn start_with<C, F>(
        max_inflight_requests: usize,
        wrap_leader: F,
    ) -> Result<Self, Box<dyn Error>>
    where
        C: ConsensusAdapter + Send + 'static,
        F: FnOnce(ClusterRaftConsensusAdapter) -> C,
    {
        let directory = tempdir()?;
        let (reservations, addresses) = reserve_three_ports()?;
        let tls_directory = directory.path().join("raft-tls");
        tls_fixture::write_cluster_tls_fixture(&tls_directory, &[1, 2, 3])?;
        let tls = ClusterRaftTlsConfig::new(tls_directory)?;
        let nodes = BTreeMap::from([
            (1, addresses[0].to_string()),
            (2, addresses[1].to_string()),
            (3, addresses[2].to_string()),
        ]);
        let node_one_config = ClusterRaftConfig::new(
            1,
            directory.path().join("node-1.redb"),
            addresses[0],
            nodes.clone(),
            tls.clone(),
            true,
        )?;
        let node_two_config = ClusterRaftConfig::new(
            2,
            directory.path().join("node-2.redb"),
            addresses[1],
            nodes.clone(),
            tls.clone(),
            false,
        )?;
        let node_three_config = ClusterRaftConfig::new(
            3,
            directory.path().join("node-3.redb"),
            addresses[2],
            nodes,
            tls,
            false,
        )?;

        let mut reservations = reservations.into_iter();
        let node_one_port = reservations.next().ok_or("missing node 1 reservation")?;
        let node_two_port = reservations.next().ok_or("missing node 2 reservation")?;
        let node_three_port = reservations.next().ok_or("missing node 3 reservation")?;

        drop(node_two_port);
        let mut node_two = ClusterRaftConsensusAdapter::open(node_two_config)?;
        drop(node_three_port);
        let mut node_three = ClusterRaftConsensusAdapter::open(node_three_config)?;
        drop(node_one_port);
        let mut node_one = ClusterRaftConsensusAdapter::open(node_one_config)?;

        let expected_voters = BTreeSet::from([1, 2, 3]);
        wait_for_cluster(&mut node_one, &expected_voters, 1)?;
        wait_for_cluster(&mut node_two, &expected_voters, 1)?;
        wait_for_cluster(&mut node_three, &expected_voters, 1)?;

        let consensus_observer = node_one.observer();
        let service =
            BrokerApplicationService::new(wrap_leader(node_one), BrokerCapacityPolicy::default());
        let dispatcher = BrokerRequestDispatcher::new(service);
        let state_owner = StateOwnerHandle::spawn(dispatcher, max_inflight_requests)?;
        let server = TcpBrokerServer::bind(
            BrokerServerConfig {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                max_frame_bytes: MAX_FRAME_BYTES,
                max_connections: 16,
                connection_io_timeout: CLIENT_TIMEOUT,
            },
            state_owner.clone(),
        )?;
        let server_observer = server.observer();
        let client_address = server.local_addr()?;
        let operations_server = OperationsServer::bind(
            OperationsServerConfig {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                ..OperationsServerConfig::default()
            },
            ClusterOperationsObserver::new(
                consensus_observer,
                state_owner.clone(),
                server_observer,
            ),
        )?;
        let operations_address = operations_server.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let server_thread = thread::Builder::new()
            .name("agent-broker-v2-cluster-tcp-e2e".to_owned())
            .spawn(move || server.serve_until(thread_stop.as_ref()))?;
        let operations_stop = Arc::clone(&stop);
        let operations_thread = thread::Builder::new()
            .name("agent-broker-operations-cluster-e2e".to_owned())
            .spawn(move || operations_server.serve_until(operations_stop.as_ref()))?;

        Ok(Self {
            _directory: directory,
            state_owner,
            node_two,
            node_three,
            client_address,
            operations_address,
            stop,
            server_thread: Some(server_thread),
            operations_thread: Some(operations_thread),
        })
    }

    fn client(&self) -> Result<BrokerClient, ClientError> {
        BrokerClient::new(BrokerClientConfig {
            address: self.client_address,
            timeout: CLIENT_TIMEOUT,
            max_response_frame_bytes: MAX_FRAME_BYTES,
        })
    }

    fn shutdown(mut self) -> Result<(), Box<dyn Error>> {
        self.stop.store(true, Ordering::Release);
        let server_thread = self
            .server_thread
            .take()
            .ok_or("server thread already joined")?;
        match server_thread.join() {
            Ok(result) => result?,
            Err(_) => return Err("cluster TCP server thread panicked".into()),
        }
        let operations_thread = self
            .operations_thread
            .take()
            .ok_or("operations server thread already joined")?;
        match operations_thread.join() {
            Ok(result) => result?,
            Err(_) => return Err("cluster operations server thread panicked".into()),
        }
        self.node_three.shutdown()?;
        self.node_two.shutdown()?;
        Ok(())
    }
}

fn operations_request(address: SocketAddr, operation: &str) -> Result<Value, Box<dyn Error>> {
    let mut stream = TcpStream::connect_timeout(&address, CLIENT_TIMEOUT)?;
    stream.set_read_timeout(Some(CLIENT_TIMEOUT))?;
    stream.set_write_timeout(Some(CLIENT_TIMEOUT))?;
    writeln!(
        stream,
        "{{\"schema_version\":1,\"operation\":\"{operation}\"}}"
    )?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    if reader.read_line(&mut response)? == 0 {
        return Err("operations server closed before a response".into());
    }
    Ok(serde_json::from_str(&response)?)
}

fn wait_for_operations_ready(address: SocketAddr) -> Result<Value, Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    loop {
        if let Ok(response) = operations_request(address, "readiness")
            && response.get("write_ready").and_then(Value::as_bool) == Some(true)
        {
            return Ok(response);
        }
        if Instant::now() >= deadline {
            return Err("operations readiness did not become ready before deadline".into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn assert_cluster_saturation_rejected(
    client: &mut BrokerClient,
    owner: &StateOwnerHandle,
    attempts: usize,
) -> Result<(), Box<dyn Error>> {
    let session_id = CommandSessionId::new("cluster-saturation-session")?;
    let owner_instance_id = SessionOwnerInstanceId::new("cluster-saturation-owner")?;
    for _ in 0..attempts {
        let result = client.acquire_command_session_owner(
            session_id.clone(),
            SessionOwnerEpoch::INITIAL,
            owner_instance_id.clone(),
        );
        let Err(ClientError::Broker(error)) = result else {
            return Err(format!("expected cluster saturation rejection, got {result:?}").into());
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

fn join_client_thread<T>(
    handle: JoinHandle<Result<T, ClientError>>,
    label: &'static str,
) -> Result<T, Box<dyn Error>> {
    match handle.join() {
        Ok(result) => Ok(result?),
        Err(_) => Err(format!("{label} thread panicked").into()),
    }
}

#[test]
fn cluster_operations_wire_reports_liveness_and_quorum_readiness() -> Result<(), Box<dyn Error>> {
    let cluster = RunningTcpCluster::start()?;
    let liveness = operations_request(cluster.operations_address, "liveness")?;
    assert_eq!(liveness.get("live").and_then(Value::as_bool), Some(true));

    let readiness = wait_for_operations_ready(cluster.operations_address)?;
    assert_eq!(
        readiness.get("reason").and_then(Value::as_str),
        Some("ready")
    );
    assert_eq!(
        readiness
            .pointer("/consensus/status")
            .and_then(Value::as_str),
        Some("ready")
    );
    assert_eq!(
        readiness
            .pointer("/consensus/progress/current_leader")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        readiness
            .pointer("/consensus/progress/node_id")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        readiness
            .get("maintenance_authority")
            .and_then(Value::as_bool),
        Some(true)
    );
    cluster.shutdown()
}

#[test]
fn cluster_application_queue_saturation_rejects_and_recovers_quorum_progress()
-> Result<(), Box<dyn Error>> {
    const OVERLOAD_ATTEMPTS: usize = 64;
    let (entered_sender, entered_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let mut cluster = RunningTcpCluster::start_with(1, |node_one| BlockingConsensus {
        inner: node_one,
        entered: entered_sender,
        release: release_receiver,
        block_first: true,
    })?;
    let address = cluster.client_address;

    let first_namespace = NamespaceId::new("cluster-saturation-first")?;
    let first = thread::spawn(move || {
        let mut client = BrokerClient::new(BrokerClientConfig {
            address,
            timeout: CLIENT_TIMEOUT,
            max_response_frame_bytes: MAX_FRAME_BYTES,
        })?;
        client.ensure_namespace(first_namespace)
    });
    entered_receiver.recv_timeout(CLIENT_TIMEOUT)?;
    wait_for_state_owner_load(&cluster.state_owner, 1, 0)?;

    let second_namespace = NamespaceId::new("cluster-saturation-second")?;
    let second = thread::spawn(move || {
        let mut client = BrokerClient::new(BrokerClientConfig {
            address,
            timeout: CLIENT_TIMEOUT,
            max_response_frame_bytes: MAX_FRAME_BYTES,
        })?;
        client.ensure_namespace(second_namespace)
    });
    wait_for_state_owner_load(&cluster.state_owner, 1, 1)?;

    let saturated = operations_request(cluster.operations_address, "status")?;
    assert_eq!(saturated.get("live").and_then(Value::as_bool), Some(true));
    assert_eq!(
        saturated.get("write_ready").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        saturated.get("reason").and_then(Value::as_str),
        Some("state_owner_saturated")
    );
    assert_eq!(
        saturated
            .pointer("/consensus/status")
            .and_then(Value::as_str),
        Some("ready")
    );
    assert_eq!(
        saturated
            .pointer("/state_owner/saturated")
            .and_then(Value::as_bool),
        Some(true)
    );

    let mut overload_client = cluster.client()?;
    assert_cluster_saturation_rejected(
        &mut overload_client,
        &cluster.state_owner,
        OVERLOAD_ATTEMPTS,
    )?;
    release_sender.send(())?;
    let _ = join_client_thread(first, "first cluster saturation client")?;
    let _ = join_client_thread(second, "second cluster saturation client")?;
    wait_for_state_owner_drain(&cluster.state_owner)?;
    let recovered = wait_for_operations_ready(cluster.operations_address)?;
    assert_eq!(
        recovered.get("reason").and_then(Value::as_str),
        Some("ready")
    );
    assert_eq!(
        recovered
            .pointer("/state_owner/saturated")
            .and_then(Value::as_bool),
        Some(false)
    );

    let before = overload_client.health()?;
    overload_client.ensure_namespace(NamespaceId::new("cluster-saturation-after-drain")?)?;
    let after = overload_client.health()?;
    assert_eq!(after.revision.get(), before.revision.get() + 1);
    wait_for_revision(&mut cluster.node_two, after.revision.get())?;
    wait_for_revision(&mut cluster.node_three, after.revision.get())?;
    overload_client.close();
    cluster.shutdown()
}

#[test]
fn cluster_client_connection_churn_drains_and_preserves_quorum_progress()
-> Result<(), Box<dyn Error>> {
    const CHURN_THREADS: usize = 8;
    const CONNECTIONS_PER_THREAD: usize = 32;

    let mut cluster = RunningTcpCluster::start()?;
    let barrier = Arc::new(Barrier::new(CHURN_THREADS));
    let mut churners = Vec::with_capacity(CHURN_THREADS);
    for index in 0..CHURN_THREADS {
        let address = cluster.client_address;
        let barrier = Arc::clone(&barrier);
        churners.push(
            thread::Builder::new()
                .name(format!("agent-broker-churn-{index}"))
                .spawn(move || -> Result<(), ClientError> {
                    barrier.wait();
                    for _ in 0..CONNECTIONS_PER_THREAD {
                        let mut client = BrokerClient::new(BrokerClientConfig {
                            address,
                            timeout: CLIENT_TIMEOUT,
                            max_response_frame_bytes: MAX_FRAME_BYTES,
                        })?;
                        let _ = client.health()?;
                        client.close();
                    }
                    Ok(())
                })?,
        );
    }
    for churner in churners {
        match churner.join() {
            Ok(result) => result?,
            Err(_) => return Err("cluster connection churn thread panicked".into()),
        }
    }
    wait_for_state_owner_drain(&cluster.state_owner)?;

    let mut client = cluster.client()?;
    let before = client.health()?;
    client.ensure_namespace(NamespaceId::new("post-churn-progress")?)?;
    let after = client.health()?;
    assert_eq!(after.revision.get(), before.revision.get() + 1);
    wait_for_revision(&mut cluster.node_two, after.revision.get())?;
    wait_for_revision(&mut cluster.node_three, after.revision.get())?;
    client.close();
    cluster.shutdown()
}

#[test]
fn cluster_maintenance_is_leader_gated_and_commits_through_raft() -> Result<(), Box<dyn Error>> {
    let mut cluster = RunningTcpCluster::start()?;
    assert!(!cluster.node_two.maintenance_authority()?);
    assert!(!cluster.node_three.maintenance_authority()?);

    let mut client = cluster.client()?;
    let namespace_id = NamespaceId::new("maintenance-project")?;
    let group_id = ConsumerGroupId::new("maintenance-group")?;
    let member_id = MemberId::new("maintenance-worker")?;
    client.ensure_namespace(namespace_id.clone())?;
    client.ensure_consumer_group(namespace_id, group_id.clone())?;
    client.join_consumer_group(
        group_id,
        member_id,
        DeclaredCapabilities::new(["maintenance-test"])?,
    )?;
    let before = client.health()?;

    let policy = StandaloneMaintenancePolicy::new(24 * 60 * 60 * 1_000, 1, 100, 32, 1, 32, 1)?;
    let result = cluster
        .state_owner
        .run_leader_maintenance(policy, TimestampMs::new(u64::MAX - 1))?;
    let LeaderMaintenanceResult::Applied(applied) = result else {
        return Err("leader maintenance was unexpectedly skipped".into());
    };
    assert_eq!(applied.reaped_stale_members, 1);
    assert_eq!(applied.pruned_completed_tasks, 0);

    let after = client.health()?;
    assert!(after.revision > before.revision);
    wait_for_revision(&mut cluster.node_two, after.revision.get())?;
    wait_for_revision(&mut cluster.node_three, after.revision.get())?;
    client.close();
    cluster.shutdown()
}

#[test]
fn cluster_tcp_v2_exact_retry_preserves_revision_and_conflict_is_rejected()
-> Result<(), Box<dyn Error>> {
    let mut cluster = RunningTcpCluster::start()?;
    let mut client = cluster.client()?;
    let committed_revision = verify_exact_identified_retry_and_conflict(&mut client)?;
    wait_for_revision(&mut cluster.node_two, committed_revision)?;
    wait_for_revision(&mut cluster.node_three, committed_revision)?;
    client.close();
    cluster.shutdown()
}

#[test]
fn cluster_tcp_v2_lost_response_recovers_committed_outcome_without_duplicate_apply()
-> Result<(), Box<dyn Error>> {
    let mut cluster = RunningTcpCluster::start()?;
    let mut observer = cluster.client()?;
    verify_lost_response_recovery(
        cluster.client_address,
        &mut observer,
        &mut cluster.node_two,
        &mut cluster.node_three,
    )?;
    observer.close();
    cluster.shutdown()
}

#[test]
fn cluster_tcp_v3_owner_acquisition_and_owned_mutation_are_fenced() -> Result<(), Box<dyn Error>> {
    let mut cluster = RunningTcpCluster::start()?;
    let mut client = cluster.client()?;
    let session_id = CommandSessionId::new("tcp-v3-owner-session")?;
    let seed_revision = seed_command_session(&mut client, &session_id, "tcp-v3-owner-seed")?;
    wait_for_revision(&mut cluster.node_two, seed_revision)?;
    wait_for_revision(&mut cluster.node_three, seed_revision)?;

    let owner_a = SessionOwnerInstanceId::new("tcp-v3-owner-a")?;
    let epoch_two = client.acquire_command_session_owner(
        session_id.clone(),
        SessionOwnerEpoch::INITIAL,
        owner_a.clone(),
    )?;
    assert_eq!(epoch_two.get(), 2);
    let acquisition_retry = client.acquire_command_session_owner(
        session_id.clone(),
        SessionOwnerEpoch::INITIAL,
        owner_a.clone(),
    )?;
    assert_eq!(acquisition_retry, epoch_two);

    let owner_b = SessionOwnerInstanceId::new("tcp-v3-owner-b")?;
    let stale_acquisition = client.acquire_command_session_owner(
        session_id.clone(),
        SessionOwnerEpoch::INITIAL,
        owner_b.clone(),
    );
    assert_broker_error(stale_acquisition, BrokerErrorCode::StaleFence)?;

    let owned_identity =
        CommandIdentity::new_with_owner(session_id, epoch_two, owner_a, CommandSequence::new(1)?);
    let owned_request = BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new("tcp-v3-owned-mutation")?,
        namespace_id: NamespaceId::new("tcp-v3-owned-namespace")?,
    });
    let first = client.execute_owned(&owned_identity, &owned_request)?;
    let after_first = client.health()?;
    assert_eq!(after_first.revision.get(), seed_revision + 1);
    let exact_retry = client.execute_owned(&owned_identity, &owned_request)?;
    assert_eq!(exact_retry, first);
    assert_eq!(client.health()?.revision, after_first.revision);

    let forged_identity = CommandIdentity::new_with_owner(
        owned_identity.session_id().clone(),
        epoch_two,
        owner_b,
        CommandSequence::new(1)?,
    );
    let forged_request = BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new("tcp-v3-forged-owner")?,
        namespace_id: NamespaceId::new("tcp-v3-forged-owner")?,
    });
    assert_broker_error(
        client.execute_owned(&forged_identity, &forged_request),
        BrokerErrorCode::StaleFence,
    )?;
    assert_eq!(client.health()?.revision, after_first.revision);
    wait_for_revision(&mut cluster.node_two, after_first.revision.get())?;
    wait_for_revision(&mut cluster.node_three, after_first.revision.get())?;
    client.close();
    cluster.shutdown()
}

#[test]
fn cluster_tcp_v3_lost_acquisition_response_recovers_same_owner_epoch() -> Result<(), Box<dyn Error>>
{
    let mut cluster = RunningTcpCluster::start()?;
    let mut client = cluster.client()?;
    let session_id = CommandSessionId::new("tcp-v3-lost-acquisition")?;
    let seed_revision =
        seed_command_session(&mut client, &session_id, "tcp-v3-lost-acquisition-seed")?;
    wait_for_revision(&mut cluster.node_two, seed_revision)?;
    wait_for_revision(&mut cluster.node_three, seed_revision)?;
    let owner_instance = SessionOwnerInstanceId::new("tcp-v3-lost-owner-a")?;

    let acquisition = OwnerAcquisitionRequestV3::new(
        RequestId::new("tcp-v3-lost-acquisition-request")?,
        session_id.clone(),
        SessionOwnerEpoch::INITIAL,
        owner_instance.clone(),
    );
    let frame = encode_owner_acquisition_request_with_limit(&acquisition, MAX_FRAME_BYTES)?;
    drop_response_after_backend_completion(cluster.client_address, &frame)?;

    let owned_identity = CommandIdentity::new_with_owner(
        session_id.clone(),
        SessionOwnerEpoch::new(2)?,
        owner_instance.clone(),
        CommandSequence::new(1)?,
    );
    let owned_request = BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new("tcp-v3-lost-acquisition-proof")?,
        namespace_id: NamespaceId::new("tcp-v3-lost-acquisition-proof")?,
    });
    let first = client.execute_owned(&owned_identity, &owned_request)?;
    let after_owned = client.health()?;
    assert_eq!(after_owned.revision.get(), seed_revision + 1);

    let recovered_epoch = client.acquire_command_session_owner(
        session_id,
        SessionOwnerEpoch::INITIAL,
        owner_instance,
    )?;
    assert_eq!(recovered_epoch.get(), 2);
    assert_eq!(client.health()?.revision, after_owned.revision);
    let exact_retry = client.execute_owned(&owned_identity, &owned_request)?;
    assert_eq!(exact_retry, first);
    assert_eq!(client.health()?.revision, after_owned.revision);
    wait_for_revision(&mut cluster.node_two, after_owned.revision.get())?;
    wait_for_revision(&mut cluster.node_three, after_owned.revision.get())?;
    client.close();
    cluster.shutdown()
}

#[test]
fn durable_client_session_recovers_committed_inflight_after_hard_process_kill()
-> Result<(), Box<dyn Error>> {
    let mut cluster = RunningTcpCluster::start()?;
    let mut observer = cluster.client()?;
    let revision_before = observer.health()?.revision;
    let directory = tempdir()?;
    let state_path = directory.path().join("durable-client-session.json");
    let proxy_marker = directory.path().join("backend-response-ready.txt");
    let (proxy_address, proxy_thread) =
        spawn_response_hold_proxy(cluster.client_address, proxy_marker.clone())?;

    let mut child = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("durable_client_session_process_child")
        .arg("--nocapture")
        .env(DURABLE_CLIENT_CHILD_ENV, "1")
        .env(
            DURABLE_CLIENT_BACKEND_ENV,
            cluster.client_address.to_string(),
        )
        .env(DURABLE_CLIENT_PROXY_ENV, proxy_address.to_string())
        .env(DURABLE_CLIENT_STATE_ENV, &state_path)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;

    if let Err(error) = wait_for_child_marker(&mut child, &proxy_marker, CLUSTER_WAIT) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }
    child.kill()?;
    let _status = child.wait()?;
    match proxy_thread.join() {
        Ok(result) => result?,
        Err(_) => return Err("durable-client response-hold proxy panicked".into()),
    }

    let committed_revision = revision_before.get() + 1;
    wait_for_revision(&mut cluster.node_two, committed_revision)?;
    wait_for_revision(&mut cluster.node_three, committed_revision)?;
    assert_eq!(observer.health()?.revision.get(), committed_revision);

    let session_id = CommandSessionId::new("durable-process-session")?;
    let owner_a = SessionOwnerInstanceId::new("durable-process-owner-a")?;
    let owner_b = SessionOwnerInstanceId::new("durable-process-owner-b")?;
    let mut store = DurableClientSessionStore::open_or_create(&state_path, session_id)?;
    let owner = store
        .owner()?
        .ok_or("durable owner missing after child crash")?;
    assert_eq!(owner.owner_epoch(), SessionOwnerEpoch::INITIAL);
    assert_eq!(owner.owner_instance_id(), &owner_a);
    let reserved = store
        .in_flight()?
        .ok_or("durable in-flight command missing after child crash")?;
    assert!(matches!(
        store.begin_owner_acquisition(owner_b.clone()),
        Err(ClientSessionStoreError::OperationBlocked(_))
    ));

    let mut recovery_client = cluster.client()?;
    let recovered = recovery_client.execute_owned(reserved.identity(), reserved.request())?;
    assert_eq!(recovery_client.health()?.revision.get(), committed_revision);
    let exact_retry = recovery_client.execute_owned(reserved.identity(), reserved.request())?;
    assert_eq!(exact_retry, recovered);
    assert_eq!(recovery_client.health()?.revision.get(), committed_revision);
    store.acknowledge_in_flight_outcome(reserved.identity())?;
    assert!(store.in_flight()?.is_none());

    let takeover = store.begin_owner_acquisition(owner_b.clone())?;
    assert_eq!(takeover.expected_owner_epoch(), SessionOwnerEpoch::INITIAL);
    let epoch_two = recovery_client.acquire_command_session_owner(
        takeover.session_id().clone(),
        takeover.expected_owner_epoch(),
        takeover.owner_instance_id().clone(),
    )?;
    assert_eq!(epoch_two.get(), 2);
    let confirmed_owner = store.confirm_owner_acquisition(epoch_two)?;
    assert_eq!(confirmed_owner.owner_instance_id(), &owner_b);

    let second = store.reserve_command(BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new("durable-process-after-takeover")?,
        namespace_id: NamespaceId::new("durable-process-after-takeover")?,
    }))?;
    assert_eq!(second.identity().sequence().get(), 1);
    let _second_result = recovery_client.execute_owned(second.identity(), second.request())?;
    store.acknowledge_in_flight_outcome(second.identity())?;
    let final_revision = committed_revision + 1;
    assert_eq!(recovery_client.health()?.revision.get(), final_revision);
    wait_for_revision(&mut cluster.node_two, final_revision)?;
    wait_for_revision(&mut cluster.node_three, final_revision)?;
    recovery_client.close();
    observer.close();
    drop(store);
    cluster.shutdown()
}

#[test]
fn protocol_v3_distinguishes_committed_business_error_from_rejected_identity_error()
-> Result<(), Box<dyn Error>> {
    let mut cluster = RunningTcpCluster::start()?;
    let mut client = cluster.client()?;
    let revision_before = client.health()?.revision;
    let session_id = CommandSessionId::new("tcp-v3-disposition-session")?;
    let owner_a = SessionOwnerInstanceId::new("tcp-v3-disposition-owner-a")?;
    let owner_b = SessionOwnerInstanceId::new("tcp-v3-disposition-owner-b")?;
    let owner_epoch = client.acquire_command_session_owner(
        session_id.clone(),
        SessionOwnerEpoch::INITIAL,
        owner_a.clone(),
    )?;
    assert_eq!(owner_epoch, SessionOwnerEpoch::INITIAL);

    let first_identity = CommandIdentity::new_with_owner(
        session_id.clone(),
        owner_epoch,
        owner_a.clone(),
        CommandSequence::new(1)?,
    );
    let missing_namespace_publish = BrokerRequest::PublishTask(PublishTaskRequest {
        request_id: RequestId::new("tcp-v3-disposition-not-found")?,
        namespace_id: NamespaceId::new("tcp-v3-disposition-missing")?,
        task_id: TaskId::new("tcp-v3-disposition-task")?,
        objective: TaskObjective::new("prove committed business error disposition")?,
    });
    let committed_error = match client.execute_owned(&first_identity, &missing_namespace_publish) {
        Err(ClientError::Broker(error)) => error,
        other => return Err(format!("expected committed Broker error, got {other:?}").into()),
    };
    assert_eq!(committed_error.code(), BrokerErrorCode::NotFound);
    assert_eq!(
        committed_error.disposition(),
        BrokerErrorDisposition::Committed
    );
    assert_eq!(client.health()?.revision, revision_before);

    let exact_retry_error = match client.execute_owned(&first_identity, &missing_namespace_publish)
    {
        Err(ClientError::Broker(error)) => error,
        other => return Err(format!("expected committed retry error, got {other:?}").into()),
    };
    assert_eq!(exact_retry_error, committed_error);
    assert_eq!(client.health()?.revision, revision_before);

    let conflicting_request = BrokerRequest::PublishTask(PublishTaskRequest {
        request_id: RequestId::new("tcp-v3-disposition-conflict")?,
        namespace_id: NamespaceId::new("tcp-v3-disposition-missing")?,
        task_id: TaskId::new("tcp-v3-disposition-other-task")?,
        objective: TaskObjective::new("same sequence different command")?,
    });
    let rejected_conflict = match client.execute_owned(&first_identity, &conflicting_request) {
        Err(ClientError::Broker(error)) => error,
        other => return Err(format!("expected rejected sequence conflict, got {other:?}").into()),
    };
    assert_eq!(rejected_conflict.code(), BrokerErrorCode::Conflict);
    assert_eq!(
        rejected_conflict.disposition(),
        BrokerErrorDisposition::Rejected
    );

    let forged_identity = CommandIdentity::new_with_owner(
        session_id.clone(),
        owner_epoch,
        owner_b,
        CommandSequence::new(1)?,
    );
    let rejected_owner = match client.execute_owned(&forged_identity, &missing_namespace_publish) {
        Err(ClientError::Broker(error)) => error,
        other => return Err(format!("expected rejected owner fence, got {other:?}").into()),
    };
    assert_eq!(rejected_owner.code(), BrokerErrorCode::StaleFence);
    assert_eq!(
        rejected_owner.disposition(),
        BrokerErrorDisposition::Rejected
    );

    let second_identity =
        CommandIdentity::new_with_owner(session_id, owner_epoch, owner_a, CommandSequence::new(2)?);
    let second_request = BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new("tcp-v3-disposition-sequence-two")?,
        namespace_id: NamespaceId::new("tcp-v3-disposition-sequence-two")?,
    });
    let _second = client.execute_owned(&second_identity, &second_request)?;
    let final_revision = revision_before.get() + 1;
    assert_eq!(client.health()?.revision.get(), final_revision);
    wait_for_revision(&mut cluster.node_two, final_revision)?;
    wait_for_revision(&mut cluster.node_three, final_revision)?;
    client.close();
    cluster.shutdown()
}

#[test]
fn durable_retry_policy_handles_response_loss_committed_error_and_rejection()
-> Result<(), Box<dyn Error>> {
    let cluster = RunningTcpCluster::start()?;
    let mut backend_client = cluster.client()?;
    let directory = tempdir()?;
    let state_path = directory.path().join("durable-retry-session.json");
    let session_id = CommandSessionId::new("durable-retry-session")?;
    let owner_a = SessionOwnerInstanceId::new("durable-retry-owner-a")?;
    let owner_b = SessionOwnerInstanceId::new("durable-retry-owner-b")?;
    let mut store = DurableClientSessionStore::open_or_create(&state_path, session_id.clone())?;
    let pending = store.begin_owner_acquisition(owner_a.clone())?;
    let owner_epoch = backend_client.acquire_command_session_owner(
        pending.session_id().clone(),
        pending.expected_owner_epoch(),
        pending.owner_instance_id().clone(),
    )?;
    assert_eq!(owner_epoch, SessionOwnerEpoch::INITIAL);
    store.confirm_owner_acquisition(owner_epoch)?;

    let revision_before = backend_client.health()?.revision;
    let (proxy_address, proxy_thread) = spawn_drop_first_response_proxy(cluster.client_address)?;
    let mut retry_client = BrokerClient::new(BrokerClientConfig {
        address: proxy_address,
        timeout: CLIENT_TIMEOUT,
        max_response_frame_bytes: MAX_FRAME_BYTES,
    })?;
    let retry_policy = DurableRetryPolicy::new(
        NonZeroUsize::new(2).ok_or("durable retry attempts must be non-zero")?,
    );
    let response_lost_request = BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new("durable-auto-retry-response-loss")?,
        namespace_id: NamespaceId::new("durable-auto-retry-response-loss")?,
    });
    let _success = retry_client.execute_durable(&mut store, response_lost_request, retry_policy)?;
    match proxy_thread.join() {
        Ok(result) => result?,
        Err(_) => return Err("drop-first-response proxy panicked".into()),
    }
    assert!(store.in_flight()?.is_none());
    assert_eq!(store.next_sequence()?.get(), 2);
    assert_eq!(
        backend_client.health()?.revision.get(),
        revision_before.get() + 1
    );
    verify_durable_committed_and_rejected_outcomes(
        &mut backend_client,
        &mut store,
        session_id,
        owner_b,
    )?;

    retry_client.close();
    backend_client.close();
    drop(store);
    cluster.shutdown()
}

fn verify_durable_committed_and_rejected_outcomes(
    client: &mut BrokerClient,
    store: &mut DurableClientSessionStore,
    session_id: CommandSessionId,
    replacement_owner: SessionOwnerInstanceId,
) -> Result<(), Box<dyn Error>> {
    let committed_error_request = BrokerRequest::PublishTask(PublishTaskRequest {
        request_id: RequestId::new("durable-auto-retry-committed-error")?,
        namespace_id: NamespaceId::new("durable-auto-retry-missing")?,
        task_id: TaskId::new("durable-auto-retry-missing-task")?,
        objective: TaskObjective::new("committed error consumes sequence")?,
    });
    let committed_error = match client.execute_durable(
        store,
        committed_error_request,
        DurableRetryPolicy::new(NonZeroUsize::MIN),
    ) {
        Err(DurableExecutionError::Client(ClientError::Broker(error))) => error,
        other => {
            return Err(format!("expected committed durable Broker error, got {other:?}").into());
        }
    };
    assert_eq!(committed_error.code(), BrokerErrorCode::NotFound);
    assert_eq!(
        committed_error.disposition(),
        BrokerErrorDisposition::Committed
    );
    assert!(store.in_flight()?.is_none());
    assert_eq!(store.next_sequence()?.get(), 3);

    let _sequence_three = client.execute_durable(
        store,
        BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
            request_id: RequestId::new("durable-auto-retry-sequence-three")?,
            namespace_id: NamespaceId::new("durable-auto-retry-sequence-three")?,
        }),
        DurableRetryPolicy::new(NonZeroUsize::MIN),
    )?;
    assert_eq!(store.next_sequence()?.get(), 4);

    let epoch_two = client.acquire_command_session_owner(
        session_id,
        SessionOwnerEpoch::INITIAL,
        replacement_owner,
    )?;
    assert_eq!(epoch_two.get(), 2);
    let rejected = match client.execute_durable(
        store,
        BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
            request_id: RequestId::new("durable-auto-retry-rejected-owner")?,
            namespace_id: NamespaceId::new("durable-auto-retry-rejected-owner")?,
        }),
        DurableRetryPolicy::new(NonZeroUsize::MIN),
    ) {
        Err(DurableExecutionError::Client(ClientError::Broker(error))) => error,
        other => {
            return Err(format!("expected rejected durable Broker error, got {other:?}").into());
        }
    };
    assert_eq!(rejected.code(), BrokerErrorCode::StaleFence);
    assert_eq!(rejected.disposition(), BrokerErrorDisposition::Rejected);
    assert!(store.in_flight()?.is_none());
    assert_eq!(store.next_sequence()?.get(), 4);
    Ok(())
}

#[test]
fn durable_retry_exhaustion_preserves_exact_inflight_for_later_recovery()
-> Result<(), Box<dyn Error>> {
    let mut cluster = RunningTcpCluster::start()?;
    let mut backend_client = cluster.client()?;
    let directory = tempdir()?;
    let state_path = directory.path().join("durable-retry-exhaustion.json");
    let session_id = CommandSessionId::new("durable-retry-exhaustion")?;
    let owner_instance = SessionOwnerInstanceId::new("durable-retry-exhaustion-owner")?;
    let mut store = DurableClientSessionStore::open_or_create(&state_path, session_id)?;
    let pending = store.begin_owner_acquisition(owner_instance)?;
    let owner_epoch = backend_client.acquire_command_session_owner(
        pending.session_id().clone(),
        pending.expected_owner_epoch(),
        pending.owner_instance_id().clone(),
    )?;
    assert_eq!(owner_epoch, SessionOwnerEpoch::INITIAL);
    store.confirm_owner_acquisition(owner_epoch)?;

    let revision_before = backend_client.health()?.revision;
    let request = BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new("durable-retry-exhaustion-request")?,
        namespace_id: NamespaceId::new("durable-retry-exhaustion")?,
    });
    let (proxy_address, proxy_thread) = spawn_drop_all_responses_proxy(cluster.client_address, 2)?;
    let mut retry_client = BrokerClient::new(BrokerClientConfig {
        address: proxy_address,
        timeout: CLIENT_TIMEOUT,
        max_response_frame_bytes: MAX_FRAME_BYTES,
    })?;
    let exhausted = retry_client.execute_durable(
        &mut store,
        request,
        DurableRetryPolicy::new(
            NonZeroUsize::new(2).ok_or("durable retry attempts must be non-zero")?,
        ),
    );
    assert!(matches!(
        exhausted,
        Err(DurableExecutionError::Client(ClientError::Transport(_)))
    ));
    match proxy_thread.join() {
        Ok(result) => result?,
        Err(_) => return Err("drop-all-responses proxy panicked".into()),
    }

    let in_flight = store
        .in_flight()?
        .ok_or("retry exhaustion must preserve exact in-flight command")?;
    assert_eq!(in_flight.identity().sequence().get(), 1);
    assert_eq!(store.next_sequence()?.get(), 1);
    let committed_revision = revision_before.get() + 1;
    wait_for_revision(&mut cluster.node_two, committed_revision)?;
    wait_for_revision(&mut cluster.node_three, committed_revision)?;
    assert_eq!(backend_client.health()?.revision.get(), committed_revision);

    let recovered = backend_client
        .recover_durable_in_flight(&mut store, DurableRetryPolicy::new(NonZeroUsize::MIN))?;
    assert!(matches!(
        recovered,
        agent_broker_protocol::SuccessPayload::Namespace { .. }
    ));
    assert!(store.in_flight()?.is_none());
    assert_eq!(store.next_sequence()?.get(), 2);
    assert_eq!(backend_client.health()?.revision.get(), committed_revision);
    retry_client.close();
    backend_client.close();
    drop(store);
    cluster.shutdown()
}

#[test]
fn durable_client_session_process_child() -> Result<(), Box<dyn Error>> {
    if std::env::var_os(DURABLE_CLIENT_CHILD_ENV).is_none() {
        return Ok(());
    }
    let backend_address = std::env::var(DURABLE_CLIENT_BACKEND_ENV)?.parse::<SocketAddr>()?;
    let proxy_address = std::env::var(DURABLE_CLIENT_PROXY_ENV)?.parse::<SocketAddr>()?;
    let state_path = std::env::var_os(DURABLE_CLIENT_STATE_ENV)
        .ok_or("durable client child state path is missing")?;
    let session_id = CommandSessionId::new("durable-process-session")?;
    let owner_instance = SessionOwnerInstanceId::new("durable-process-owner-a")?;
    let mut store = DurableClientSessionStore::open_or_create(&state_path, session_id)?;
    let pending = store.begin_owner_acquisition(owner_instance)?;
    let mut backend = BrokerClient::new(BrokerClientConfig {
        address: backend_address,
        timeout: CLIENT_TIMEOUT,
        max_response_frame_bytes: MAX_FRAME_BYTES,
    })?;
    let owner_epoch = backend.acquire_command_session_owner(
        pending.session_id().clone(),
        pending.expected_owner_epoch(),
        pending.owner_instance_id().clone(),
    )?;
    assert_eq!(owner_epoch, SessionOwnerEpoch::INITIAL);
    store.confirm_owner_acquisition(owner_epoch)?;
    let reserved =
        store.reserve_command(BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
            request_id: RequestId::new("durable-process-before-crash")?,
            namespace_id: NamespaceId::new("durable-process-before-crash")?,
        }))?;
    backend.close();

    let mut response_lost_client = BrokerClient::new(BrokerClientConfig {
        address: proxy_address,
        timeout: CLUSTER_WAIT,
        max_response_frame_bytes: MAX_FRAME_BYTES,
    })?;
    let result = response_lost_client.execute_owned(reserved.identity(), reserved.request());
    Err(
        format!("durable client child should have been killed while awaiting response: {result:?}")
            .into(),
    )
}

fn verify_exact_identified_retry_and_conflict(
    client: &mut BrokerClient,
) -> Result<u64, Box<dyn Error>> {
    let before = client.health()?;
    let identity = CommandIdentity::new(
        CommandSessionId::new("tcp-cluster-client")?,
        CommandSequence::new(1)?,
    );
    let request = BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new("identified-tcp-1")?,
        namespace_id: NamespaceId::new("identified-tcp")?,
    });
    let first = client.execute_identified(&identity, &request)?;
    let after_first = client.health()?;
    assert_eq!(after_first.revision.get(), before.revision.get() + 1);

    let retry = client.execute_identified(&identity, &request)?;
    assert_eq!(retry, first);
    assert_eq!(client.health()?.revision, after_first.revision);

    let conflicting_request = BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new("identified-tcp-conflict")?,
        namespace_id: NamespaceId::new("identified-tcp-conflict")?,
    });
    let conflict_error = match client.execute_identified(&identity, &conflicting_request) {
        Err(ClientError::Broker(error)) => error,
        other => return Err(format!("expected v2 conflict response, got {other:?}").into()),
    };
    assert_eq!(conflict_error.code(), BrokerErrorCode::Conflict);
    assert_eq!(client.health()?.revision, after_first.revision);
    Ok(after_first.revision.get())
}

fn verify_lost_response_recovery(
    client_address: SocketAddr,
    observer: &mut BrokerClient,
    node_two: &mut ClusterRaftConsensusAdapter,
    node_three: &mut ClusterRaftConsensusAdapter,
) -> Result<(), Box<dyn Error>> {
    let before = observer.health()?;
    let identity = CommandIdentity::new(
        CommandSessionId::new("tcp-lost-response-client")?,
        CommandSequence::new(1)?,
    );
    let request = BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new("identified-tcp-lost-response")?,
        namespace_id: NamespaceId::new("identified-tcp-lost-response")?,
    });
    let wire = IdentifiedBrokerRequest::new(identity.clone(), request.clone())?;
    let frame = encode_identified_request(&wire)?;
    let mut abandoned = TcpStream::connect(client_address)?;
    abandoned.set_write_timeout(Some(CLIENT_TIMEOUT))?;
    abandoned.write_all(&frame)?;
    abandoned.flush()?;
    abandoned.shutdown(Shutdown::Both)?;
    drop(abandoned);

    let committed_revision = before.revision.get() + 1;
    wait_for_revision(node_two, committed_revision)?;
    wait_for_revision(node_three, committed_revision)?;
    let committed_health = observer.health()?;
    assert_eq!(committed_health.revision.get(), committed_revision);

    let mut recovery_client = BrokerClient::new(BrokerClientConfig {
        address: client_address,
        timeout: CLIENT_TIMEOUT,
        max_response_frame_bytes: MAX_FRAME_BYTES,
    })?;
    let recovered = recovery_client.execute_identified(&identity, &request)?;
    assert_eq!(
        recovery_client.health()?.revision,
        committed_health.revision
    );
    let exact_retry = recovery_client.execute_identified(&identity, &request)?;
    assert_eq!(exact_retry, recovered);
    assert_eq!(
        recovery_client.health()?.revision,
        committed_health.revision
    );
    recovery_client.close();
    Ok(())
}

fn seed_command_session(
    client: &mut BrokerClient,
    session_id: &CommandSessionId,
    namespace_id: &str,
) -> Result<u64, Box<dyn Error>> {
    let before = client.health()?;
    let identity = CommandIdentity::new(session_id.clone(), CommandSequence::new(1)?);
    let request = BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new(format!("seed-{namespace_id}"))?,
        namespace_id: NamespaceId::new(namespace_id)?,
    });
    let _seeded = client.execute_identified(&identity, &request)?;
    let after = client.health()?;
    assert_eq!(after.revision.get(), before.revision.get() + 1);
    Ok(after.revision.get())
}

fn spawn_response_hold_proxy(
    backend_address: SocketAddr,
    marker_path: PathBuf,
) -> Result<(SocketAddr, ProxyThread), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let proxy_address = listener.local_addr()?;
    let proxy = thread::Builder::new()
        .name("agent-broker-hold-client-response-proxy".to_owned())
        .spawn(move || -> std::io::Result<()> {
            let (client, _peer) = listener.accept()?;
            client.set_read_timeout(Some(CLUSTER_WAIT))?;
            client.set_write_timeout(Some(CLIENT_TIMEOUT))?;
            let mut client_reader = BufReader::new(client);
            let mut request = Vec::new();
            client_reader.read_until(b'\n', &mut request)?;
            if request.last() != Some(&b'\n') {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "proxy did not receive a complete durable-client request frame",
                ));
            }

            let backend = TcpStream::connect(backend_address)?;
            backend.set_read_timeout(Some(CLIENT_TIMEOUT))?;
            backend.set_write_timeout(Some(CLIENT_TIMEOUT))?;
            let mut backend_reader = BufReader::new(backend);
            backend_reader.get_mut().write_all(&request)?;
            backend_reader.get_mut().flush()?;
            let mut response = Vec::new();
            backend_reader.read_until(b'\n', &mut response)?;
            if response.last() != Some(&b'\n') {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "backend did not produce a complete durable-client response frame",
                ));
            }

            let mut marker = std::fs::File::create(marker_path)?;
            marker.write_all(b"backend-response-complete\n")?;
            marker.flush()?;
            marker.sync_all()?;

            let mut probe = [0_u8; 1];
            match client_reader.read(&mut probe) {
                Ok(0) => Ok(()),
                Ok(_) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "durable-client child wrote unexpected bytes while awaiting response",
                )),
                Err(error) => Err(error),
            }
        })?;
    Ok((proxy_address, proxy))
}

fn spawn_drop_first_response_proxy(
    backend_address: SocketAddr,
) -> Result<(SocketAddr, ProxyThread), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let proxy_address = listener.local_addr()?;
    let proxy = thread::Builder::new()
        .name("agent-broker-drop-first-response-proxy".to_owned())
        .spawn(move || -> std::io::Result<()> {
            for attempt in 0..2 {
                let (client, _peer) = listener.accept()?;
                client.set_read_timeout(Some(CLIENT_TIMEOUT))?;
                client.set_write_timeout(Some(CLIENT_TIMEOUT))?;
                let mut client_reader = BufReader::new(client);
                let mut request = Vec::new();
                client_reader.read_until(b'\n', &mut request)?;
                if request.last() != Some(&b'\n') {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "retry proxy did not receive a complete request frame",
                    ));
                }

                let backend = TcpStream::connect(backend_address)?;
                backend.set_read_timeout(Some(CLIENT_TIMEOUT))?;
                backend.set_write_timeout(Some(CLIENT_TIMEOUT))?;
                let mut backend_reader = BufReader::new(backend);
                backend_reader.get_mut().write_all(&request)?;
                backend_reader.get_mut().flush()?;
                let mut response = Vec::new();
                backend_reader.read_until(b'\n', &mut response)?;
                if response.last() != Some(&b'\n') {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "backend did not produce a complete retry response frame",
                    ));
                }

                if attempt == 0 {
                    client_reader.get_mut().shutdown(Shutdown::Both)?;
                } else {
                    client_reader.get_mut().write_all(&response)?;
                    client_reader.get_mut().flush()?;
                }
            }
            Ok(())
        })?;
    Ok((proxy_address, proxy))
}

fn spawn_drop_all_responses_proxy(
    backend_address: SocketAddr,
    attempts: usize,
) -> Result<(SocketAddr, ProxyThread), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let proxy_address = listener.local_addr()?;
    let proxy = thread::Builder::new()
        .name("agent-broker-drop-all-responses-proxy".to_owned())
        .spawn(move || -> std::io::Result<()> {
            for _ in 0..attempts {
                let (client, _peer) = listener.accept()?;
                client.set_read_timeout(Some(CLIENT_TIMEOUT))?;
                client.set_write_timeout(Some(CLIENT_TIMEOUT))?;
                let mut client_reader = BufReader::new(client);
                let mut request = Vec::new();
                client_reader.read_until(b'\n', &mut request)?;
                if request.last() != Some(&b'\n') {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "retry exhaustion proxy did not receive a complete request frame",
                    ));
                }

                let backend = TcpStream::connect(backend_address)?;
                backend.set_read_timeout(Some(CLIENT_TIMEOUT))?;
                backend.set_write_timeout(Some(CLIENT_TIMEOUT))?;
                let mut backend_reader = BufReader::new(backend);
                backend_reader.get_mut().write_all(&request)?;
                backend_reader.get_mut().flush()?;
                let mut response = Vec::new();
                backend_reader.read_until(b'\n', &mut response)?;
                if response.last() != Some(&b'\n') {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "backend did not produce a complete retry exhaustion response frame",
                    ));
                }
                client_reader.get_mut().shutdown(Shutdown::Both)?;
            }
            Ok(())
        })?;
    Ok((proxy_address, proxy))
}

fn wait_for_child_marker(
    child: &mut Child,
    marker_path: &Path,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        if marker_path.try_exists()? {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            return Err(format!(
                "durable-client child exited before backend response barrier: {status}"
            )
            .into());
        }
        if Instant::now() >= deadline {
            return Err("durable-client child did not reach backend response barrier".into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn drop_response_after_backend_completion(
    backend_address: SocketAddr,
    frame: &[u8],
) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let proxy_address = listener.local_addr()?;
    let proxy = thread::Builder::new()
        .name("agent-broker-drop-client-response-proxy".to_owned())
        .spawn(move || -> std::io::Result<()> {
            let (client, _peer) = listener.accept()?;
            client.set_read_timeout(Some(CLIENT_TIMEOUT))?;
            client.set_write_timeout(Some(CLIENT_TIMEOUT))?;
            let mut client_reader = BufReader::new(client);
            let mut request = Vec::new();
            client_reader.read_until(b'\n', &mut request)?;
            if request.last() != Some(&b'\n') {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "proxy did not receive a complete Broker request frame",
                ));
            }

            let backend = TcpStream::connect(backend_address)?;
            backend.set_read_timeout(Some(CLIENT_TIMEOUT))?;
            backend.set_write_timeout(Some(CLIENT_TIMEOUT))?;
            let mut backend_reader = BufReader::new(backend);
            backend_reader.get_mut().write_all(&request)?;
            backend_reader.get_mut().flush()?;
            let mut response = Vec::new();
            backend_reader.read_until(b'\n', &mut response)?;
            if response.last() != Some(&b'\n') {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "backend did not produce a complete Broker response frame",
                ));
            }
            client_reader.get_mut().shutdown(Shutdown::Both)
        })?;

    let mut simulated_client = TcpStream::connect(proxy_address)?;
    simulated_client.set_write_timeout(Some(CLIENT_TIMEOUT))?;
    simulated_client.write_all(frame)?;
    simulated_client.flush()?;
    match proxy.join() {
        Ok(result) => result?,
        Err(_) => return Err("response-drop proxy thread panicked".into()),
    }
    drop(simulated_client);
    Ok(())
}

fn assert_broker_error<T: std::fmt::Debug>(
    result: Result<T, ClientError>,
    expected: BrokerErrorCode,
) -> Result<(), Box<dyn Error>> {
    match result {
        Err(ClientError::Broker(error)) => {
            assert_eq!(error.code(), expected);
            Ok(())
        }
        other => Err(format!("expected Broker error {expected:?}, got {other:?}").into()),
    }
}

fn reserve_three_ports() -> Result<(Vec<TcpListener>, [SocketAddr; 3]), Box<dyn Error>> {
    let mut listeners = Vec::with_capacity(3);
    let mut addresses = Vec::with_capacity(3);
    for _ in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        addresses.push(listener.local_addr()?);
        listeners.push(listener);
    }
    let addresses: [SocketAddr; 3] = addresses
        .try_into()
        .map_err(|_| "failed to reserve exactly three Raft ports")?;
    Ok((listeners, addresses))
}

fn wait_for_cluster(
    node: &mut ClusterRaftConsensusAdapter,
    voters: &BTreeSet<u64>,
    leader: u64,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    loop {
        let progress = node.progress()?;
        if progress.voters == *voters && progress.current_leader == Some(leader) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "cluster did not converge for node {}: {progress:?}",
                progress.node_id
            )
            .into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_revision(
    node: &mut ClusterRaftConsensusAdapter,
    revision: u64,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    loop {
        let progress = node.progress()?;
        if progress.broker_revision.get() >= revision
            && progress.applied_index == progress.committed_index
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("node did not reach Broker revision {revision}: {progress:?}").into(),
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_state_owner_drain(owner: &StateOwnerHandle) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    loop {
        let load = owner.load();
        if load.active_jobs == 0 && load.queued_jobs == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("state owner did not drain after connection churn: {load:?}").into(),
            );
        }
        thread::yield_now();
    }
}

fn wait_for_state_owner_load(
    owner: &StateOwnerHandle,
    active_jobs: usize,
    queued_jobs: usize,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    loop {
        let load = owner.load();
        if load.active_jobs == active_jobs && load.queued_jobs == queued_jobs {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "state owner did not reach active={active_jobs} queued={queued_jobs}: {load:?}"
            )
            .into());
        }
        thread::yield_now();
    }
}

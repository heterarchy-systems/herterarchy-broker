use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use agent_broker_application::{
    BrokerErrorCode, CommandIdentity, CommandSequence, CommandSessionId, ConsensusAdapter,
    SessionOwnerEpoch, SessionOwnerInstanceId,
};
use agent_broker_consensus::{
    ClusterRaftConfig, ClusterRaftConsensusAdapter, ClusterRaftReadinessStatus,
    ClusterRaftTlsConfig,
};
use agent_broker_domain::NamespaceId;
use agent_broker_domain::commands::{BrokerCommand, EnsureNamespaceCommand};
use agent_broker_domain::results::BrokerMutationResult;
use redb::{Database, TableDefinition};
use tempfile::tempdir;

#[path = "../test_support/tls_fixture.rs"]
mod tls_fixture;

const CLUSTER_WAIT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(20);
const SUPPRESSION_PROXY_POLL_INTERVAL: Duration = Duration::from_millis(1);
const OPAQUE_PROXY_BURST_GAP: Duration = Duration::from_millis(20);
const OPAQUE_SNAPSHOT_CUT_AFTER_CLIENT_BYTES: usize = 8 * 1024;
const PROXY_IO_TIMEOUT: Duration = Duration::from_secs(2);
const IDENTIFIED_TIMEOUT_TEST_DEADLINE: Duration = Duration::from_millis(100);
const IDENTIFIED_TIMEOUT_TEST_ELECTION_MIN: Duration = Duration::from_secs(3);
const IDENTIFIED_TIMEOUT_TEST_ELECTION_MAX: Duration = Duration::from_secs(4);
const IDENTIFIED_TIMEOUT_TEST_HEARTBEAT: Duration = Duration::from_secs(1);
const TEST_RAFT_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta_v1");

fn validation_tls() -> Result<ClusterRaftTlsConfig, Box<dyn Error>> {
    Ok(ClusterRaftTlsConfig::new("unused-cluster-tls")?)
}

#[test]
fn snapshot_catch_up_policy_rejects_invalid_threshold_relationships() -> Result<(), Box<dyn Error>>
{
    let nodes = BTreeMap::from([
        (1, "127.0.0.1:18001".to_owned()),
        (2, "127.0.0.1:18002".to_owned()),
        (3, "127.0.0.1:18003".to_owned()),
    ]);
    let base = ClusterRaftConfig::new(
        1,
        "unused-snapshot-policy-validation.redb",
        "127.0.0.1:18001".parse()?,
        nodes,
        validation_tls()?,
        true,
    )?;

    let zero_interval = base.clone().with_snapshot_catch_up_policy(0, 16, 2);
    assert!(zero_interval.is_err());

    let equal_threshold = base.clone().with_snapshot_catch_up_policy(8, 8, 2);
    assert!(equal_threshold.is_err());

    let lower_threshold = base.with_snapshot_catch_up_policy(8, 7, 2);
    assert!(lower_threshold.is_err());

    let hostname_nodes = BTreeMap::from([
        (1, "raft-node-1:18001".to_owned()),
        (2, "127.0.0.1:18002".to_owned()),
        (3, "127.0.0.1:18003".to_owned()),
    ]);
    let hostname = ClusterRaftConfig::new(
        1,
        "unused-hostname-validation.redb",
        "127.0.0.1:18001".parse()?,
        hostname_nodes,
        validation_tls()?,
        true,
    );
    let Err(hostname_error) = hostname else {
        return Err("cluster config unexpectedly accepted a DNS hostname peer".into());
    };
    assert_eq!(hostname_error.code(), BrokerErrorCode::InvalidRequest);

    let timeout_nodes = BTreeMap::from([
        (1, "127.0.0.1:18001".to_owned()),
        (2, "127.0.0.1:18002".to_owned()),
        (3, "127.0.0.1:18003".to_owned()),
    ]);
    let timeout_base = ClusterRaftConfig::new(
        1,
        "unused-connect-timeout-validation.redb",
        "127.0.0.1:18001".parse()?,
        timeout_nodes,
        validation_tls()?,
        true,
    )?;
    assert!(
        timeout_base
            .clone()
            .with_rpc_connect_timeout(Duration::ZERO)
            .is_err()
    );
    assert!(
        timeout_base
            .with_rpc_connect_timeout(Duration::from_secs(31))
            .is_err()
    );
    Ok(())
}

struct RunningCluster {
    bootstrap_config: ClusterRaftConfig,
    node_two_config: ClusterRaftConfig,
    node_three_config: ClusterRaftConfig,
    node_one: ClusterRaftConsensusAdapter,
    node_two: ClusterRaftConsensusAdapter,
    node_three: ClusterRaftConsensusAdapter,
}

struct SnapshotCutProxy {
    local_addr: SocketAddr,
    cut_next_snapshot: Arc<AtomicBool>,
    snapshot_cut_count: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<io::Result<()>>>,
}

impl SnapshotCutProxy {
    fn start(backend_addr: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let cut_next_snapshot = Arc::new(AtomicBool::new(false));
        let snapshot_cut_count = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_cut = Arc::clone(&cut_next_snapshot);
        let thread_count = Arc::clone(&snapshot_cut_count);
        let thread_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("agent-broker-snapshot-cut-proxy".to_owned())
            .spawn(move || {
                run_snapshot_cut_proxy(
                    &listener,
                    backend_addr,
                    thread_cut.as_ref(),
                    thread_count.as_ref(),
                    thread_stop.as_ref(),
                )
            })?;
        Ok(Self {
            local_addr,
            cut_next_snapshot,
            snapshot_cut_count,
            stop,
            thread: Some(thread),
        })
    }

    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    fn cut_next_snapshot(&self) {
        self.cut_next_snapshot.store(true, Ordering::Release);
    }

    fn snapshot_cut_count(&self) -> u64 {
        self.snapshot_cut_count.load(Ordering::Acquire)
    }

    fn stop(&mut self) -> io::Result<()> {
        self.stop.store(true, Ordering::Release);
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        match thread.join() {
            Ok(result) => result,
            Err(_) => Err(io::Error::other("snapshot cut proxy thread panicked")),
        }
    }
}

impl Drop for SnapshotCutProxy {
    fn drop(&mut self) {
        let _stop_result = self.stop();
    }
}

struct AppendResponseSuppressionProxy {
    local_addr: SocketAddr,
    suppression_armed: Arc<AtomicBool>,
    suppressed_count: Arc<AtomicU64>,
    completed_count: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<io::Result<()>>>,
}

impl AppendResponseSuppressionProxy {
    fn start(backend_addr: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let suppression_armed = Arc::new(AtomicBool::new(false));
        let suppressed_count = Arc::new(AtomicU64::new(0));
        let completed_count = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_armed = Arc::clone(&suppression_armed);
        let thread_suppressed = Arc::clone(&suppressed_count);
        let thread_completed = Arc::clone(&completed_count);
        let thread_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("agent-broker-append-response-suppression-proxy".to_owned())
            .spawn(move || {
                run_append_response_suppression_proxy(
                    &listener,
                    backend_addr,
                    thread_armed.as_ref(),
                    thread_suppressed.as_ref(),
                    thread_completed.as_ref(),
                    thread_stop.as_ref(),
                )
            })?;
        Ok(Self {
            local_addr,
            suppression_armed,
            suppressed_count,
            completed_count,
            stop,
            thread: Some(thread),
        })
    }

    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    fn arm(&self) {
        self.suppression_armed.store(true, Ordering::Release);
    }

    fn disarm(&self) {
        self.suppression_armed.store(false, Ordering::Release);
    }

    fn suppressed_count(&self) -> u64 {
        self.suppressed_count.load(Ordering::Acquire)
    }

    fn completed_count(&self) -> u64 {
        self.completed_count.load(Ordering::Acquire)
    }

    fn stop(&mut self) -> io::Result<()> {
        self.stop.store(true, Ordering::Release);
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        match thread.join() {
            Ok(result) => result,
            Err(_) => Err(io::Error::other(
                "append response suppression proxy thread panicked",
            )),
        }
    }
}

impl Drop for AppendResponseSuppressionProxy {
    fn drop(&mut self) {
        let _stop_result = self.stop();
    }
}

fn run_append_response_suppression_proxy(
    listener: &TcpListener,
    backend_addr: SocketAddr,
    suppression_armed: &AtomicBool,
    suppressed_count: &AtomicU64,
    completed_count: &AtomicU64,
    stop: &AtomicBool,
) -> io::Result<()> {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((client, _peer)) => {
                if let Err(error) = proxy_one_tls_rpc_with_response_suppression(
                    client,
                    backend_addr,
                    suppression_armed,
                    suppressed_count,
                    completed_count,
                ) && !is_expected_proxy_disconnect(&error)
                {
                    return Err(error);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(SUPPRESSION_PROXY_POLL_INTERVAL);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[test]
fn three_node_cluster_bootstraps_replicates_and_rejects_follower_writes()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let RunningCluster {
        bootstrap_config: _,
        node_two_config: _,
        node_three_config: _,
        mut node_one,
        mut node_two,
        mut node_three,
    } = open_three_node_cluster(directory.path())?;

    node_one.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("three-node-project")?,
        max_namespaces: 64,
    }))?;
    let leader_revision = node_one.revision();
    wait_for_revision(&mut node_two, leader_revision.get())?;
    wait_for_revision(&mut node_three, leader_revision.get())?;
    assert_eq!(node_two.revision(), leader_revision);
    assert_eq!(node_three.revision(), leader_revision);

    let follower_write = node_two.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("follower-write-must-not-commit")?,
        max_namespaces: 64,
    }));
    let follower_error = match follower_write {
        Ok(result) => return Err(format!("follower unexpectedly committed: {result:?}").into()),
        Err(error) => error,
    };
    assert_eq!(follower_error.code(), BrokerErrorCode::TransportError);
    assert_eq!(node_two.progress()?.current_leader, Some(1));

    node_three.shutdown()?;
    node_two.shutdown()?;
    node_one.shutdown()?;
    Ok(())
}

#[test]
fn three_node_cluster_rejects_plaintext_raft_transport() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let RunningCluster {
        bootstrap_config: _,
        node_two_config: _,
        node_three_config: _,
        mut node_one,
        mut node_two,
        node_three,
    } = open_three_node_cluster(directory.path())?;

    let follower_addr = node_two.progress()?.raft_rpc_addr;
    let mut plaintext = TcpStream::connect(follower_addr)?;
    plaintext.set_nodelay(true)?;
    plaintext.set_read_timeout(Some(Duration::from_secs(3)))?;
    plaintext.set_write_timeout(Some(PROXY_IO_TIMEOUT))?;
    plaintext.write_all(&4_u32.to_be_bytes())?;
    plaintext.write_all(b"{}\n\n")?;
    plaintext.flush()?;

    let mut received = Vec::with_capacity(256);
    let mut buffer = [0_u8; 256];
    loop {
        match plaintext.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                received.extend_from_slice(&buffer[..read]);
                if received.len() > 1_024 {
                    return Err(
                        "plaintext Raft rejection emitted an unexpectedly large response".into(),
                    );
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::UnexpectedEof
                ) =>
            {
                break;
            }
            Err(error) => {
                return Err(
                    format!("plaintext Raft connection was not fail-closed: {error}").into(),
                );
            }
        }
    }
    if received.len() >= 4 {
        let declared = u32::from_be_bytes(received[..4].try_into()?) as usize;
        if declared <= received.len().saturating_sub(4)
            && serde_json::from_slice::<serde_json::Value>(&received[4..4 + declared]).is_ok()
        {
            return Err(
                "plaintext Raft request unexpectedly received a framed JSON response".into(),
            );
        }
    }

    node_one.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("after-plaintext-raft-rejection")?,
        max_namespaces: 64,
    }))?;
    let revision = node_one.revision().get();
    wait_for_revision(&mut node_two, revision)?;

    node_three.shutdown()?;
    node_two.shutdown()?;
    node_one.shutdown()?;
    Ok(())
}

#[test]
fn three_node_readiness_requires_current_leader_quorum_authority() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let RunningCluster {
        bootstrap_config: _,
        node_two_config: _,
        node_three_config: _,
        mut node_one,
        mut node_two,
        mut node_three,
    } = open_three_node_cluster(directory.path())?;

    let leader_observer = node_one.observer();
    let follower_two_observer = node_two.observer();
    let follower_three_observer = node_three.observer();

    let leader_readiness = leader_observer.readiness();
    assert_eq!(leader_readiness.status, ClusterRaftReadinessStatus::Ready);
    assert!(leader_readiness.is_write_ready());
    assert_eq!(
        leader_readiness
            .progress
            .as_ref()
            .and_then(|progress| progress.current_leader),
        Some(1)
    );

    for follower_readiness in [
        follower_two_observer.readiness(),
        follower_three_observer.readiness(),
    ] {
        assert_eq!(
            follower_readiness.status,
            ClusterRaftReadinessStatus::Follower
        );
        assert!(!follower_readiness.is_write_ready());
        assert_eq!(
            follower_readiness
                .progress
                .as_ref()
                .and_then(|progress| progress.current_leader),
            Some(1)
        );
    }

    let leader_directory = node_one.group_directory()?;
    assert_eq!(leader_directory.term(), node_one.term());
    let follower_two_read = node_two.group_directory();
    let Err(follower_two_error) = follower_two_read else {
        return Err("follower must not return an authoritative Group directory".into());
    };
    assert_eq!(follower_two_error.code(), BrokerErrorCode::TransportError);
    let follower_three_read = node_three.group_directory();
    let Err(follower_three_error) = follower_three_read else {
        return Err("follower must not return an authoritative Group directory".into());
    };
    assert_eq!(follower_three_error.code(), BrokerErrorCode::TransportError);

    node_three.shutdown()?;
    node_two.shutdown()?;
    let quorum_lost = leader_observer.readiness();
    assert_eq!(
        quorum_lost.status,
        ClusterRaftReadinessStatus::QuorumUnavailable
    );
    assert!(!quorum_lost.is_write_ready());
    let quorum_lost_read = node_one.group_directory();
    let Err(quorum_lost_error) = quorum_lost_read else {
        return Err("quorum-lost leader must not return an authoritative Group directory".into());
    };
    assert_eq!(quorum_lost_error.code(), BrokerErrorCode::TransportError);

    node_one.shutdown()?;
    assert_eq!(
        leader_observer.readiness().status,
        ClusterRaftReadinessStatus::ConsensusUnavailable
    );
    Ok(())
}

#[test]
fn three_node_cluster_re_elects_after_leader_shutdown_and_rejoins() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let RunningCluster {
        bootstrap_config,
        node_two_config: _,
        node_three_config: _,
        mut node_one,
        mut node_two,
        mut node_three,
    } = open_three_node_cluster(directory.path())?;

    node_one.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("before-leader-shutdown")?,
        max_namespaces: 64,
    }))?;
    let initial_revision = node_one.revision().get();
    let initial_raft_term = node_one.progress()?.raft_term;
    wait_for_revision(&mut node_two, initial_revision)?;
    wait_for_revision(&mut node_three, initial_revision)?;
    assert!(node_one.maintenance_authority()?);
    assert!(!node_two.maintenance_authority()?);
    assert!(!node_three.maintenance_authority()?);
    node_one.shutdown()?;

    let new_leader = wait_for_survivor_leader(&mut node_two, &mut node_three, initial_raft_term)?;
    if new_leader == 2 {
        assert!(node_two.maintenance_authority()?);
        assert!(!node_three.maintenance_authority()?);
    } else {
        assert!(node_three.maintenance_authority()?);
        assert!(!node_two.maintenance_authority()?);
    }
    let leader = if new_leader == 2 {
        &mut node_two
    } else {
        &mut node_three
    };
    leader.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("after-leader-shutdown")?,
        max_namespaces: 64,
    }))?;
    let failover_revision = leader.revision().get();
    let failover_term = leader.term();
    wait_for_revision(&mut node_two, failover_revision)?;
    wait_for_revision(&mut node_three, failover_revision)?;
    assert!(failover_term.get() > 1);

    let mut rejoined = ClusterRaftConsensusAdapter::open(bootstrap_config)?;
    wait_for_revision(&mut rejoined, failover_revision)?;
    let rejoined_progress = rejoined.progress()?;
    assert_eq!(rejoined_progress.current_leader, Some(new_leader));
    assert_eq!(rejoined.term(), failover_term);

    rejoined.shutdown()?;
    node_three.shutdown()?;
    node_two.shutdown()?;
    Ok(())
}

#[test]
fn three_node_cluster_forces_snapshot_catch_up_after_follower_log_gap() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let RunningCluster {
        bootstrap_config: _,
        node_two_config: _,
        node_three_config,
        mut node_one,
        mut node_two,
        mut node_three,
    } = open_three_node_cluster_with_snapshot_policy(directory.path(), 8, 16, 2)?;

    let follower_before_gap = node_three.progress()?;
    let follower_last_log_before_gap = follower_before_gap
        .last_log_index
        .ok_or("follower had no Raft log before snapshot-gap isolation")?;
    node_three.shutdown()?;

    for sequence in 0..48_u64 {
        node_one.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: NamespaceId::new(format!("snapshot-gap-{sequence:02}"))?,
            max_namespaces: 128,
        }))?;
    }
    let target_revision = node_one.revision().get();
    wait_for_revision(&mut node_two, target_revision)?;

    let leader_compacted =
        wait_for_snapshot_purge_past(&mut node_one, follower_last_log_before_gap)?;
    let leader_snapshot_index = leader_compacted
        .snapshot_index
        .ok_or("leader did not report a durable snapshot")?;
    let leader_purged_index = leader_compacted
        .purged_index
        .ok_or("leader did not report purged Raft logs")?;
    assert!(leader_snapshot_index > follower_last_log_before_gap);
    assert!(leader_purged_index > follower_last_log_before_gap);

    let mut rejoined = ClusterRaftConsensusAdapter::open(node_three_config)?;
    wait_for_revision(&mut rejoined, target_revision)?;
    let follower_after_catch_up = rejoined.progress()?;
    let follower_snapshot_index = follower_after_catch_up
        .snapshot_index
        .ok_or("rejoined follower did not install a snapshot")?;

    assert_eq!(
        follower_after_catch_up.broker_revision.get(),
        target_revision
    );
    assert_eq!(
        follower_after_catch_up.applied_index,
        follower_after_catch_up.committed_index
    );
    assert!(follower_snapshot_index > follower_last_log_before_gap);
    assert!(follower_snapshot_index <= leader_snapshot_index);

    rejoined.shutdown()?;
    node_two.shutdown()?;
    node_one.shutdown()?;
    Ok(())
}

#[test]
fn three_node_session_owner_epoch_fences_stale_owner_after_leader_change()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let RunningCluster {
        bootstrap_config: _,
        node_two_config: _,
        node_three_config: _,
        mut node_one,
        mut node_two,
        mut node_three,
    } = open_three_node_cluster(directory.path())?;

    let session_id = CommandSessionId::new("three-node-owner-session")?;
    let epoch_one = SessionOwnerEpoch::INITIAL;
    let epoch_one_identity = CommandIdentity::new_with_owner_epoch(
        session_id.clone(),
        epoch_one,
        CommandSequence::new(1)?,
    );
    let first_command = BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("three-node-owner-epoch-one")?,
        max_namespaces: 64,
    });
    let _first = node_one.propose_identified(epoch_one_identity, first_command)?;

    let owner_instance = SessionOwnerInstanceId::new("three-node-owner-process-a")?;
    let epoch_two = node_one.acquire_command_session_owner(
        session_id.clone(),
        epoch_one,
        owner_instance.clone(),
    )?;
    assert_eq!(epoch_two.get(), 2);
    let acquisition_retry = node_one.acquire_command_session_owner(
        session_id.clone(),
        epoch_one,
        owner_instance.clone(),
    )?;
    assert_eq!(acquisition_retry, epoch_two);
    let stale_contender = node_one.acquire_command_session_owner(
        session_id.clone(),
        epoch_one,
        SessionOwnerInstanceId::new("three-node-owner-process-b")?,
    );
    let stale_contender_error = match stale_contender {
        Ok(epoch) => return Err(format!("stale competing owner acquired epoch {epoch:?}").into()),
        Err(error) => error,
    };
    assert_eq!(stale_contender_error.code(), BrokerErrorCode::StaleFence);
    let epoch_two_identity = CommandIdentity::new_with_owner(
        session_id.clone(),
        epoch_two,
        owner_instance.clone(),
        CommandSequence::new(1)?,
    );
    let epoch_two_command = BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("three-node-owner-epoch-two")?,
        max_namespaces: 64,
    });
    let epoch_two_result =
        node_one.propose_identified(epoch_two_identity.clone(), epoch_two_command.clone())?;
    let committed_revision = node_one.revision().get();
    wait_for_revision(&mut node_two, committed_revision)?;
    wait_for_revision(&mut node_three, committed_revision)?;
    let previous_raft_term = node_one.progress()?.raft_term;
    node_one.shutdown()?;

    let new_leader = wait_for_survivor_leader(&mut node_two, &mut node_three, previous_raft_term)?;
    let leader = if new_leader == 2 {
        &mut node_two
    } else {
        &mut node_three
    };
    let _term_sync = leader.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("three-node-owner-epoch-one")?,
        max_namespaces: 64,
    }))?;
    let revision_after_term_sync = leader.revision().get();

    let acquisition_retry_after_failover =
        leader.acquire_command_session_owner(session_id.clone(), epoch_one, owner_instance)?;
    assert_eq!(acquisition_retry_after_failover, epoch_two);
    assert_eq!(leader.revision().get(), revision_after_term_sync);

    let stale_identity =
        CommandIdentity::new_with_owner_epoch(session_id, epoch_one, CommandSequence::new(2)?);
    let stale = leader.propose_identified(
        stale_identity,
        BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: NamespaceId::new("three-node-stale-owner-must-not-apply")?,
            max_namespaces: 64,
        }),
    );
    let stale_error = match stale {
        Ok(result) => return Err(format!("stale owner unexpectedly committed: {result:?}").into()),
        Err(error) => error,
    };
    assert_eq!(stale_error.code(), BrokerErrorCode::StaleFence);
    assert_eq!(leader.revision().get(), revision_after_term_sync);

    let exact_retry = leader.propose_identified(epoch_two_identity, epoch_two_command)?;
    assert_eq!(exact_retry, epoch_two_result);
    assert_eq!(leader.revision().get(), revision_after_term_sync);

    node_three.shutdown()?;
    node_two.shutdown()?;
    Ok(())
}

#[test]
fn three_node_cluster_corrupted_raft_vote_fails_stop_while_majority_progresses()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let RunningCluster {
        bootstrap_config: _,
        node_two_config: _,
        node_three_config,
        mut node_one,
        mut node_two,
        node_three,
    } = open_three_node_cluster(directory.path())?;

    node_one.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("before-durable-corruption")?,
        max_namespaces: 64,
    }))?;
    let revision_before_corruption = node_one.revision().get();
    wait_for_revision(&mut node_two, revision_before_corruption)?;
    node_three.shutdown()?;

    corrupt_raft_meta_value(node_three_config.state_path(), "vote_v1", b"not-valid-json")?;

    let first_reopen_error = match ClusterRaftConsensusAdapter::open(node_three_config.clone()) {
        Ok(adapter) => {
            adapter.shutdown()?;
            return Err("corrupted Raft node unexpectedly reopened".into());
        }
        Err(error) => error,
    };
    assert_eq!(first_reopen_error.code(), BrokerErrorCode::PersistenceError);

    let surviving_leader = wait_for_pair_leader(&mut node_one, &mut node_two)?;
    let majority_command = BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("majority-progress-after-corruption")?,
        max_namespaces: 64,
    });
    if surviving_leader == 1 {
        node_one.propose(majority_command)?;
    } else {
        node_two.propose(majority_command)?;
    }
    let majority_revision = if surviving_leader == 1 {
        node_one.revision().get()
    } else {
        node_two.revision().get()
    };
    assert!(majority_revision > revision_before_corruption);
    wait_for_revision(&mut node_two, majority_revision)?;
    assert_eq!(node_two.revision().get(), majority_revision);

    let second_reopen_error = match ClusterRaftConsensusAdapter::open(node_three_config) {
        Ok(adapter) => {
            adapter.shutdown()?;
            return Err("corrupted Raft node silently reset durable state on retry".into());
        }
        Err(error) => error,
    };
    assert_eq!(
        second_reopen_error.code(),
        BrokerErrorCode::PersistenceError
    );

    node_two.shutdown()?;
    node_one.shutdown()?;
    Ok(())
}

#[test]
fn three_node_cluster_retries_snapshot_after_mid_transfer_tcp_cut() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let (
        RunningCluster {
            bootstrap_config: _,
            node_two_config: _,
            node_three_config,
            mut node_one,
            mut node_two,
            mut node_three,
        },
        mut proxy,
    ) = open_three_node_cluster_with_snapshot_proxy(directory.path(), 8, 16, 2)?;

    let follower_before_gap = node_three.progress()?;
    let follower_last_log_before_gap = follower_before_gap
        .last_log_index
        .ok_or("proxied follower had no Raft log before snapshot-gap isolation")?;
    node_three.shutdown()?;

    for sequence in 0..128_u64 {
        let suffix = "x".repeat(96);
        node_one.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: NamespaceId::new(format!("snapshot-retry-gap-{sequence:03}-{suffix}"))?,
            max_namespaces: 256,
        }))?;
    }
    let target_revision = node_one.revision().get();
    wait_for_revision(&mut node_two, target_revision)?;
    let leader_compacted =
        wait_for_snapshot_purge_past(&mut node_one, follower_last_log_before_gap)?;
    assert!(
        leader_compacted
            .purged_index
            .is_some_and(|index| index > follower_last_log_before_gap)
    );

    proxy.cut_next_snapshot();
    let mut rejoined = ClusterRaftConsensusAdapter::open(node_three_config)?;
    wait_for_revision(&mut rejoined, target_revision)?;
    let follower_after_retry = rejoined.progress()?;

    assert_eq!(proxy.snapshot_cut_count(), 1);
    assert_eq!(follower_after_retry.broker_revision.get(), target_revision);
    assert_eq!(
        follower_after_retry.applied_index,
        follower_after_retry.committed_index
    );
    assert!(
        follower_after_retry
            .snapshot_index
            .is_some_and(|index| index > follower_last_log_before_gap)
    );

    rejoined.shutdown()?;
    node_two.shutdown()?;
    node_one.shutdown()?;
    proxy.stop()?;
    Ok(())
}

#[test]
fn three_node_cluster_slow_raft_connection_does_not_block_quorum_rpc() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let RunningCluster {
        bootstrap_config: _,
        node_two_config: _,
        node_three_config: _,
        mut node_one,
        mut node_two,
        node_three,
    } = open_three_node_cluster(directory.path())?;
    node_three.shutdown()?;

    let surviving_leader = wait_for_pair_leader(&mut node_one, &mut node_two)?;
    let follower_addr = if surviving_leader == 1 {
        node_two.progress()?.raft_rpc_addr
    } else {
        node_one.progress()?.raft_rpc_addr
    };
    let mut slow_connection = TcpStream::connect(follower_addr)?;
    slow_connection.set_nodelay(true)?;
    slow_connection.write_all(&1_024_u32.to_be_bytes())?;
    slow_connection.flush()?;
    if surviving_leader == 1 {
        wait_for_active_raft_rpc(&mut node_two)?;
    } else {
        wait_for_active_raft_rpc(&mut node_one)?;
    }

    let command = BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("slow-peer-must-not-block-quorum")?,
        max_namespaces: 64,
    });
    let started = Instant::now();
    if surviving_leader == 1 {
        node_one.propose(command)?;
    } else {
        node_two.propose(command)?;
    }
    let elapsed = started.elapsed();
    if elapsed >= PROXY_IO_TIMEOUT {
        return Err(format!(
            "quorum RPC was head-of-line blocked by a slow Raft connection for {elapsed:?}"
        )
        .into());
    }

    let _shutdown_result = slow_connection.shutdown(Shutdown::Both);
    let committed_revision = if surviving_leader == 1 {
        node_one.revision().get()
    } else {
        node_two.revision().get()
    };
    wait_for_revision(&mut node_one, committed_revision)?;
    wait_for_revision(&mut node_two, committed_revision)?;

    node_two.shutdown()?;
    node_one.shutdown()?;
    Ok(())
}

#[test]
fn identified_write_quorum_ack_loss_reports_commit_outcome_unknown() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let (
        RunningCluster {
            bootstrap_config: _,
            node_two_config: _,
            node_three_config: _,
            mut node_one,
            mut node_two,
            mut node_three,
        },
        mut proxies,
    ) = open_three_node_cluster_with_append_response_suppression_proxies(directory.path())?;

    let baseline_namespace_id = NamespaceId::new("late-commit-baseline")?;
    let revision_before = namespace_revision(
        node_one.propose(ensure_namespace_command(&baseline_namespace_id))?,
        "baseline namespace proposal",
    )?;
    wait_for_three_node_revision(
        &mut node_one,
        &mut node_two,
        &mut node_three,
        revision_before,
    )?;
    let stable_before = node_one.progress()?;
    assert_eq!(stable_before.current_leader, Some(1));
    assert_eq!(stable_before.broker_term.get(), stable_before.raft_term);
    node_three.shutdown()?;

    let identity = CommandIdentity::new(
        CommandSessionId::new("late-commit-client")?,
        CommandSequence::new(1)?,
    );
    let namespace_id = NamespaceId::new("late-commit-identified")?;

    assert_identified_write_reports_unknown_after_submission(
        &mut node_one,
        &proxies[1],
        &identity,
        &namespace_id,
    )?;
    wait_for_suppressed_append(&proxies[1])?;
    proxies[1].disarm();

    node_two.shutdown()?;
    node_one.shutdown()?;
    for proxy in &mut proxies {
        proxy.stop()?;
    }
    Ok(())
}

#[test]
fn three_node_cluster_repeated_leader_restart_preserves_idempotent_state()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let RunningCluster {
        bootstrap_config,
        node_two_config,
        node_three_config,
        node_one,
        node_two,
        node_three,
    } = open_three_node_cluster(directory.path())?;
    let configs = BTreeMap::from([
        (1_u64, bootstrap_config),
        (2_u64, node_two_config),
        (3_u64, node_three_config),
    ]);
    let mut nodes = BTreeMap::from([(1_u64, node_one), (2_u64, node_two), (3_u64, node_three)]);

    for round in 0..3_u64 {
        let old_leader = wait_for_common_running_leader(&mut nodes)?;
        let stopped = nodes
            .remove(&old_leader)
            .ok_or("current leader disappeared before restart cycle")?;
        stopped.shutdown()?;

        let survivor_leader = wait_for_common_running_leader(&mut nodes)?;
        let namespace_id = NamespaceId::new(format!("restart-cycle-{round}"))?;
        nodes
            .get_mut(&survivor_leader)
            .ok_or("surviving leader disappeared before commit")?
            .propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
                namespace_id: namespace_id.clone(),
                max_namespaces: 64,
            }))?;
        let committed_revision = nodes
            .get(&survivor_leader)
            .ok_or("surviving leader disappeared after commit")?
            .revision()
            .get();
        wait_for_all_running_revisions(&mut nodes, committed_revision)?;

        let config = configs
            .get(&old_leader)
            .ok_or("restart configuration disappeared")?
            .clone();
        let mut rejoined = ClusterRaftConsensusAdapter::open(config)?;
        wait_for_revision(&mut rejoined, committed_revision)?;
        if nodes.insert(old_leader, rejoined).is_some() {
            return Err("rejoined node replaced an unexpected live node".into());
        }
        wait_for_all_running_revisions(&mut nodes, committed_revision)?;

        let retry_leader = wait_for_common_running_leader(&mut nodes)?;
        let retry_command = || {
            BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
                namespace_id: namespace_id.clone(),
                max_namespaces: 64,
            })
        };
        nodes
            .get_mut(&retry_leader)
            .ok_or("retry leader disappeared before fencing synchronization")?
            .propose(retry_command())?;
        let revision_before_exact_retry = nodes
            .get(&retry_leader)
            .ok_or("retry leader disappeared after fencing synchronization")?
            .revision()
            .get();
        nodes
            .get_mut(&retry_leader)
            .ok_or("retry leader disappeared before idempotency retry")?
            .propose(retry_command())?;
        let revision_after_exact_retry = nodes
            .get(&retry_leader)
            .ok_or("retry leader disappeared after idempotency retry")?
            .revision()
            .get();
        assert_eq!(revision_after_exact_retry, revision_before_exact_retry);
        wait_for_all_running_revisions(&mut nodes, revision_after_exact_retry)?;
    }

    for (_node_id, node) in nodes {
        node.shutdown()?;
    }
    Ok(())
}

#[test]
fn three_node_cluster_raft_connection_queue_saturates_with_bounded_rejection()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let RunningCluster {
        bootstrap_config: _,
        node_two_config: _,
        node_three_config: _,
        mut node_one,
        mut node_two,
        node_three,
    } = open_three_node_cluster(directory.path())?;
    let follower_addr = node_two.progress()?.raft_rpc_addr;
    let mut held_connections = Vec::with_capacity(72);
    for _index in 0..72_usize {
        let stream = TcpStream::connect(follower_addr)?;
        stream.set_nodelay(true)?;
        // Hold the accepted socket before ClientHello. Invalid plaintext is rejected immediately
        // by mandatory mTLS and therefore cannot exercise the bounded handshake worker/queue.
        held_connections.push(stream);
    }

    wait_for_raft_rpc_saturation(&mut node_two, 8, 64)?;
    let saturated = node_two.progress()?;
    assert_eq!(saturated.raft_rpc_active_connections, 8);
    assert_eq!(saturated.raft_rpc_queued_connections, 64);

    let mut rejected = TcpStream::connect(follower_addr)?;
    rejected.set_read_timeout(Some(PROXY_IO_TIMEOUT))?;
    let mut byte = [0_u8; 1];
    match rejected.read(&mut byte) {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::BrokenPipe
            ) => {}
        Ok(read) => {
            return Err(
                format!("saturated Raft RPC queue unexpectedly returned {read} bytes").into(),
            );
        }
        Err(error) => {
            return Err(format!(
                "saturated Raft RPC connection was not promptly rejected: {error}"
            )
            .into());
        }
    }

    for connection in held_connections {
        let _shutdown_result = connection.shutdown(Shutdown::Both);
    }
    wait_for_raft_rpc_load_at_most(&mut node_two, 1, 0)?;

    node_one.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("after-raft-connection-saturation")?,
        max_namespaces: 64,
    }))?;
    let revision = node_one.revision().get();
    wait_for_revision(&mut node_two, revision)?;

    node_three.shutdown()?;
    node_two.shutdown()?;
    node_one.shutdown()?;
    Ok(())
}

fn ensure_namespace_command(namespace_id: &NamespaceId) -> BrokerCommand {
    BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: namespace_id.clone(),
        max_namespaces: 64,
    })
}

fn namespace_revision(
    result: BrokerMutationResult,
    context: &'static str,
) -> Result<u64, Box<dyn Error>> {
    let BrokerMutationResult::Namespace(namespace) = result else {
        return Err(format!("{context} returned an unexpected mutation result").into());
    };
    Ok(namespace.metadata.revision.get())
}

fn wait_for_proxy_completion_after(
    proxy: &AppendResponseSuppressionProxy,
    completed_before: u64,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    while proxy.completed_count() <= completed_before {
        if Instant::now() >= deadline {
            return Err("quorum follower proxy did not observe a pass-through heartbeat".into());
        }
        thread::sleep(POLL_INTERVAL);
    }
    Ok(())
}

fn wait_for_suppressed_append(
    proxy: &AppendResponseSuppressionProxy,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    while proxy.suppressed_count() < 1 {
        if Instant::now() >= deadline {
            return Err(
                "identified AppendEntries ACK was not suppressed on the quorum follower".into(),
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
    Ok(())
}

fn wait_for_three_node_revision(
    node_one: &mut ClusterRaftConsensusAdapter,
    node_two: &mut ClusterRaftConsensusAdapter,
    node_three: &mut ClusterRaftConsensusAdapter,
    revision: u64,
) -> Result<(), Box<dyn Error>> {
    wait_for_revision(node_one, revision)?;
    wait_for_revision(node_two, revision)?;
    wait_for_revision(node_three, revision)
}

fn assert_identified_write_reports_unknown_after_submission(
    leader: &mut ClusterRaftConsensusAdapter,
    quorum_follower_proxy: &AppendResponseSuppressionProxy,
    identity: &CommandIdentity,
    namespace_id: &NamespaceId,
) -> Result<(), Box<dyn Error>> {
    let completed_before = quorum_follower_proxy.completed_count();
    wait_for_proxy_completion_after(quorum_follower_proxy, completed_before)?;
    quorum_follower_proxy.arm();
    let started = Instant::now();
    let first = leader.propose_identified(identity.clone(), ensure_namespace_command(namespace_id));
    let elapsed = started.elapsed();
    let timeout_error = match first {
        Ok(result) => {
            return Err(format!(
                "identified mutation unexpectedly returned success: elapsed={elapsed:?} suppressed_count={} result={result:?}",
                quorum_follower_proxy.suppressed_count()
            )
            .into());
        }
        Err(error) => error,
    };
    assert_eq!(timeout_error.code(), BrokerErrorCode::CommitOutcomeUnknown);
    assert!(elapsed >= IDENTIFIED_TIMEOUT_TEST_DEADLINE);
    Ok(())
}

fn open_three_node_cluster(root: &Path) -> Result<RunningCluster, Box<dyn Error>> {
    open_three_node_cluster_with_policy(root, None)
}

fn open_three_node_cluster_with_snapshot_policy(
    root: &Path,
    snapshot_log_interval: u64,
    replication_lag_threshold: u64,
    max_in_snapshot_log_to_keep: u64,
) -> Result<RunningCluster, Box<dyn Error>> {
    open_three_node_cluster_with_policy(
        root,
        Some((
            snapshot_log_interval,
            replication_lag_threshold,
            max_in_snapshot_log_to_keep,
        )),
    )
}

fn open_three_node_cluster_with_policy(
    root: &Path,
    snapshot_policy: Option<(u64, u64, u64)>,
) -> Result<RunningCluster, Box<dyn Error>> {
    let (reservations, addresses) = reserve_three_ports()?;
    open_three_node_cluster_with_topology(
        root,
        snapshot_policy,
        reservations,
        addresses,
        addresses,
        None,
    )
}

fn open_three_node_cluster_with_snapshot_proxy(
    root: &Path,
    snapshot_log_interval: u64,
    replication_lag_threshold: u64,
    max_in_snapshot_log_to_keep: u64,
) -> Result<(RunningCluster, SnapshotCutProxy), Box<dyn Error>> {
    let (reservations, addresses) = reserve_three_ports()?;
    let proxy = SnapshotCutProxy::start(addresses[2])?;
    let cluster = open_three_node_cluster_with_topology(
        root,
        Some((
            snapshot_log_interval,
            replication_lag_threshold,
            max_in_snapshot_log_to_keep,
        )),
        reservations,
        addresses,
        [addresses[0], addresses[1], proxy.local_addr()],
        None,
    )?;
    Ok((cluster, proxy))
}

fn open_three_node_cluster_with_append_response_suppression_proxies(
    root: &Path,
) -> Result<(RunningCluster, [AppendResponseSuppressionProxy; 3]), Box<dyn Error>> {
    let (reservations, addresses) = reserve_three_ports()?;
    let proxy_one = AppendResponseSuppressionProxy::start(addresses[0])?;
    let proxy_two = AppendResponseSuppressionProxy::start(addresses[1])?;
    let proxy_three = AppendResponseSuppressionProxy::start(addresses[2])?;
    let cluster = open_three_node_cluster_with_topology(
        root,
        None,
        reservations,
        addresses,
        [
            proxy_one.local_addr(),
            proxy_two.local_addr(),
            proxy_three.local_addr(),
        ],
        Some(IDENTIFIED_TIMEOUT_TEST_DEADLINE),
    )?;
    Ok((cluster, [proxy_one, proxy_two, proxy_three]))
}

fn open_three_node_cluster_with_topology(
    root: &Path,
    snapshot_policy: Option<(u64, u64, u64)>,
    reservations: Vec<TcpListener>,
    addresses: [SocketAddr; 3],
    advertised_addresses: [SocketAddr; 3],
    identified_write_timeout: Option<Duration>,
) -> Result<RunningCluster, Box<dyn Error>> {
    let tls_directory = root.join("raft-tls");
    tls_fixture::write_cluster_tls_fixture(&tls_directory, &[1, 2, 3])?;
    let tls = ClusterRaftTlsConfig::new(tls_directory)?;
    let nodes = BTreeMap::from([
        (1, advertised_addresses[0].to_string()),
        (2, advertised_addresses[1].to_string()),
        (3, advertised_addresses[2].to_string()),
    ]);
    let mut node_one_config = ClusterRaftConfig::new(
        1,
        root.join("node-1.redb"),
        addresses[0],
        nodes.clone(),
        tls.clone(),
        true,
    )?;
    let mut node_two_config = ClusterRaftConfig::new(
        2,
        root.join("node-2.redb"),
        addresses[1],
        nodes.clone(),
        tls.clone(),
        false,
    )?;
    let mut node_three_config =
        ClusterRaftConfig::new(3, root.join("node-3.redb"), addresses[2], nodes, tls, false)?;
    if let Some((snapshot_log_interval, replication_lag_threshold, max_in_snapshot_log_to_keep)) =
        snapshot_policy
    {
        node_one_config = node_one_config.with_snapshot_catch_up_policy(
            snapshot_log_interval,
            replication_lag_threshold,
            max_in_snapshot_log_to_keep,
        )?;
        node_two_config = node_two_config.with_snapshot_catch_up_policy(
            snapshot_log_interval,
            replication_lag_threshold,
            max_in_snapshot_log_to_keep,
        )?;
        node_three_config = node_three_config.with_snapshot_catch_up_policy(
            snapshot_log_interval,
            replication_lag_threshold,
            max_in_snapshot_log_to_keep,
        )?;
    }
    if let Some(timeout) = identified_write_timeout {
        node_one_config = node_one_config
            .with_identified_write_timeout(timeout)?
            .with_raft_timing(
                IDENTIFIED_TIMEOUT_TEST_ELECTION_MIN,
                IDENTIFIED_TIMEOUT_TEST_ELECTION_MAX,
                IDENTIFIED_TIMEOUT_TEST_HEARTBEAT,
            )?;
        node_two_config = node_two_config
            .with_identified_write_timeout(timeout)?
            .with_raft_timing(
                IDENTIFIED_TIMEOUT_TEST_ELECTION_MIN,
                IDENTIFIED_TIMEOUT_TEST_ELECTION_MAX,
                IDENTIFIED_TIMEOUT_TEST_HEARTBEAT,
            )?;
        node_three_config = node_three_config
            .with_identified_write_timeout(timeout)?
            .with_raft_timing(
                IDENTIFIED_TIMEOUT_TEST_ELECTION_MIN,
                IDENTIFIED_TIMEOUT_TEST_ELECTION_MAX,
                IDENTIFIED_TIMEOUT_TEST_HEARTBEAT,
            )?;
    }
    let mut reservations = reservations.into_iter();
    let node_one_port = reservations
        .next()
        .ok_or("missing node 1 port reservation")?;
    let node_two_port = reservations
        .next()
        .ok_or("missing node 2 port reservation")?;
    let node_three_port = reservations
        .next()
        .ok_or("missing node 3 port reservation")?;

    drop(node_two_port);
    let mut node_two = ClusterRaftConsensusAdapter::open(node_two_config.clone())?;
    drop(node_three_port);
    let mut node_three = ClusterRaftConsensusAdapter::open(node_three_config.clone())?;
    drop(node_one_port);
    let mut node_one = ClusterRaftConsensusAdapter::open(node_one_config.clone())?;
    let expected_voters = BTreeSet::from([1, 2, 3]);
    wait_for_cluster(&mut node_one, &expected_voters, 1)?;
    wait_for_cluster(&mut node_two, &expected_voters, 1)?;
    wait_for_cluster(&mut node_three, &expected_voters, 1)?;
    Ok(RunningCluster {
        bootstrap_config: node_one_config,
        node_two_config: node_two_config.clone(),
        node_three_config,
        node_one,
        node_two,
        node_three,
    })
}

fn wait_for_snapshot_purge_past(
    node: &mut ClusterRaftConsensusAdapter,
    stale_follower_last_log: u64,
) -> Result<agent_broker_consensus::ClusterRaftProgress, Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    loop {
        let progress = node.progress()?;
        if progress
            .snapshot_index
            .is_some_and(|index| index > stale_follower_last_log)
            && progress
                .purged_index
                .is_some_and(|index| index > stale_follower_last_log)
        {
            return Ok(progress);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "leader did not snapshot and purge beyond stale follower log {stale_follower_last_log}: {progress:?}"
            )
            .into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_active_raft_rpc(node: &mut ClusterRaftConsensusAdapter) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    loop {
        let progress = node.progress()?;
        if progress.raft_rpc_active_connections >= 1 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("slow Raft connection was never observed as active: {progress:?}").into(),
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_raft_rpc_saturation(
    node: &mut ClusterRaftConsensusAdapter,
    expected_active: usize,
    expected_queued: usize,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    loop {
        let progress = node.progress()?;
        if progress.raft_rpc_active_connections == expected_active
            && progress.raft_rpc_queued_connections == expected_queued
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("Raft RPC queue did not saturate at bounded limits: {progress:?}").into(),
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_raft_rpc_load_at_most(
    node: &mut ClusterRaftConsensusAdapter,
    max_active: usize,
    max_queued: usize,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    loop {
        let progress = node.progress()?;
        if progress.raft_rpc_active_connections <= max_active
            && progress.raft_rpc_queued_connections <= max_queued
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("Raft RPC load did not drain after saturation: {progress:?}").into(),
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn corrupt_raft_meta_value(path: &Path, key: &str, payload: &[u8]) -> Result<(), Box<dyn Error>> {
    let database = Database::create(path)?;
    let transaction = database.begin_write()?;
    {
        let mut table = transaction.open_table(TEST_RAFT_META_TABLE)?;
        table.insert(key, payload)?;
    }
    transaction.commit()?;
    Ok(())
}

fn run_snapshot_cut_proxy(
    listener: &TcpListener,
    backend_addr: SocketAddr,
    cut_next_snapshot: &AtomicBool,
    snapshot_cut_count: &AtomicU64,
    stop: &AtomicBool,
) -> io::Result<()> {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((client, _peer)) => {
                if let Err(error) = proxy_one_tls_rpc_with_snapshot_cut(
                    client,
                    backend_addr,
                    cut_next_snapshot,
                    snapshot_cut_count,
                ) && !is_expected_proxy_disconnect(&error)
                {
                    return Err(error);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(POLL_INTERVAL);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn proxy_one_tls_rpc_with_snapshot_cut(
    mut client: TcpStream,
    backend_addr: SocketAddr,
    cut_next_snapshot: &AtomicBool,
    snapshot_cut_count: &AtomicU64,
) -> io::Result<()> {
    configure_proxy_stream(&client)?;
    let mut backend = TcpStream::connect(backend_addr)?;
    configure_proxy_stream(&backend)?;

    relay_tls_burst(&mut client, &mut backend, None)?;
    relay_tls_burst(&mut backend, &mut client, None)?;
    let cut_after = cut_next_snapshot
        .load(Ordering::Acquire)
        .then_some(OPAQUE_SNAPSHOT_CUT_AFTER_CLIENT_BYTES);
    if relay_tls_burst(&mut client, &mut backend, cut_after)? {
        cut_next_snapshot.store(false, Ordering::Release);
        snapshot_cut_count.fetch_add(1, Ordering::AcqRel);
        let _backend_shutdown = backend.shutdown(Shutdown::Both);
        let _client_shutdown = client.shutdown(Shutdown::Both);
        return Ok(());
    }
    relay_tls_burst(&mut backend, &mut client, None)?;
    Ok(())
}

fn proxy_one_tls_rpc_with_response_suppression(
    mut client: TcpStream,
    backend_addr: SocketAddr,
    suppression_armed: &AtomicBool,
    suppressed_count: &AtomicU64,
    completed_count: &AtomicU64,
) -> io::Result<()> {
    configure_proxy_stream(&client)?;
    let mut backend = TcpStream::connect(backend_addr)?;
    configure_proxy_stream(&backend)?;

    relay_tls_burst(&mut client, &mut backend, None)?;
    relay_tls_burst(&mut backend, &mut client, None)?;
    relay_tls_burst(&mut client, &mut backend, None)?;

    if suppression_armed.load(Ordering::Acquire) {
        discard_tls_burst(&mut backend)?;
        suppressed_count.fetch_add(1, Ordering::AcqRel);
        let _backend_shutdown = backend.shutdown(Shutdown::Both);
        let _shutdown_result = client.shutdown(Shutdown::Both);
        return Ok(());
    }
    relay_tls_burst(&mut backend, &mut client, None)?;
    completed_count.fetch_add(1, Ordering::AcqRel);
    Ok(())
}

fn configure_proxy_stream(stream: &TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(PROXY_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(PROXY_IO_TIMEOUT))
}

fn relay_tls_burst(
    source: &mut TcpStream,
    destination: &mut TcpStream,
    cut_after_bytes: Option<usize>,
) -> io::Result<bool> {
    source.set_read_timeout(Some(PROXY_IO_TIMEOUT))?;
    let mut buffer = [0_u8; 16 * 1024];
    let mut forwarded = 0_usize;
    loop {
        match source.read(&mut buffer) {
            Ok(0) => return Ok(false),
            Ok(read) => {
                let allowed =
                    cut_after_bytes.map_or(read, |limit| limit.saturating_sub(forwarded).min(read));
                if allowed > 0 {
                    destination.write_all(&buffer[..allowed])?;
                    destination.flush()?;
                    forwarded = forwarded.saturating_add(allowed);
                }
                if cut_after_bytes.is_some_and(|limit| forwarded >= limit) {
                    return Ok(true);
                }
                source.set_read_timeout(Some(OPAQUE_PROXY_BURST_GAP))?;
            }
            Err(error)
                if forwarded > 0
                    && matches!(
                        error.kind(),
                        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                    ) =>
            {
                source.set_read_timeout(Some(PROXY_IO_TIMEOUT))?;
                return Ok(false);
            }
            Err(error) => return Err(error),
        }
    }
}

fn discard_tls_burst(source: &mut TcpStream) -> io::Result<()> {
    source.set_read_timeout(Some(PROXY_IO_TIMEOUT))?;
    let mut buffer = [0_u8; 16 * 1024];
    let first = source.read(&mut buffer)?;
    if first == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "opaque TLS proxy backend closed before a suppressible response",
        ));
    }
    source.set_read_timeout(Some(OPAQUE_PROXY_BURST_GAP))?;
    loop {
        match source.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_expected_proxy_disconnect(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::TimedOut
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
    )
}

fn reserve_three_ports() -> Result<(Vec<TcpListener>, [SocketAddr; 3]), Box<dyn Error>> {
    let first = TcpListener::bind("127.0.0.1:0")?;
    let second = TcpListener::bind("127.0.0.1:0")?;
    let third = TcpListener::bind("127.0.0.1:0")?;
    let addresses = [
        first.local_addr()?,
        second.local_addr()?,
        third.local_addr()?,
    ];
    Ok((vec![first, second, third], addresses))
}

fn wait_for_cluster(
    node: &mut ClusterRaftConsensusAdapter,
    expected_voters: &BTreeSet<u64>,
    expected_leader: u64,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    loop {
        let progress = node.progress()?;
        if progress.voters == *expected_voters && progress.current_leader == Some(expected_leader) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("cluster membership/leader did not converge: {progress:?}").into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_revision(
    node: &mut ClusterRaftConsensusAdapter,
    expected_revision: u64,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    loop {
        let progress = node.progress()?;
        if progress.broker_revision.get() >= expected_revision
            && progress.applied_index == progress.committed_index
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("follower did not apply committed revision: {progress:?}").into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_survivor_leader(
    node_two: &mut ClusterRaftConsensusAdapter,
    node_three: &mut ClusterRaftConsensusAdapter,
    previous_raft_term: u64,
) -> Result<u64, Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    loop {
        let two = node_two.progress()?;
        let three = node_three.progress()?;
        if two.current_leader == three.current_leader
            && two.raft_term > previous_raft_term
            && three.raft_term > previous_raft_term
            && matches!(two.current_leader, Some(2 | 3))
        {
            return two
                .current_leader
                .ok_or_else(|| "survivor leader disappeared".into());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "survivor election did not converge: node2={two:?}, node3={three:?}"
            )
            .into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_pair_leader(
    node_one: &mut ClusterRaftConsensusAdapter,
    node_two: &mut ClusterRaftConsensusAdapter,
) -> Result<u64, Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    let mut stable_leader = None;
    let mut stable_observations = 0_u8;
    loop {
        let one = node_one.progress()?;
        let two = node_two.progress()?;
        if one.current_leader == two.current_leader && matches!(one.current_leader, Some(1 | 2)) {
            if stable_leader == one.current_leader {
                stable_observations = stable_observations.saturating_add(1);
            } else {
                stable_leader = one.current_leader;
                stable_observations = 1;
            }
            if stable_observations >= 3 {
                return one
                    .current_leader
                    .ok_or_else(|| "surviving pair leader disappeared".into());
            }
        } else {
            stable_leader = None;
            stable_observations = 0;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "surviving pair leader did not converge: node1={one:?}, node2={two:?}"
            )
            .into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_common_running_leader(
    nodes: &mut BTreeMap<u64, ClusterRaftConsensusAdapter>,
) -> Result<u64, Box<dyn Error>> {
    let deadline = Instant::now() + CLUSTER_WAIT;
    let running_ids = nodes.keys().copied().collect::<BTreeSet<_>>();
    let mut stable_leader = None;
    let mut stable_observations = 0_u8;
    loop {
        let mut common_leader = None;
        let mut converged = true;
        for node in nodes.values_mut() {
            let progress = node.progress()?;
            let Some(leader) = progress.current_leader else {
                converged = false;
                break;
            };
            if !running_ids.contains(&leader) {
                converged = false;
                break;
            }
            match common_leader {
                None => common_leader = Some(leader),
                Some(expected) if expected == leader => {}
                Some(_) => {
                    converged = false;
                    break;
                }
            }
        }
        if converged && common_leader.is_some() {
            if stable_leader == common_leader {
                stable_observations = stable_observations.saturating_add(1);
            } else {
                stable_leader = common_leader;
                stable_observations = 1;
            }
            if stable_observations >= 3 {
                return common_leader.ok_or_else(|| "common leader disappeared".into());
            }
        } else {
            stable_leader = None;
            stable_observations = 0;
        }
        if Instant::now() >= deadline {
            return Err(
                format!("running cluster leader did not stabilize: nodes={running_ids:?}").into(),
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_all_running_revisions(
    nodes: &mut BTreeMap<u64, ClusterRaftConsensusAdapter>,
    expected_revision: u64,
) -> Result<(), Box<dyn Error>> {
    for node in nodes.values_mut() {
        wait_for_revision(node, expected_revision)?;
    }
    Ok(())
}

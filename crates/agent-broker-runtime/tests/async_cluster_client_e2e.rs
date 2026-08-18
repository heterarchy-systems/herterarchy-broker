use std::collections::BTreeMap;
use std::error::Error;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use agent_broker_application::{
    CommandIdentity, CommandSequence, CommandSessionId, SessionOwnerEpoch, SessionOwnerInstanceId,
};
use agent_broker_client::{
    AsyncStaticClusterRouter, StaticClusterNode, StaticClusterRouterConfig,
    StaticClusterRoutingRetryPolicy,
};
use agent_broker_domain::{ConsumerGroupId, NamespaceId, TaskId, TaskObjective};
use agent_broker_protocol::{
    BrokerRequest, EnsureConsumerGroupRequest, EnsureNamespaceRequest, PublishTaskRequest,
    RequestId, SuccessPayload,
};
use tempfile::tempdir;
use tokio::net::TcpStream as TokioTcpStream;

#[path = "../../agent-broker-consensus/test_support/tls_fixture.rs"]
mod tls_fixture;

const WAIT: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_FRAME_BYTES: usize = 128 * 1024;

struct ClusterLaunch {
    binary: PathBuf,
    broker_ports: [u16; 3],
    operations_ports: [u16; 3],
    raft_ports: [u16; 3],
    tls_dir: PathBuf,
    state_paths: [PathBuf; 3],
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_async_router_survives_leader_restart_and_preserves_owner_sequence()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let tls_dir = directory.path().join("tls");
    tls_fixture::write_cluster_tls_fixture(&tls_dir, &[1, 2, 3])?;

    let ports = reserve_ports(9)?;
    let launch = ClusterLaunch {
        binary: PathBuf::from(env!("CARGO_BIN_EXE_agentbrokerd")),
        broker_ports: [ports[0], ports[1], ports[2]],
        operations_ports: [ports[3], ports[4], ports[5]],
        raft_ports: [ports[6], ports[7], ports[8]],
        tls_dir,
        state_paths: [
            directory.path().join("node-1.redb"),
            directory.path().join("node-2.redb"),
            directory.path().join("node-3.redb"),
        ],
    };

    let mut children = start_cluster(&launch).await?;
    let router = build_router(launch.broker_ports, launch.operations_ports)?;
    let result = run_failover_scenario(&router, &mut children, &launch).await;

    stop_all(&mut children);
    result
}

async fn start_cluster(launch: &ClusterLaunch) -> Result<BTreeMap<u64, Child>, Box<dyn Error>> {
    let mut children = BTreeMap::new();
    for node_id in [2_u64, 3] {
        let mut child = start_node(launch, node_id)?;
        let index = usize::try_from(node_id - 1)?;
        wait_for_raft_listener(launch.raft_ports[index], &mut child).await?;
        children.insert(node_id, child);
    }
    children.insert(1, start_node(launch, 1)?);
    Ok(children)
}

async fn wait_for_raft_listener(port: u16, child: &mut Child) -> Result<(), Box<dyn Error>> {
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(format!("Broker exited before Raft listener opened: {status}").into());
        }
        if TokioTcpStream::connect(address).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for Raft listener {address}").into());
        }
        tokio::time::sleep(POLL).await;
    }
}

fn build_router(
    broker_ports: [u16; 3],
    operations_ports: [u16; 3],
) -> Result<AsyncStaticClusterRouter, Box<dyn Error>> {
    Ok(AsyncStaticClusterRouter::new(StaticClusterRouterConfig {
        nodes: [
            static_node(1, broker_ports[0], operations_ports[0]),
            static_node(2, broker_ports[1], operations_ports[1]),
            static_node(3, broker_ports[2], operations_ports[2]),
        ],
        timeout: CLIENT_TIMEOUT,
        max_response_frame_bytes: MAX_FRAME_BYTES,
        retry_policy: StaticClusterRoutingRetryPolicy::default(),
    })?)
}

async fn run_failover_scenario(
    router: &AsyncStaticClusterRouter,
    children: &mut BTreeMap<u64, Child>,
    launch: &ClusterLaunch,
) -> Result<(), Box<dyn Error>> {
    let leader = wait_for_leader(router, children, None).await?;
    let initial_leader_id = leader.node_id;

    let session_id = CommandSessionId::new("rust-async-process-e2e-session")?;
    let owner_instance = SessionOwnerInstanceId::new("rust-async-process-e2e-owner")?;
    let owner_epoch = router
        .acquire_command_session_owner(
            session_id.clone(),
            SessionOwnerEpoch::INITIAL,
            owner_instance.clone(),
        )
        .await?;

    let namespace_id = NamespaceId::new("rust-async-process-e2e")?;
    let namespace_request = BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new("rust-async-e2e-namespace-1")?,
        namespace_id: namespace_id.clone(),
    });
    let namespace = router
        .execute_owned(
            &identity(&session_id, owner_epoch, &owner_instance, 1)?,
            &namespace_request,
        )
        .await?;
    assert!(matches!(namespace, SuccessPayload::Namespace { .. }));

    stop_node(children, initial_leader_id)?;
    let failover = wait_for_leader(router, children, Some(initial_leader_id)).await?;
    assert_ne!(failover.node_id, initial_leader_id);

    let task_request = BrokerRequest::PublishTask(PublishTaskRequest {
        request_id: RequestId::new("rust-async-e2e-task-2")?,
        namespace_id: namespace_id.clone(),
        task_id: TaskId::new("rust-async-e2e-task")?,
        objective: TaskObjective::new("prove native Tokio routing after leader failure")?,
    });
    let task = router
        .execute_owned(
            &identity(&session_id, owner_epoch, &owner_instance, 2)?,
            &task_request,
        )
        .await?;
    assert!(matches!(task, SuccessPayload::TaskPublished { .. }));

    children.insert(initial_leader_id, start_node(launch, initial_leader_id)?);
    let _recovered_leader = wait_for_leader(router, children, None).await?;

    let group_request = BrokerRequest::EnsureConsumerGroup(EnsureConsumerGroupRequest {
        request_id: RequestId::new("rust-async-e2e-group-3")?,
        namespace_id,
        group_id: ConsumerGroupId::new("rust-async-e2e-group")?,
    });
    let group = router
        .execute_owned(
            &identity(&session_id, owner_epoch, &owner_instance, 3)?,
            &group_request,
        )
        .await?;
    assert!(matches!(group, SuccessPayload::ConsumerGroup { .. }));
    Ok(())
}

fn stop_node(children: &mut BTreeMap<u64, Child>, node_id: u64) -> Result<(), Box<dyn Error>> {
    let mut stopped = children
        .remove(&node_id)
        .ok_or("discovered leader process was missing")?;
    stopped.kill()?;
    let _status = stopped.wait()?;
    Ok(())
}

fn identity(
    session_id: &CommandSessionId,
    owner_epoch: SessionOwnerEpoch,
    owner_instance: &SessionOwnerInstanceId,
    sequence: u64,
) -> Result<CommandIdentity, Box<dyn Error>> {
    Ok(CommandIdentity::new_with_owner(
        session_id.clone(),
        owner_epoch,
        owner_instance.clone(),
        CommandSequence::new(sequence)?,
    ))
}

async fn wait_for_leader(
    router: &AsyncStaticClusterRouter,
    children: &mut BTreeMap<u64, Child>,
    excluded: Option<u64>,
) -> Result<StaticClusterNode, Box<dyn Error>> {
    let deadline = Instant::now() + WAIT;
    loop {
        for (node_id, child) in children.iter_mut() {
            if let Some(status) = child.try_wait()? {
                return Err(
                    format!("Broker node {node_id} exited before readiness: {status}").into(),
                );
            }
        }
        if let Ok(leader) = router.discover_write_leader().await
            && excluded != Some(leader.node_id)
        {
            return Ok(leader);
        }
        if Instant::now() >= deadline {
            return Err("timed out waiting for the expected async write-ready leader".into());
        }
        tokio::time::sleep(POLL).await;
    }
}

fn start_node(launch: &ClusterLaunch, node_id: u64) -> Result<Child, Box<dyn Error>> {
    let index = usize::try_from(node_id - 1)?;
    let mut command = Command::new(&launch.binary);
    command
        .arg("serve-cluster")
        .arg("--node-id")
        .arg(node_id.to_string())
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(launch.broker_ports[index].to_string())
        .arg("--operations-port")
        .arg(launch.operations_ports[index].to_string())
        .arg("--raft-host")
        .arg("127.0.0.1")
        .arg("--raft-port")
        .arg(launch.raft_ports[index].to_string());
    for peer_id in 1_u64..=3 {
        let peer_index = usize::try_from(peer_id - 1)?;
        command.arg("--raft-node").arg(format!(
            "{peer_id}=127.0.0.1:{}",
            launch.raft_ports[peer_index]
        ));
    }
    command
        .arg("--raft-tls-dir")
        .arg(&launch.tls_dir)
        .arg("--state-path")
        .arg(&launch.state_paths[index])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if node_id == 1 {
        command.arg("--bootstrap");
    }
    Ok(command.spawn()?)
}

fn static_node(node_id: u64, broker_port: u16, operations_port: u16) -> StaticClusterNode {
    StaticClusterNode {
        node_id,
        broker_address: SocketAddr::from((Ipv4Addr::LOCALHOST, broker_port)),
        operations_address: SocketAddr::from((Ipv4Addr::LOCALHOST, operations_port)),
    }
}

fn reserve_ports(count: usize) -> Result<Vec<u16>, Box<dyn Error>> {
    let mut listeners = Vec::with_capacity(count);
    let mut ports = Vec::with_capacity(count);
    for _ in 0..count {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        ports.push(listener.local_addr()?.port());
        listeners.push(listener);
    }
    drop(listeners);
    Ok(ports)
}

fn stop_all(children: &mut BTreeMap<u64, Child>) {
    for child in children.values_mut() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

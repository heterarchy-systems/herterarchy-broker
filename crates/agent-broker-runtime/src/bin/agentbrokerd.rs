use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::io::{self, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use agent_broker_application::{BrokerApplicationService, ConsensusAdapter};
use agent_broker_client::{BrokerClient, BrokerClientConfig};
use agent_broker_consensus::{
    ClusterRaftConfig, ClusterRaftConsensusAdapter, ClusterRaftTlsConfig,
    StandaloneConsensusAdapter,
};
use agent_broker_domain::BrokerCapacityPolicy;
use agent_broker_protocol::BrokerRequestDispatcher;
use agent_broker_runtime::{
    BrokerBindPolicy, BrokerServerConfig, BrokerStateProcessLock, ClusterOperationsObserver,
    LeaderMaintenanceRunner, OperationsBindPolicy, OperationsServer, OperationsServerConfig,
    StandaloneMaintenancePolicy, StandaloneMaintenanceRunner, StateOwnerHandle, TcpBrokerServer,
};
use agent_broker_storage::{JournalCompactionPolicy, JournaledBrokerStateRepository};
use clap::{Args, Parser, Subcommand};
use serde_json::json;

const DEFAULT_PORT: u16 = 8_811;
const DEFAULT_OPERATIONS_PORT: u16 = 8_812;
const DEFAULT_RAFT_PORT: u16 = 18_811;
const DEFAULT_MAX_FRAME_BYTES: usize = 128 * 1024;
const DEFAULT_MAX_CONNECTIONS: usize = 256;
const DEFAULT_MAX_INFLIGHT_REQUESTS: usize = 64;
const DEFAULT_COMPLETED_TASK_RETENTION_SECONDS: u64 = 24 * 60 * 60;
const DEFAULT_MAINTENANCE_INTERVAL_SECONDS: f64 = 5.0;
const DEFAULT_MAINTENANCE_BATCH: usize = 1_024;
const DEFAULT_MEMBER_TIMEOUT_SECONDS: u64 = 45;
const DEFAULT_HEALTH_TIMEOUT_MS: u64 = 2_000;
const MAX_MAINTENANCE_BATCHES_PER_TICK: usize = 4;

#[derive(Debug, Parser)]
#[command(name = "agentbrokerd", about = "Rust Agent Broker standalone runtime")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validate quorum arithmetic without starting the Broker.
    Doctor(DoctorArgs),
    /// Probe a running Broker using the typed protocol health operation.
    Health(HealthArgs),
    /// Start the loopback-only standalone Broker server.
    Serve(ServeArgs),
    /// Start one member of the initial three-node `OpenRaft` cluster.
    ServeCluster(ServeClusterArgs),
}

#[derive(Debug, Args)]
struct DoctorArgs {
    #[arg(long, default_value_t = 1)]
    nodes: usize,
}

#[derive(Debug, Args)]
struct HealthArgs {
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    host: IpAddr,
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
    #[arg(long, default_value_t = DEFAULT_HEALTH_TIMEOUT_MS)]
    timeout_ms: u64,
}

#[derive(Debug, Args)]
struct MaintenanceArgs {
    #[arg(long, default_value_t = DEFAULT_COMPLETED_TASK_RETENTION_SECONDS)]
    completed_task_retention_seconds: u64,
    #[arg(long, default_value_t = DEFAULT_MAINTENANCE_INTERVAL_SECONDS)]
    maintenance_interval_seconds: f64,
    #[arg(long, default_value_t = DEFAULT_MAINTENANCE_BATCH)]
    maintenance_prune_batch: usize,
    #[arg(long, default_value_t = DEFAULT_MEMBER_TIMEOUT_SECONDS)]
    member_timeout_seconds: u64,
    #[arg(long, default_value_t = DEFAULT_MAINTENANCE_BATCH)]
    maintenance_reap_batch: usize,
}

#[derive(Debug, Args)]
struct ServeArgs {
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    host: IpAddr,
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
    /// Permit the configured listener on a container bridge interface.
    #[arg(long, default_value_t = false)]
    container_bridge_bind: bool,
    #[arg(long)]
    state_path: Option<PathBuf>,
    #[arg(long, default_value_t = DEFAULT_MAX_FRAME_BYTES)]
    max_frame_bytes: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_CONNECTIONS)]
    max_connections: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_INFLIGHT_REQUESTS)]
    max_inflight_requests: usize,
    #[command(flatten)]
    maintenance: MaintenanceArgs,
}

#[derive(Debug, Args)]
struct ServeClusterArgs {
    #[arg(long)]
    node_id: u64,
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    host: IpAddr,
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
    #[arg(long, default_value_t = DEFAULT_OPERATIONS_PORT)]
    operations_port: u16,
    #[arg(long, default_value_t = false)]
    container_bridge_bind: bool,
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    raft_host: IpAddr,
    #[arg(long, default_value_t = DEFAULT_RAFT_PORT)]
    raft_port: u16,
    /// Initial cluster node in `node_id=advertised_ip:raft_port` form. Exactly three are required.
    #[arg(long = "raft-node", required = true)]
    raft_nodes: Vec<String>,
    /// Directory containing `ca.pem`, `node-{id}.pem`, and `node-{id}-key.pem` for mandatory Raft mTLS.
    #[arg(long)]
    raft_tls_dir: PathBuf,
    #[arg(long, default_value_t = false)]
    bootstrap: bool,
    #[arg(long)]
    state_path: Option<PathBuf>,
    #[arg(long, default_value_t = DEFAULT_MAX_FRAME_BYTES)]
    max_frame_bytes: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_CONNECTIONS)]
    max_connections: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_INFLIGHT_REQUESTS)]
    max_inflight_requests: usize,
    #[command(flatten)]
    maintenance: MaintenanceArgs,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("agentbrokerd: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    match Cli::parse().command {
        Command::Doctor(args) => doctor(&args),
        Command::Health(args) => health(&args),
        Command::Serve(args) => serve(args),
        Command::ServeCluster(args) => serve_cluster(args),
    }
}

fn doctor(args: &DoctorArgs) -> Result<(), Box<dyn Error>> {
    if args.nodes == 0 {
        return Err("--nodes must be positive".into());
    }
    let majority = args.nodes / 2 + 1;
    let tolerated_failures = args.nodes.saturating_sub(majority);
    println!(
        "{}",
        json!({
            "cluster": {
                "has_high_availability": args.nodes >= 3,
                "has_odd_membership": args.nodes % 2 == 1,
                "majority": majority,
                "node_count": args.nodes,
                "tolerated_failures": tolerated_failures,
            },
            "runtime": {
                "implementation": "Rust",
            },
        })
    );
    Ok(())
}

fn health(args: &HealthArgs) -> Result<(), Box<dyn Error>> {
    if args.timeout_ms == 0 {
        return Err("--timeout-ms must be positive".into());
    }
    let address = SocketAddr::new(args.host, args.port);
    let mut client = BrokerClient::new(BrokerClientConfig {
        address,
        timeout: Duration::from_millis(args.timeout_ms),
        max_response_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
    })?;
    let result = client.health()?;
    println!(
        "{}",
        json!({
            "status": "ok",
            "host": address.ip().to_string(),
            "port": address.port(),
            "term": result.term.get(),
            "revision": result.revision.get(),
            "runtime": "rust",
        })
    );
    Ok(())
}

fn serve(args: ServeArgs) -> Result<(), Box<dyn Error>> {
    let maintenance_policy = maintenance_policy(&args.maintenance)?;
    let state_path = args.state_path.unwrap_or_else(default_state_path);
    let _state_lock = BrokerStateProcessLock::acquire(&state_path)?;
    let repository =
        JournaledBrokerStateRepository::new(state_path, None, JournalCompactionPolicy::default());
    let consensus = StandaloneConsensusAdapter::new(repository)?;
    let initial_term = consensus.term();
    let initial_revision = consensus.revision();
    let service = BrokerApplicationService::new(consensus, BrokerCapacityPolicy::default());
    let dispatcher = BrokerRequestDispatcher::new(service);
    let state_owner = StateOwnerHandle::spawn(dispatcher, args.max_inflight_requests)?;
    let maintenance = StandaloneMaintenanceRunner::new(state_owner.clone(), maintenance_policy);
    let bind_policy = if args.container_bridge_bind {
        BrokerBindPolicy::ContainerBridge
    } else {
        BrokerBindPolicy::LocalOnly
    };
    let server = TcpBrokerServer::bind_with_policy(
        BrokerServerConfig {
            address: SocketAddr::new(args.host, args.port),
            max_frame_bytes: args.max_frame_bytes,
            max_connections: args.max_connections,
            connection_io_timeout: BrokerServerConfig::default().connection_io_timeout,
        },
        state_owner,
        bind_policy,
    )?;
    let address = server.local_addr()?;
    println!(
        "{}",
        json!({
            "event": "broker_ready",
            "host": address.ip().to_string(),
            "port": address.port(),
            "term": initial_term.get(),
            "revision": initial_revision.get(),
            "runtime": "rust",
        })
    );
    io::stdout().flush()?;
    let stop = Arc::new(AtomicBool::new(false));
    let maintenance_handle = maintenance.spawn(Arc::clone(&stop))?;
    let server_result = server.serve_until(stop.as_ref());
    stop.store(true, Ordering::Release);
    if maintenance_handle.join().is_err() {
        return Err("standalone maintenance thread panicked".into());
    }
    server_result?;
    Ok(())
}

fn serve_cluster(args: ServeClusterArgs) -> Result<(), Box<dyn Error>> {
    let maintenance_policy = maintenance_policy(&args.maintenance)?;
    let state_path = args
        .state_path
        .unwrap_or_else(|| default_cluster_state_path(args.node_id));
    let _state_lock = BrokerStateProcessLock::acquire(&state_path)?;
    let nodes = parse_cluster_nodes(&args.raft_nodes)?;
    let consensus = ClusterRaftConsensusAdapter::open(ClusterRaftConfig::new(
        args.node_id,
        &state_path,
        SocketAddr::new(args.raft_host, args.raft_port),
        nodes,
        ClusterRaftTlsConfig::new(&args.raft_tls_dir)?,
        args.bootstrap,
    )?)?;
    let consensus_observer = consensus.observer();
    let initial_term = consensus.term();
    let initial_revision = consensus.revision();
    let service = BrokerApplicationService::new(consensus, BrokerCapacityPolicy::default());
    let dispatcher = BrokerRequestDispatcher::new(service);
    let state_owner = StateOwnerHandle::spawn(dispatcher, args.max_inflight_requests)?;
    let maintenance = LeaderMaintenanceRunner::new(state_owner.clone(), maintenance_policy);
    let bind_policy = if args.container_bridge_bind {
        BrokerBindPolicy::ContainerBridge
    } else {
        BrokerBindPolicy::LocalOnly
    };
    let server = TcpBrokerServer::bind_with_policy(
        BrokerServerConfig {
            address: SocketAddr::new(args.host, args.port),
            max_frame_bytes: args.max_frame_bytes,
            max_connections: args.max_connections,
            connection_io_timeout: BrokerServerConfig::default().connection_io_timeout,
        },
        state_owner.clone(),
        bind_policy,
    )?;
    let server_observer = server.observer();
    let operations_observer =
        ClusterOperationsObserver::new(consensus_observer, state_owner, server_observer);
    let operations_bind_policy = if args.container_bridge_bind {
        OperationsBindPolicy::ContainerBridge
    } else {
        OperationsBindPolicy::LocalOnly
    };
    let operations_server = OperationsServer::bind_with_policy(
        OperationsServerConfig {
            address: SocketAddr::new(args.host, args.operations_port),
            ..OperationsServerConfig::default()
        },
        operations_observer,
        operations_bind_policy,
    )?;
    let address = server.local_addr()?;
    let operations_address = operations_server.local_addr()?;
    println!(
        "{}",
        json!({
            "event": "broker_ready",
            "mode": "cluster",
            "node_id": args.node_id,
            "host": address.ip().to_string(),
            "port": address.port(),
            "operations_host": operations_address.ip().to_string(),
            "operations_port": operations_address.port(),
            "raft_host": args.raft_host.to_string(),
            "raft_port": args.raft_port,
            "bootstrap": args.bootstrap,
            "term": initial_term.get(),
            "revision": initial_revision.get(),
            "runtime": "rust",
        })
    );
    io::stdout().flush()?;
    let stop = Arc::new(AtomicBool::new(false));
    let maintenance_handle = maintenance.spawn(Arc::clone(&stop))?;
    let operations_stop = Arc::clone(&stop);
    let operations_handle = thread::Builder::new()
        .name("agent-broker-operations".to_owned())
        .spawn(move || {
            let result = operations_server.serve_until(operations_stop.as_ref());
            if result.is_err() {
                operations_stop.store(true, Ordering::Release);
            }
            result
        })?;
    let server_result = server.serve_until(stop.as_ref());
    stop.store(true, Ordering::Release);
    if maintenance_handle.join().is_err() {
        return Err("leader maintenance thread panicked".into());
    }
    match operations_handle.join() {
        Ok(result) => result?,
        Err(_) => return Err("operations server thread panicked".into()),
    }
    server_result?;
    Ok(())
}

fn parse_cluster_nodes(values: &[String]) -> Result<BTreeMap<u64, String>, Box<dyn Error>> {
    let mut nodes = BTreeMap::new();
    for value in values {
        let (node_id, address) = value
            .split_once('=')
            .ok_or("--raft-node must use node_id=ip:port format")?;
        let node_id = node_id.parse::<u64>()?;
        if address.trim().is_empty() {
            return Err("--raft-node advertised address must not be empty".into());
        }
        if nodes.insert(node_id, address.to_owned()).is_some() {
            return Err(format!("duplicate --raft-node id: {node_id}").into());
        }
    }
    Ok(nodes)
}

fn maintenance_policy(
    args: &MaintenanceArgs,
) -> Result<StandaloneMaintenancePolicy, Box<dyn Error>> {
    let completed_task_retention_ms = args
        .completed_task_retention_seconds
        .checked_mul(1_000)
        .ok_or("completed task retention milliseconds overflowed")?;
    let member_timeout_ms = args
        .member_timeout_seconds
        .checked_mul(1_000)
        .ok_or("member timeout milliseconds overflowed")?;
    let interval = Duration::try_from_secs_f64(args.maintenance_interval_seconds)
        .map_err(|_| "maintenance interval seconds must be finite and non-negative")?;
    let interval_ms = u64::try_from(interval.as_millis())?;
    Ok(StandaloneMaintenancePolicy::new(
        completed_task_retention_ms,
        member_timeout_ms,
        interval_ms,
        args.maintenance_prune_batch,
        MAX_MAINTENANCE_BATCHES_PER_TICK,
        args.maintenance_reap_batch,
        MAX_MAINTENANCE_BATCHES_PER_TICK,
    )?)
}

fn default_state_path() -> PathBuf {
    env::var_os("HOME").map_or_else(
        || PathBuf::from(".agentbroker/broker-state.json"),
        |home| PathBuf::from(home).join(".agentbroker/broker-state.json"),
    )
}

fn default_cluster_state_path(node_id: u64) -> PathBuf {
    env::var_os("HOME").map_or_else(
        || PathBuf::from(format!(".agentbroker/cluster-node-{node_id}.redb")),
        |home| PathBuf::from(home).join(format!(".agentbroker/cluster-node-{node_id}.redb")),
    )
}

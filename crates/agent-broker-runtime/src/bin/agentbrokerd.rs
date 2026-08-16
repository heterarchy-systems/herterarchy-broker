use std::env;
use std::error::Error;
use std::io::{self, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use agent_broker_application::{BrokerApplicationService, ConsensusAdapter};
use agent_broker_consensus::StandaloneConsensusAdapter;
use agent_broker_domain::BrokerCapacityPolicy;
use agent_broker_protocol::BrokerRequestDispatcher;
use agent_broker_runtime::{
    BrokerServerConfig, BrokerStateProcessLock, StandaloneMaintenancePolicy,
    StandaloneMaintenanceRunner, StateOwnerHandle, TcpBrokerServer,
};
use agent_broker_storage::{JournalCompactionPolicy, JournaledBrokerStateRepository};
use clap::{Args, Parser, Subcommand};
use serde_json::json;

const DEFAULT_PORT: u16 = 8_811;
const DEFAULT_MAX_FRAME_BYTES: usize = 128 * 1024;
const DEFAULT_MAX_CONNECTIONS: usize = 256;
const DEFAULT_MAX_INFLIGHT_REQUESTS: usize = 64;
const DEFAULT_COMPLETED_TASK_RETENTION_SECONDS: u64 = 24 * 60 * 60;
const DEFAULT_MAINTENANCE_INTERVAL_SECONDS: f64 = 5.0;
const DEFAULT_MAINTENANCE_BATCH: usize = 1_024;
const DEFAULT_MEMBER_TIMEOUT_SECONDS: u64 = 45;
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
    /// Start the loopback-only standalone Broker server.
    Serve(ServeArgs),
}

#[derive(Debug, Args)]
struct DoctorArgs {
    #[arg(long, default_value_t = 1)]
    nodes: usize,
}

#[derive(Debug, Args)]
struct ServeArgs {
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    host: IpAddr,
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
    #[arg(long)]
    state_path: Option<PathBuf>,
    #[arg(long, default_value_t = DEFAULT_MAX_FRAME_BYTES)]
    max_frame_bytes: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_CONNECTIONS)]
    max_connections: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_INFLIGHT_REQUESTS)]
    max_inflight_requests: usize,
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

fn main() {
    if let Err(error) = run() {
        eprintln!("agentbrokerd: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    match Cli::parse().command {
        Command::Doctor(args) => doctor(&args),
        Command::Serve(args) => serve(args),
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

fn serve(args: ServeArgs) -> Result<(), Box<dyn Error>> {
    let maintenance_policy = maintenance_policy(&args)?;
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
    let server = TcpBrokerServer::bind(
        BrokerServerConfig {
            address: SocketAddr::new(args.host, args.port),
            max_frame_bytes: args.max_frame_bytes,
            max_connections: args.max_connections,
        },
        state_owner,
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

fn maintenance_policy(args: &ServeArgs) -> Result<StandaloneMaintenancePolicy, Box<dyn Error>> {
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

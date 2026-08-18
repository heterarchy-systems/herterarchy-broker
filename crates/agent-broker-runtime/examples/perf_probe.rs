use std::error::Error;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use agent_broker_application::{BrokerErrorCode, ConsensusAdapter};
use agent_broker_client::{BrokerClient, BrokerClientConfig, ClientError};
use agent_broker_consensus::{
    OneNodeRaftConfig, OneNodeRaftConsensusAdapter, StandaloneConsensusAdapter,
};
use agent_broker_domain::commands::{
    BrokerCommand, ClaimTaskCommand, CompleteTaskCommand, EnsureConsumerGroupCommand,
    EnsureNamespaceCommand, JoinConsumerGroupCommand, PublishTaskCommand,
};
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{
    BrokerStateMachine, Capabilities, ConsumerGroupId, LeaseId, MemberId, NamespaceId, TaskId,
    TaskObjective, TaskResult, Term, TimestampMs,
};
use agent_broker_storage::{
    BrokerStateRepository, JournalCompactionPolicy, JournaledBrokerStateRepository,
};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

const HOT_PUBLISH_COUNT: u32 = 20_000;
const CLAIM_COMPLETE_COUNT: u32 = 5_000;
const DURABLE_PUBLISH_COUNT: u32 = 500;
const ONE_NODE_RAFT_WRITE_COUNT: u32 = 500;
const SNAPSHOT_TASK_COUNT: u32 = 1_000;
const PROTOCOL_LATENCY_SAMPLES: usize = 500;
const QUEUE_SATURATION_CLIENTS: usize = 16;

const MAX_COLD_BOOT_MS: f64 = 750.0;
const MAX_IDLE_RSS_MIB: f64 = 32.0;
const MIN_PUBLISH_OPS_PER_SECOND: f64 = 100_000.0;
const MIN_CLAIM_COMPLETE_PAIRS_PER_SECOND: f64 = 50_000.0;
const MIN_DURABLE_PUBLISH_OPS_PER_SECOND: f64 = 2_000.0;
const MIN_ONE_NODE_RAFT_WRITES_PER_SECOND: f64 = 50.0;
const MAX_PROTOCOL_P99_MS: f64 = 5.0;
const MAX_ONE_NODE_RAFT_WRITE_P99_MS: f64 = 50.0;
const MAX_SNAPSHOT_INSTALL_MS: f64 = 250.0;
const MAX_RECOVERY_MS: f64 = 250.0;
const MAX_QUEUE_SATURATION_MS: f64 = 1_000.0;

#[derive(Debug)]
struct ProcessProbe {
    child: Child,
    address: SocketAddr,
    boot_ms: f64,
    rss_mib: f64,
    _directory: TempDir,
}

impl ProcessProbe {
    fn stop(mut self) -> Result<(), Box<dyn Error>> {
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }
        let _status = self.child.wait()?;
        Ok(())
    }
}

#[derive(Debug)]
struct HotPathMetrics {
    publish_ops_per_second: f64,
    claim_complete_pairs_per_second: f64,
}

#[derive(Debug)]
struct StorageMetrics {
    durable_publish_ops_per_second: f64,
    snapshot_install_ms: f64,
    recovery_ms: f64,
}

#[derive(Debug)]
struct ProtocolMetrics {
    p50: f64,
    p95: f64,
    p99: f64,
    queue_saturation_max: f64,
    queue_saturation_successes: usize,
    queue_saturation_rejections: usize,
}

#[derive(Debug)]
struct OneNodeRaftMetrics {
    committed_write_ops_per_second: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    remote_attempt_count: u64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("Rust Agent Broker perf probe failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let hot = measure_hot_paths()?;
    let storage = measure_storage()?;
    let one_node_raft = measure_one_node_raft()?;
    let process = start_release_daemon()?;
    let protocol = measure_protocol(&process)?;
    let logical_cpus = thread::available_parallelism()?.get();
    let output = json!({
        "environment": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "family": std::env::consts::FAMILY,
            "logical_cpus": logical_cpus,
        },
        "cold_boot_ms": process.boot_ms,
        "idle_rss_mib": process.rss_mib,
        "publish_ops_per_second": hot.publish_ops_per_second,
        "claim_complete_pairs_per_second": hot.claim_complete_pairs_per_second,
        "durable_publish_ops_per_second": storage.durable_publish_ops_per_second,
        "protocol_health_latency_ms": {
            "p50": protocol.p50,
            "p95": protocol.p95,
            "p99": protocol.p99,
        },
        "snapshot_install_ms": storage.snapshot_install_ms,
        "recovery_ms": storage.recovery_ms,
        "one_node_raft": {
            "committed_write_ops_per_second": one_node_raft.committed_write_ops_per_second,
            "committed_write_latency_ms": {
                "p50": one_node_raft.p50,
                "p95": one_node_raft.p95,
                "p99": one_node_raft.p99,
            },
            "remote_attempt_count": one_node_raft.remote_attempt_count,
        },
        "queue_saturation_max_ms": protocol.queue_saturation_max,
        "queue_saturation_successes": protocol.queue_saturation_successes,
        "queue_saturation_rejections": protocol.queue_saturation_rejections,
        "budgets": {
            "max_cold_boot_ms": MAX_COLD_BOOT_MS,
            "max_idle_rss_mib": MAX_IDLE_RSS_MIB,
            "min_publish_ops_per_second": MIN_PUBLISH_OPS_PER_SECOND,
            "min_claim_complete_pairs_per_second": MIN_CLAIM_COMPLETE_PAIRS_PER_SECOND,
            "min_durable_publish_ops_per_second": MIN_DURABLE_PUBLISH_OPS_PER_SECOND,
            "min_one_node_raft_writes_per_second": MIN_ONE_NODE_RAFT_WRITES_PER_SECOND,
            "max_protocol_p99_ms": MAX_PROTOCOL_P99_MS,
            "max_one_node_raft_write_p99_ms": MAX_ONE_NODE_RAFT_WRITE_P99_MS,
            "max_snapshot_install_ms": MAX_SNAPSHOT_INSTALL_MS,
            "max_recovery_ms": MAX_RECOVERY_MS,
            "max_queue_saturation_ms": MAX_QUEUE_SATURATION_MS,
        },
        "profile": "release",
    });
    println!("{output}");
    enforce_budgets(&process, &hot, &storage, &one_node_raft, &protocol)?;
    process.stop()?;
    Ok(())
}

fn measure_one_node_raft() -> Result<OneNodeRaftMetrics, Box<dyn Error>> {
    let directory = tempdir()?;
    let state_path = directory.path().join("one-node-perf.redb");
    let snapshot_interval = u64::from(ONE_NODE_RAFT_WRITE_COUNT) + 1_024;
    let config =
        OneNodeRaftConfig::new(state_path).with_snapshot_log_interval(snapshot_interval)?;
    let mut adapter = OneNodeRaftConsensusAdapter::open(config)?;
    let namespace_id = NamespaceId::new("one-node-perf")?;
    adapter.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: namespace_id.clone(),
        max_namespaces: 64,
    }))?;

    let started = Instant::now();
    let mut latency_micros = Vec::with_capacity(usize::try_from(ONE_NODE_RAFT_WRITE_COUNT)?);
    for index in 0..ONE_NODE_RAFT_WRITE_COUNT {
        let write_started = Instant::now();
        adapter.propose(BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: namespace_id.clone(),
            task_id: TaskId::new(format!("one-node-{index}"))?,
            objective: TaskObjective::new("committed raft write")?,
            created_at_ms: TimestampMs::new(u64::from(index)),
            max_namespace_tasks: usize::try_from(ONE_NODE_RAFT_WRITE_COUNT + 1)?,
        }))?;
        latency_micros.push(write_started.elapsed().as_secs_f64() * 1_000_000.0);
    }
    let committed_write_ops_per_second = rate(ONE_NODE_RAFT_WRITE_COUNT, started.elapsed());
    latency_micros.sort_by(f64::total_cmp);

    let progress = adapter.progress()?;
    if progress.remote_attempt_count != 0 {
        return Err("one-node Raft perf probe attempted a remote RPC".into());
    }
    if progress.applied_index != progress.committed_index {
        return Err("one-node Raft perf probe observed committed/applied divergence".into());
    }
    adapter.shutdown()?;

    Ok(OneNodeRaftMetrics {
        committed_write_ops_per_second,
        p50: percentile(&latency_micros, 50) / 1_000.0,
        p95: percentile(&latency_micros, 95) / 1_000.0,
        p99: percentile(&latency_micros, 99) / 1_000.0,
        remote_attempt_count: progress.remote_attempt_count,
    })
}

fn enforce_budgets(
    process: &ProcessProbe,
    hot: &HotPathMetrics,
    storage: &StorageMetrics,
    one_node_raft: &OneNodeRaftMetrics,
    protocol: &ProtocolMetrics,
) -> Result<(), Box<dyn Error>> {
    require_max("cold boot", process.boot_ms, MAX_COLD_BOOT_MS)?;
    require_max("idle RSS", process.rss_mib, MAX_IDLE_RSS_MIB)?;
    require_min(
        "publish throughput",
        hot.publish_ops_per_second,
        MIN_PUBLISH_OPS_PER_SECOND,
    )?;
    require_min(
        "claim+complete throughput",
        hot.claim_complete_pairs_per_second,
        MIN_CLAIM_COMPLETE_PAIRS_PER_SECOND,
    )?;
    require_min(
        "durable publish throughput",
        storage.durable_publish_ops_per_second,
        MIN_DURABLE_PUBLISH_OPS_PER_SECOND,
    )?;
    require_min(
        "one-node Raft committed write throughput",
        one_node_raft.committed_write_ops_per_second,
        MIN_ONE_NODE_RAFT_WRITES_PER_SECOND,
    )?;
    require_max(
        "one-node Raft committed write p99",
        one_node_raft.p99,
        MAX_ONE_NODE_RAFT_WRITE_P99_MS,
    )?;
    require_max("protocol p99", protocol.p99, MAX_PROTOCOL_P99_MS)?;
    require_max(
        "snapshot install",
        storage.snapshot_install_ms,
        MAX_SNAPSHOT_INSTALL_MS,
    )?;
    require_max("recovery", storage.recovery_ms, MAX_RECOVERY_MS)?;
    require_max(
        "queue saturation",
        protocol.queue_saturation_max,
        MAX_QUEUE_SATURATION_MS,
    )
}

fn require_min(label: &str, actual: f64, minimum: f64) -> Result<(), Box<dyn Error>> {
    if actual < minimum {
        return Err(format!(
            "Rust Broker {label} regression: actual={actual:.3}, minimum={minimum:.3}"
        )
        .into());
    }
    Ok(())
}

fn require_max(label: &str, actual: f64, maximum: f64) -> Result<(), Box<dyn Error>> {
    if actual > maximum {
        return Err(format!(
            "Rust Broker {label} regression: actual={actual:.3}, maximum={maximum:.3}"
        )
        .into());
    }
    Ok(())
}

fn measure_hot_paths() -> Result<HotPathMetrics, Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let namespace_id = NamespaceId::new("perf")?;
    machine.apply(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: namespace_id.clone(),
        max_namespaces: 64,
    }))?;

    let publish_start = Instant::now();
    for index in 0..HOT_PUBLISH_COUNT {
        machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: namespace_id.clone(),
            task_id: TaskId::new(format!("task-{index}"))?,
            objective: TaskObjective::new("perf")?,
            created_at_ms: TimestampMs::new(u64::from(index)),
            max_namespace_tasks: usize::try_from(HOT_PUBLISH_COUNT + 1)?,
        }))?;
    }
    let publish_ops_per_second = rate(HOT_PUBLISH_COUNT, publish_start.elapsed());

    let group_id = ConsumerGroupId::new("workers")?;
    machine.apply(BrokerCommand::EnsureConsumerGroup(
        EnsureConsumerGroupCommand {
            namespace_id,
            group_id: group_id.clone(),
            max_namespace_groups: 64,
        },
    ))?;
    let member_id = MemberId::new("worker-1")?;
    let joined = machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
        group_id: group_id.clone(),
        member_id: member_id.clone(),
        capabilities: Capabilities::new(["perf"])?,
        now_ms: TimestampMs::new(1_000_000),
        max_group_members: 256,
    }))?;
    let BrokerMutationResult::ConsumerGroup(joined) = joined.result else {
        return Err("join result must be ConsumerGroup".into());
    };

    let lifecycle_start = Instant::now();
    for index in 0..CLAIM_COMPLETE_COUNT {
        let lease_id = LeaseId::new(format!("lease-{index}"))?;
        let now_ms = TimestampMs::new(2_000_000 + u64::from(index) * 2);
        let claimed = machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: joined.generation,
            lease_id: lease_id.clone(),
            now_ms,
            lease_duration_ms: 60_000,
        }))?;
        let BrokerMutationResult::TaskClaim(claimed) = claimed.result else {
            return Err("claim result must be TaskClaim".into());
        };
        let task_id = claimed.task_id.ok_or("perf claim returned no Task")?;
        let lease_epoch = claimed
            .lease_epoch
            .ok_or("perf claim returned no lease epoch")?;
        machine.apply(BrokerCommand::CompleteTask(CompleteTaskCommand {
            task_id,
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: joined.generation,
            expected_lease_epoch: lease_epoch,
            lease_id,
            result: TaskResult::new("done")?,
            completed_at_ms: TimestampMs::new(now_ms.get() + 1),
        }))?;
    }
    let claim_complete_pairs_per_second = rate(CLAIM_COMPLETE_COUNT, lifecycle_start.elapsed());
    Ok(HotPathMetrics {
        publish_ops_per_second,
        claim_complete_pairs_per_second,
    })
}

fn measure_storage() -> Result<StorageMetrics, Box<dyn Error>> {
    let durable_directory = tempdir()?;
    let repository = JournaledBrokerStateRepository::new(
        durable_directory.path().join("broker-state.json"),
        None,
        JournalCompactionPolicy::new(10_000, 64 * 1024 * 1024)?,
    );
    let mut adapter = StandaloneConsensusAdapter::new(repository)?;
    adapter.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("durable")?,
        max_namespaces: 64,
    }))?;
    let durable_start = Instant::now();
    for index in 0..DURABLE_PUBLISH_COUNT {
        adapter.propose(BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: NamespaceId::new("durable")?,
            task_id: TaskId::new(format!("durable-{index}"))?,
            objective: TaskObjective::new("fsync")?,
            created_at_ms: TimestampMs::new(u64::from(index)),
            max_namespace_tasks: usize::try_from(DURABLE_PUBLISH_COUNT + 1)?,
        }))?;
    }
    let durable_publish_ops_per_second = rate(DURABLE_PUBLISH_COUNT, durable_start.elapsed());

    let snapshot_directory = tempdir()?;
    let snapshot_path = snapshot_directory.path().join("broker-state.json");
    let mut machine = BrokerStateMachine::default();
    let namespace_id = NamespaceId::new("snapshot")?;
    machine.apply(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: namespace_id.clone(),
        max_namespaces: 64,
    }))?;
    for index in 0..SNAPSHOT_TASK_COUNT - 1 {
        machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: namespace_id.clone(),
            task_id: TaskId::new(format!("snapshot-{index}"))?,
            objective: TaskObjective::new("snapshot")?,
            created_at_ms: TimestampMs::new(u64::from(index)),
            max_namespace_tasks: usize::try_from(SNAPSHOT_TASK_COUNT + 1)?,
        }))?;
    }
    let last_index = SNAPSHOT_TASK_COUNT - 1;
    let last_applied = machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
        namespace_id: namespace_id.clone(),
        task_id: TaskId::new(format!("snapshot-{last_index}"))?,
        objective: TaskObjective::new("snapshot")?,
        created_at_ms: TimestampMs::new(u64::from(last_index)),
        max_namespace_tasks: usize::try_from(SNAPSHOT_TASK_COUNT + 1)?,
    }))?;
    let mut repository = JournaledBrokerStateRepository::new(
        snapshot_path.clone(),
        None,
        JournalCompactionPolicy::new(1, 64 * 1024 * 1024)?,
    );
    let snapshot_start = Instant::now();
    repository.commit(machine.state(), &last_applied.changes)?;
    let snapshot_install_ms = millis(snapshot_start.elapsed());
    drop(repository);

    let recovery_start = Instant::now();
    let mut repository = JournaledBrokerStateRepository::new(
        snapshot_path,
        None,
        JournalCompactionPolicy::new(10_000, 64 * 1024 * 1024)?,
    );
    let checkpoint = repository.load()?;
    let restored = BrokerStateMachine::from_checkpoint(checkpoint)?;
    let recovery_ms = millis(recovery_start.elapsed());
    if restored.state().task_count() != usize::try_from(SNAPSHOT_TASK_COUNT)? {
        return Err("snapshot recovery task count mismatch".into());
    }
    Ok(StorageMetrics {
        durable_publish_ops_per_second,
        snapshot_install_ms,
        recovery_ms,
    })
}

fn start_release_daemon() -> Result<ProcessProbe, Box<dyn Error>> {
    let root = workspace_root()?;
    let executable = root.join("target/release/agentbrokerd");
    if !executable.is_file() {
        return Err(format!("release agentbrokerd is missing: {}", executable.display()).into());
    }
    let directory = tempdir()?;
    let state_path = directory.path().join("broker-state.json");
    let start = Instant::now();
    let mut child = Command::new(executable)
        .args([
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            "0",
            "--max-inflight-requests",
            "4",
            "--state-path",
        ])
        .arg(state_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or("release agentbrokerd stdout unavailable")?;
    let mut reader = BufReader::new(stdout);
    let mut ready_line = String::new();
    if reader.read_line(&mut ready_line)? == 0 {
        return Err("release agentbrokerd exited before ready event".into());
    }
    let ready: Value = serde_json::from_str(&ready_line)?;
    let host = ready
        .get("host")
        .and_then(Value::as_str)
        .ok_or("ready host missing")?
        .parse::<IpAddr>()?;
    let port = ready
        .get("port")
        .and_then(Value::as_u64)
        .ok_or("ready port missing")?;
    let port = u16::try_from(port)?;
    let boot_ms = millis(start.elapsed());
    let rss_mib = process_rss_mib(child.id())?;
    Ok(ProcessProbe {
        child,
        address: SocketAddr::new(host, port),
        boot_ms,
        rss_mib,
        _directory: directory,
    })
}

fn measure_protocol(process: &ProcessProbe) -> Result<ProtocolMetrics, Box<dyn Error>> {
    let config = BrokerClientConfig {
        address: process.address,
        timeout: Duration::from_secs(5),
        max_response_frame_bytes: 128 * 1024,
    };
    let mut client = BrokerClient::new(config)?;
    let mut latency_micros = Vec::with_capacity(PROTOCOL_LATENCY_SAMPLES);
    for _ in 0..PROTOCOL_LATENCY_SAMPLES {
        let start = Instant::now();
        client.health()?;
        latency_micros.push(start.elapsed().as_secs_f64() * 1_000_000.0);
    }
    latency_micros.sort_by(f64::total_cmp);
    let p50 = percentile(&latency_micros, 50) / 1_000.0;
    let p95 = percentile(&latency_micros, 95) / 1_000.0;
    let p99 = percentile(&latency_micros, 99) / 1_000.0;

    let namespace_id = NamespaceId::new("queue-perf")?;
    client.ensure_namespace(namespace_id.clone())?;
    let barrier = Arc::new(Barrier::new(QUEUE_SATURATION_CLIENTS));
    let mut handles = Vec::with_capacity(QUEUE_SATURATION_CLIENTS);
    for index in 0..QUEUE_SATURATION_CLIENTS {
        let barrier = Arc::clone(&barrier);
        let namespace_id = namespace_id.clone();
        handles.push(thread::spawn(move || -> Result<(f64, bool), String> {
            let mut client = BrokerClient::new(config).map_err(|error| error.to_string())?;
            barrier.wait();
            let start = Instant::now();
            let result = client.publish_task(
                namespace_id,
                TaskId::new(format!("queue-{index}")).map_err(|error| error.to_string())?,
                TaskObjective::new("queue saturation").map_err(|error| error.to_string())?,
            );
            let rejected = match result {
                Ok(_) => false,
                Err(ClientError::Broker(error))
                    if error.code() == BrokerErrorCode::CapacityExceeded =>
                {
                    true
                }
                Err(error) => return Err(error.to_string()),
            };
            Ok((millis(start.elapsed()), rejected))
        }));
    }
    let mut queue_saturation_max = 0.0_f64;
    let mut queue_saturation_successes = 0_usize;
    let mut queue_saturation_rejections = 0_usize;
    for handle in handles {
        let result = handle
            .join()
            .map_err(|_| "queue saturation worker panicked")?;
        let (elapsed_ms, rejected) =
            result.map_err(|error| format!("queue saturation worker failed: {error}"))?;
        queue_saturation_max = queue_saturation_max.max(elapsed_ms);
        if rejected {
            queue_saturation_rejections += 1;
        } else {
            queue_saturation_successes += 1;
        }
    }
    if queue_saturation_successes + queue_saturation_rejections != QUEUE_SATURATION_CLIENTS {
        return Err("queue saturation probe did not classify every worker outcome".into());
    }
    client.health()?;
    Ok(ProtocolMetrics {
        p50,
        p95,
        p99,
        queue_saturation_max,
        queue_saturation_successes,
        queue_saturation_rejections,
    })
}

fn workspace_root() -> Result<PathBuf, Box<dyn Error>> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| "workspace root could not be resolved".into())
}

fn process_rss_mib(pid: u32) -> Result<f64, Box<dyn Error>> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()?;
    if !output.status.success() {
        return Err("ps failed while reading agentbrokerd RSS".into());
    }
    let kib = String::from_utf8(output.stdout)?.trim().parse::<f64>()?;
    Ok(kib / 1024.0)
}

fn percentile(sorted_micros: &[f64], percentile: usize) -> f64 {
    let last_index = sorted_micros.len().saturating_sub(1);
    let index = last_index.saturating_mul(percentile).div_ceil(100);
    sorted_micros[index.min(last_index)]
}

fn rate(count: u32, elapsed: Duration) -> f64 {
    f64::from(count) / elapsed.as_secs_f64()
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

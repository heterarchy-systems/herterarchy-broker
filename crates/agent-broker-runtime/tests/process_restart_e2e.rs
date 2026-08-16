use std::error::Error;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use agent_broker_client::{BrokerClient, BrokerClientConfig, ClaimInput, CompleteInput};
use agent_broker_domain::{
    ConsumerGroupId, Generation, LeaseDurationMs, LeaseEpoch, LeaseId, MemberId, NamespaceId,
    Revision, TaskId, TaskObjective, TaskResult, TaskStatus, Term,
};
use agent_broker_protocol::DeclaredCapabilities;
use serde_json::Value;
use tempfile::tempdir;

struct RunningBroker {
    child: Child,
    client: BrokerClient,
}

impl RunningBroker {
    fn crash(mut self) -> Result<(), Box<dyn Error>> {
        self.client.close();
        self.child.kill()?;
        let _status = self.child.wait()?;
        Ok(())
    }
}

fn start_broker(state_path: &Path) -> Result<RunningBroker, Box<dyn Error>> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_agentbrokerd"))
        .arg("serve")
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg("0")
        .arg("--state-path")
        .arg(state_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or("agentbrokerd stdout was not piped")?;
    let mut reader = BufReader::new(stdout);
    let mut ready_line = String::new();
    if reader.read_line(&mut ready_line)? == 0 {
        return Err("agentbrokerd exited before emitting broker_ready".into());
    }
    let ready: Value = serde_json::from_str(&ready_line)?;
    if ready.get("event").and_then(Value::as_str) != Some("broker_ready") {
        return Err(format!("unexpected agentbrokerd ready event: {ready_line}").into());
    }
    if ready.get("runtime").and_then(Value::as_str) != Some("rust") {
        return Err("agentbrokerd did not report Rust runtime".into());
    }
    let host = ready
        .get("host")
        .and_then(Value::as_str)
        .ok_or("broker_ready host missing")?
        .parse::<IpAddr>()?;
    let port = ready
        .get("port")
        .and_then(Value::as_u64)
        .ok_or("broker_ready port missing")?;
    let port = u16::try_from(port)?;
    let client = BrokerClient::new(BrokerClientConfig {
        address: SocketAddr::new(host, port),
        timeout: Duration::from_secs(3),
        max_response_frame_bytes: 128 * 1024,
    })?;
    Ok(RunningBroker { child, client })
}

#[test]
fn acked_mutations_survive_hard_process_kill_and_restart() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let state_path = directory.path().join("broker-state.json");
    let mut first = start_broker(&state_path)?;

    let initial = first.client.health()?;
    assert_eq!(initial.term, Term::INITIAL);
    assert_eq!(initial.revision, Revision::new(0));
    let namespace_id = NamespaceId::new("project-a")?;
    first.client.ensure_namespace(namespace_id.clone())?;
    let task_id = TaskId::new("task-1")?;
    first.client.publish_task(
        namespace_id.clone(),
        task_id.clone(),
        TaskObjective::new("survive hard process kill")?,
    )?;
    let group_id = ConsumerGroupId::new("engineering")?;
    first
        .client
        .ensure_consumer_group(namespace_id, group_id.clone())?;
    let member_id = MemberId::new("worker-a")?;
    let joined = first.client.join_consumer_group(
        group_id.clone(),
        member_id.clone(),
        DeclaredCapabilities::new(["code", "review"])?,
    )?;
    assert_eq!(joined.generation, Generation::new(1));
    let lease_id = LeaseId::new("lease-1")?;
    let claimed = first.client.claim_task(ClaimInput {
        group_id: group_id.clone(),
        member_id: member_id.clone(),
        expected_term: initial.term,
        expected_generation: joined.generation,
        lease_id: lease_id.clone(),
        lease_duration: LeaseDurationMs::new(60_000)?,
    })?;
    let Some(lease_epoch) = claimed.lease_epoch else {
        return Err("claim must contain lease epoch".into());
    };
    assert_eq!(lease_epoch, LeaseEpoch::new(1));
    let completed = first.client.complete_task(CompleteInput {
        task_id: task_id.clone(),
        group_id: group_id.clone(),
        member_id: member_id.clone(),
        expected_term: initial.term,
        expected_generation: joined.generation,
        expected_lease_epoch: lease_epoch,
        lease_id: lease_id.clone(),
        result: TaskResult::new("done")?,
    })?;
    assert_eq!(completed.status, TaskStatus::Completed);
    let committed_revision = first.client.health()?.revision;
    assert!(committed_revision.get() >= 6);
    first.crash()?;

    let mut second = start_broker(&state_path)?;
    let recovered_health = second.client.health()?;
    assert_eq!(recovered_health.term, initial.term);
    assert_eq!(recovered_health.revision, committed_revision);
    let idempotent = second.client.complete_task(CompleteInput {
        task_id,
        group_id,
        member_id,
        expected_term: initial.term,
        expected_generation: joined.generation,
        expected_lease_epoch: lease_epoch,
        lease_id,
        result: TaskResult::new("done")?,
    })?;
    assert_eq!(idempotent.status, TaskStatus::Completed);
    assert_eq!(second.client.health()?.revision, committed_revision);
    second.crash()?;
    Ok(())
}

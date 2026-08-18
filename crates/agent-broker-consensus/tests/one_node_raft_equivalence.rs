use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use agent_broker_application::ConsensusAdapter;
use agent_broker_consensus::{
    OneNodeRaftConfig, OneNodeRaftConsensusAdapter, StandaloneConsensusAdapter,
};
use agent_broker_domain::commands::{
    AdvanceTermCommand, BrokerCommand, ClaimTaskCommand, CompleteTaskCommand,
    EnsureConsumerGroupCommand, EnsureNamespaceCommand, JoinConsumerGroupCommand,
    PublishTaskCommand, RenewTaskLeaseCommand,
};
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{
    BrokerCheckpoint, BrokerState, Capabilities, ConsumerGroupId, ConsumerId, Generation,
    LeaseEpoch, LeaseId, NamespaceId, TaskId, TaskObjective, TaskResult, Term, TimestampMs,
};
use agent_broker_storage::{BrokerStateRepository, RepositoryError};
use tempfile::tempdir;

#[derive(Debug)]
struct MemoryRepository {
    checkpoint: BrokerCheckpoint,
}

impl Default for MemoryRepository {
    fn default() -> Self {
        Self {
            checkpoint: BrokerState::default().checkpoint(),
        }
    }
}

impl BrokerStateRepository for MemoryRepository {
    fn load(&mut self) -> Result<BrokerCheckpoint, RepositoryError> {
        Ok(self.checkpoint.clone())
    }

    fn commit(
        &mut self,
        state: &BrokerState,
        _changes: &agent_broker_domain::results::StateChangeSet,
    ) -> Result<(), RepositoryError> {
        self.checkpoint = state.checkpoint();
        Ok(())
    }
}

#[test]
fn one_node_raft_matches_standalone_lifecycle_semantics() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let raft_path = directory.path().join("one-node.redb");
    let mut raft = OneNodeRaftConsensusAdapter::open(
        OneNodeRaftConfig::new(&raft_path).with_snapshot_log_interval(64)?,
    )?;
    let mut standalone = StandaloneConsensusAdapter::new(MemoryRepository::default())?;

    synchronize_standalone_term(&mut standalone, raft.term())?;
    assert_eq!(standalone.term(), raft.term());
    assert_eq!(standalone.revision(), raft.revision());

    let commands = lifecycle_commands(raft.term())?;
    for command in commands {
        let standalone_result = standalone.propose(command.clone())?;
        let raft_result = raft.propose(command)?;
        assert_eq!(raft_result, standalone_result);
        assert_eq!(raft.term(), standalone.term());
        assert_eq!(raft.revision(), standalone.revision());
    }

    let progress = raft.progress()?;
    assert_eq!(progress.current_leader, Some(1));
    assert_eq!(progress.remote_attempt_count, 0);
    assert_eq!(progress.broker_term, raft.term());
    assert_eq!(progress.broker_revision, raft.revision());
    assert_eq!(progress.raft_term, progress.broker_term.get());
    assert_eq!(progress.applied_index, progress.committed_index);
    assert!(
        progress
            .last_log_index
            .zip(progress.committed_index)
            .is_some_and(|(last, committed)| last >= committed)
    );

    raft.shutdown()?;
    Ok(())
}

#[test]
fn one_node_raft_snapshot_restart_recovers_state_and_fences_old_term() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let raft_path = directory.path().join("one-node-restart.redb");
    let config = OneNodeRaftConfig::new(&raft_path).with_snapshot_log_interval(4)?;

    let mut raft = OneNodeRaftConsensusAdapter::open(config.clone())?;
    let fixture = prepare_restart_fixture(&mut raft)?;

    let before_snapshot = raft.progress()?;
    let snapshotted = raft.trigger_snapshot()?;
    assert_eq!(snapshotted.remote_attempt_count, 0);
    assert_eq!(snapshotted.applied_index, snapshotted.committed_index);
    assert!(snapshotted.broker_revision >= before_snapshot.broker_revision);
    raft.shutdown()?;

    let mut restarted = OneNodeRaftConsensusAdapter::open(config)?;
    let recovered = restarted.progress()?;
    assert_eq!(recovered.current_leader, Some(1));
    assert_eq!(recovered.remote_attempt_count, 0);
    assert_eq!(recovered.broker_term, restarted.term());
    assert_eq!(recovered.broker_revision, restarted.revision());
    assert_eq!(recovered.applied_index, recovered.committed_index);
    assert!(restarted.revision() >= snapshotted.broker_revision);

    verify_restarted_lease_lifecycle(&mut restarted, &fixture)?;

    restarted.shutdown()?;
    Ok(())
}

struct RestartFixture {
    initial_term: Term,
    task_id: TaskId,
    group_id: ConsumerGroupId,
    member_id: ConsumerId,
    lease_id: LeaseId,
    generation: Generation,
    lease_epoch: LeaseEpoch,
}

fn prepare_restart_fixture(
    raft: &mut OneNodeRaftConsensusAdapter,
) -> Result<RestartFixture, Box<dyn Error>> {
    let initial_term = raft.term();
    let namespace_id = NamespaceId::new("project-restart")?;
    let task_id = TaskId::new("task-before-restart")?;
    let group_id = ConsumerGroupId::new("workers-restart")?;
    let member_id = ConsumerId::new("worker-restart")?;
    let lease_id = LeaseId::new("lease-before-restart")?;

    raft.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: namespace_id.clone(),
        max_namespaces: 64,
    }))?;
    raft.propose(BrokerCommand::PublishTask(PublishTaskCommand {
        namespace_id: namespace_id.clone(),
        task_id: task_id.clone(),
        objective: TaskObjective::new("survive raft restart")?,
        created_at_ms: TimestampMs::new(1_000),
        max_namespace_tasks: 4_096,
    }))?;
    raft.propose(BrokerCommand::EnsureConsumerGroup(
        EnsureConsumerGroupCommand {
            namespace_id,
            group_id: group_id.clone(),
            max_namespace_groups: 64,
        },
    ))?;
    let join = raft.propose(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
        group_id: group_id.clone(),
        member_id: member_id.clone(),
        capabilities: Capabilities::new(["rust", "restart"])?,
        now_ms: TimestampMs::new(1_100),
        max_group_members: 256,
    }))?;
    let generation = match join {
        BrokerMutationResult::ConsumerGroup(result) => result.generation,
        other => return Err(format!("unexpected join result: {other:?}").into()),
    };
    let claim = raft.propose(BrokerCommand::ClaimTask(ClaimTaskCommand {
        group_id: group_id.clone(),
        member_id: member_id.clone(),
        expected_term: initial_term,
        expected_generation: generation,
        lease_id: lease_id.clone(),
        now_ms: TimestampMs::new(1_200),
        lease_duration_ms: 30_000,
    }))?;
    let lease_epoch = match claim {
        BrokerMutationResult::TaskClaim(result) => result
            .lease_epoch
            .ok_or("expected the queued task to be leased before restart")?,
        other => return Err(format!("unexpected claim result: {other:?}").into()),
    };

    Ok(RestartFixture {
        initial_term,
        task_id,
        group_id,
        member_id,
        lease_id,
        generation,
        lease_epoch,
    })
}

fn verify_restarted_lease_lifecycle(
    restarted: &mut OneNodeRaftConsensusAdapter,
    fixture: &RestartFixture,
) -> Result<(), Box<dyn Error>> {
    if restarted.term() != fixture.initial_term {
        let stale = restarted.propose(BrokerCommand::RenewTaskLease(RenewTaskLeaseCommand {
            task_id: fixture.task_id.clone(),
            group_id: fixture.group_id.clone(),
            member_id: fixture.member_id.clone(),
            expected_term: fixture.initial_term,
            expected_generation: fixture.generation,
            expected_lease_epoch: fixture.lease_epoch,
            lease_id: fixture.lease_id.clone(),
            now_ms: TimestampMs::new(1_300),
            lease_duration_ms: 30_000,
        }));
        let stale = match stale {
            Err(error) => error,
            Ok(result) => {
                return Err(
                    format!("old Raft/Broker term unexpectedly renewed lease: {result:?}").into(),
                );
            }
        };
        assert_eq!(
            stale.code(),
            agent_broker_application::BrokerErrorCode::StaleFence
        );
    }

    let renewed = restarted.propose(BrokerCommand::RenewTaskLease(RenewTaskLeaseCommand {
        task_id: fixture.task_id.clone(),
        group_id: fixture.group_id.clone(),
        member_id: fixture.member_id.clone(),
        expected_term: restarted.term(),
        expected_generation: fixture.generation,
        expected_lease_epoch: fixture.lease_epoch,
        lease_id: fixture.lease_id.clone(),
        now_ms: TimestampMs::new(1_300),
        lease_duration_ms: 30_000,
    }))?;
    let renewed_epoch = match renewed {
        BrokerMutationResult::TaskLeaseRenewed(result) => {
            assert_eq!(&result.task_id, &fixture.task_id);
            result.lease_epoch
        }
        other => return Err(format!("unexpected renewed result: {other:?}").into()),
    };
    assert_eq!(renewed_epoch, fixture.lease_epoch);

    let completed = restarted.propose(BrokerCommand::CompleteTask(CompleteTaskCommand {
        task_id: fixture.task_id.clone(),
        group_id: fixture.group_id.clone(),
        member_id: fixture.member_id.clone(),
        expected_term: restarted.term(),
        expected_generation: fixture.generation,
        expected_lease_epoch: fixture.lease_epoch,
        lease_id: fixture.lease_id.clone(),
        result: TaskResult::new("recovered and completed")?,
        completed_at_ms: TimestampMs::new(1_400),
    }))?;
    match completed {
        BrokerMutationResult::TaskCompleted(result) => {
            assert_eq!(result.status, agent_broker_domain::TaskStatus::Completed);
        }
        other => return Err(format!("unexpected completion result: {other:?}").into()),
    }

    Ok(())
}

#[test]
fn one_node_raft_acked_mutations_survive_hard_process_kill() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let raft_path = directory.path().join("one-node-hard-kill.redb");
    let ready_path = directory.path().join("child-ready.txt");

    let mut child = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("one_node_raft_process_child")
        .arg("--nocapture")
        .env("AGENT_BROKER_RAFT_CHILD", "1")
        .env("AGENT_BROKER_RAFT_STATE_PATH", &raft_path)
        .env("AGENT_BROKER_RAFT_READY_PATH", &ready_path)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;

    if let Err(error) = wait_for_child_ready(&mut child, &ready_path, Duration::from_secs(8)) {
        let _kill_result = child.kill();
        let _wait_result = child.wait();
        return Err(error);
    }

    let ready = std::fs::read_to_string(&ready_path)?;
    let mut fields = ready.split_ascii_whitespace();
    let child_term = Term::new(
        fields
            .next()
            .ok_or("child readiness marker is missing term")?
            .parse::<u64>()?,
    )?;
    let child_revision = fields
        .next()
        .ok_or("child readiness marker is missing revision")?
        .parse::<u64>()?;
    if fields.next().is_some() {
        return Err("child readiness marker contains unexpected fields".into());
    }

    child.kill()?;
    let _status = child.wait()?;

    let mut restarted = OneNodeRaftConsensusAdapter::open(OneNodeRaftConfig::new(&raft_path))?;
    let recovered_before_retry = restarted.progress()?;
    assert_eq!(recovered_before_retry.current_leader, Some(1));
    assert_eq!(recovered_before_retry.remote_attempt_count, 0);
    assert!(recovered_before_retry.broker_revision.get() >= child_revision);
    assert!(restarted.term() >= child_term);

    let revision_before_retry = restarted.revision();
    let namespace_id = NamespaceId::new("hard-kill-project")?;
    let task_id = TaskId::new("hard-kill-task")?;
    restarted.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: namespace_id.clone(),
        max_namespaces: 64,
    }))?;
    restarted.propose(BrokerCommand::PublishTask(PublishTaskCommand {
        namespace_id,
        task_id,
        objective: TaskObjective::new("survive hard kill after ack")?,
        created_at_ms: TimestampMs::new(9_000),
        max_namespace_tasks: 4_096,
    }))?;
    assert_eq!(restarted.revision(), revision_before_retry);

    let recovered_after_retry = restarted.progress()?;
    assert_eq!(
        recovered_after_retry.applied_index,
        recovered_after_retry.committed_index
    );
    assert_eq!(recovered_after_retry.remote_attempt_count, 0);
    restarted.shutdown()?;
    Ok(())
}

#[test]
fn one_node_raft_process_child() -> Result<(), Box<dyn Error>> {
    if std::env::var_os("AGENT_BROKER_RAFT_CHILD").is_none() {
        return Ok(());
    }

    let raft_path = std::env::var_os("AGENT_BROKER_RAFT_STATE_PATH")
        .ok_or("AGENT_BROKER_RAFT_STATE_PATH is missing")?;
    let ready_path = std::env::var_os("AGENT_BROKER_RAFT_READY_PATH")
        .ok_or("AGENT_BROKER_RAFT_READY_PATH is missing")?;
    let mut raft = OneNodeRaftConsensusAdapter::open(OneNodeRaftConfig::new(raft_path))?;
    let namespace_id = NamespaceId::new("hard-kill-project")?;
    raft.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: namespace_id.clone(),
        max_namespaces: 64,
    }))?;
    raft.propose(BrokerCommand::PublishTask(PublishTaskCommand {
        namespace_id,
        task_id: TaskId::new("hard-kill-task")?,
        objective: TaskObjective::new("survive hard kill after ack")?,
        created_at_ms: TimestampMs::new(9_000),
        max_namespace_tasks: 4_096,
    }))?;
    let progress = raft.progress()?;
    assert_eq!(progress.applied_index, progress.committed_index);
    assert_eq!(progress.remote_attempt_count, 0);

    let mut ready_file = File::create(ready_path)?;
    writeln!(
        ready_file,
        "{} {}",
        raft.term().get(),
        raft.revision().get()
    )?;
    ready_file.sync_all()?;

    loop {
        thread::park_timeout(Duration::from_mins(1));
    }
}

fn wait_for_child_ready(
    child: &mut Child,
    ready_path: &Path,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        if ready_path.is_file() {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            return Err(format!("one-node Raft child exited before readiness: {status}").into());
        }
        if Instant::now() >= deadline {
            return Err("one-node Raft child did not become ready before timeout".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn synchronize_standalone_term(
    standalone: &mut StandaloneConsensusAdapter<MemoryRepository>,
    target: Term,
) -> Result<(), Box<dyn Error>> {
    while standalone.term() < target {
        let current = standalone.term();
        let next = Term::new(current.get().checked_add(1).ok_or("term overflow")?)?;
        standalone.propose(BrokerCommand::AdvanceTerm(AdvanceTermCommand {
            expected_term: current,
            new_term: next,
        }))?;
    }
    Ok(())
}

fn lifecycle_commands(term: Term) -> Result<Vec<BrokerCommand>, Box<dyn Error>> {
    let namespace_id = NamespaceId::new("project-a")?;
    let task_id = TaskId::new("task-1")?;
    let group_id = ConsumerGroupId::new("workers")?;
    let member_id = ConsumerId::new("worker-1")?;
    let lease_id = LeaseId::new("lease-1")?;
    let generation = Generation::new(1);
    let lease_epoch = LeaseEpoch::new(1);

    Ok(vec![
        BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: namespace_id.clone(),
            max_namespaces: 64,
        }),
        BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: namespace_id.clone(),
            task_id: task_id.clone(),
            objective: TaskObjective::new("one node raft parity")?,
            created_at_ms: TimestampMs::new(1_000),
            max_namespace_tasks: 4_096,
        }),
        BrokerCommand::EnsureConsumerGroup(EnsureConsumerGroupCommand {
            namespace_id,
            group_id: group_id.clone(),
            max_namespace_groups: 64,
        }),
        BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            capabilities: Capabilities::new(["rust", "one-node"])?,
            now_ms: TimestampMs::new(1_100),
            max_group_members: 256,
        }),
        BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: term,
            expected_generation: generation,
            lease_id: lease_id.clone(),
            now_ms: TimestampMs::new(1_200),
            lease_duration_ms: 30_000,
        }),
        BrokerCommand::RenewTaskLease(RenewTaskLeaseCommand {
            task_id: task_id.clone(),
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: term,
            expected_generation: generation,
            expected_lease_epoch: lease_epoch,
            lease_id: lease_id.clone(),
            now_ms: TimestampMs::new(1_300),
            lease_duration_ms: 30_000,
        }),
        BrokerCommand::CompleteTask(CompleteTaskCommand {
            task_id,
            group_id,
            member_id,
            expected_term: term,
            expected_generation: generation,
            expected_lease_epoch: lease_epoch,
            lease_id,
            result: TaskResult::new("complete")?,
            completed_at_ms: TimestampMs::new(1_400),
        }),
    ])
}

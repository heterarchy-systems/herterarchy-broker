use std::error::Error;

use agent_broker_domain::commands::{
    BrokerCommand, ClaimTaskCommand, CompleteTaskCommand, EnsureConsumerGroupCommand,
    EnsureNamespaceCommand, JoinConsumerGroupCommand, LeaveConsumerGroupCommand,
    PruneCompletedTasksCommand, PublishTaskCommand,
};
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{
    BrokerStateMachine, Capabilities, CheckpointError, ConsumerGroupId, ConsumerId, Generation,
    LeaseId, NamespaceId, TaskCheckpointState, TaskId, TaskObjective, TaskResult, TaskStatus, Term,
    TimestampMs,
};

struct WorkerFixture {
    namespace_id: NamespaceId,
    group_id: ConsumerGroupId,
    member_id: ConsumerId,
    generation: Generation,
}

fn setup_worker(machine: &mut BrokerStateMachine) -> Result<WorkerFixture, Box<dyn Error>> {
    let namespace_id = NamespaceId::new("project-a")?;
    machine.apply(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: namespace_id.clone(),
        max_namespaces: 8,
    }))?;
    let group_id = ConsumerGroupId::new("engineering")?;
    machine.apply(BrokerCommand::EnsureConsumerGroup(
        EnsureConsumerGroupCommand {
            namespace_id: namespace_id.clone(),
            group_id: group_id.clone(),
            max_namespace_groups: 8,
        },
    ))?;
    let member_id = ConsumerId::new("worker-a")?;
    let joined = machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
        group_id: group_id.clone(),
        member_id: member_id.clone(),
        capabilities: Capabilities::new(["code"])?,
        now_ms: TimestampMs::new(1_000),
        max_group_members: 8,
    }))?;
    let BrokerMutationResult::ConsumerGroup(group) = joined.result else {
        return Err("join result must be ConsumerGroup".into());
    };
    Ok(WorkerFixture {
        namespace_id,
        group_id,
        member_id,
        generation: group.generation,
    })
}

fn publish(
    machine: &mut BrokerStateMachine,
    worker: &WorkerFixture,
    task_id: &str,
    created_at_ms: u64,
) -> Result<TaskId, Box<dyn Error>> {
    let task_id = TaskId::new(task_id)?;
    machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
        namespace_id: worker.namespace_id.clone(),
        task_id: task_id.clone(),
        objective: TaskObjective::new(format!("objective-{task_id}"))?,
        created_at_ms: TimestampMs::new(created_at_ms),
        max_namespace_tasks: 16,
    }))?;
    Ok(task_id)
}

fn claim(
    machine: &mut BrokerStateMachine,
    worker: &WorkerFixture,
    lease_id: &str,
    now_ms: u64,
) -> Result<(TaskId, LeaseId, agent_broker_domain::LeaseEpoch), Box<dyn Error>> {
    let lease_id = LeaseId::new(lease_id)?;
    let claimed = machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
        group_id: worker.group_id.clone(),
        member_id: worker.member_id.clone(),
        expected_term: Term::INITIAL,
        expected_generation: worker.generation,
        lease_id: lease_id.clone(),
        now_ms: TimestampMs::new(now_ms),
        lease_duration_ms: 60_000,
    }))?;
    let BrokerMutationResult::TaskClaim(claim) = claimed.result else {
        return Err("claim result must be TaskClaim".into());
    };
    Ok((
        claim.task_id.ok_or("claim must return a task")?,
        lease_id,
        claim.lease_epoch.ok_or("claim must return a lease epoch")?,
    ))
}

#[test]
fn checkpoint_restore_rebuilds_ready_task_index() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let worker = setup_worker(&mut machine)?;
    let task_id = publish(&mut machine, &worker, "task-ready", 1_500)?;
    let checkpoint = machine.state().checkpoint();
    let mut restored = BrokerStateMachine::from_checkpoint(checkpoint)?;
    assert_eq!(restored.state(), machine.state());

    let (claimed_task, _, _) = claim(&mut restored, &worker, "lease-ready", 2_000)?;
    assert_eq!(claimed_task, task_id);
    Ok(())
}

#[test]
fn checkpoint_restore_rebuilds_member_owned_active_lease_index() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let worker = setup_worker(&mut machine)?;
    let task_id = publish(&mut machine, &worker, "task-leased", 1_500)?;
    claim(&mut machine, &worker, "lease-active", 2_000)?;
    let checkpoint = machine.state().checkpoint();
    let mut restored = BrokerStateMachine::from_checkpoint(checkpoint)?;
    assert_eq!(restored.state(), machine.state());

    restored.apply(BrokerCommand::LeaveConsumerGroup(
        LeaveConsumerGroupCommand {
            group_id: worker.group_id,
            member_id: worker.member_id,
            expected_generation: worker.generation,
        },
    ))?;
    assert_eq!(
        restored
            .state()
            .task(&task_id)
            .map(agent_broker_domain::Task::status),
        Some(TaskStatus::Queued)
    );
    Ok(())
}

#[test]
fn checkpoint_restore_rebuilds_completed_retention_index() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let worker = setup_worker(&mut machine)?;
    let task_id = publish(&mut machine, &worker, "task-completed", 1_500)?;
    let (_, lease_id, lease_epoch) = claim(&mut machine, &worker, "lease-complete", 2_000)?;
    machine.apply(BrokerCommand::CompleteTask(CompleteTaskCommand {
        task_id: task_id.clone(),
        group_id: worker.group_id,
        member_id: worker.member_id,
        expected_term: Term::INITIAL,
        expected_generation: worker.generation,
        expected_lease_epoch: lease_epoch,
        lease_id,
        result: TaskResult::new("done")?,
        completed_at_ms: TimestampMs::new(3_000),
    }))?;
    let checkpoint = machine.state().checkpoint();
    let mut restored = BrokerStateMachine::from_checkpoint(checkpoint)?;
    assert_eq!(restored.state(), machine.state());

    let pruned = restored.apply(BrokerCommand::PruneCompletedTasks(
        PruneCompletedTasksCommand {
            completed_before_ms: TimestampMs::new(4_000),
            max_tasks: 8,
        },
    ))?;
    let BrokerMutationResult::CompletedTasksPruned(pruned) = pruned.result else {
        return Err("prune result must be CompletedTasksPruned".into());
    };
    assert_eq!(pruned.pruned_count, 1);
    assert!(restored.state().task(&task_id).is_none());
    Ok(())
}

#[test]
fn checkpoint_restore_rejects_duplicate_active_lease_ids() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let worker = setup_worker(&mut machine)?;
    publish(&mut machine, &worker, "task-a", 1_500)?;
    publish(&mut machine, &worker, "task-b", 1_600)?;
    claim(&mut machine, &worker, "lease-a", 2_000)?;
    claim(&mut machine, &worker, "lease-b", 2_100)?;
    let mut checkpoint = machine.state().checkpoint();
    let duplicate_lease = LeaseId::new("lease-a")?;
    let Some(second_task) = checkpoint.tasks.get_mut(1) else {
        return Err("expected two checkpoint tasks".into());
    };
    let TaskCheckpointState::Leased { lease_id, .. } = &mut second_task.state else {
        return Err("second checkpoint task must be leased".into());
    };
    *lease_id = duplicate_lease.clone();

    let restored = BrokerStateMachine::from_checkpoint(checkpoint);
    assert_eq!(
        restored.err(),
        Some(CheckpointError::DuplicateActiveLease(duplicate_lease))
    );
    Ok(())
}

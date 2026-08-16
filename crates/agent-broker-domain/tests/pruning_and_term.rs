use std::error::Error;

use agent_broker_domain::commands::{
    AdvanceTermCommand, BrokerCommand, ClaimTaskCommand, CompleteTaskCommand,
    EnsureConsumerGroupCommand, EnsureNamespaceCommand, JoinConsumerGroupCommand,
    PruneCompletedTasksCommand, PublishTaskCommand,
};
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{
    BrokerStateMachine, Capabilities, ConsumerGroupId, Generation, LeaseId, MemberId, NamespaceId,
    StateMachineError, TaskId, TaskObjective, TaskResult, TaskStatus, Term, TimestampMs,
};

struct ReadyWorker {
    namespace_id: NamespaceId,
    group_id: ConsumerGroupId,
    member_id: MemberId,
    generation: Generation,
}

fn setup_worker(machine: &mut BrokerStateMachine) -> Result<ReadyWorker, Box<dyn Error>> {
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
    let member_id = MemberId::new("worker-a")?;
    let joined = machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
        group_id: group_id.clone(),
        member_id: member_id.clone(),
        capabilities: Capabilities::new(["code"])?,
        now_ms: TimestampMs::new(1_000),
        max_group_members: 256,
    }))?;
    let BrokerMutationResult::ConsumerGroup(group) = joined.result else {
        return Err("join result must be ConsumerGroup".into());
    };
    Ok(ReadyWorker {
        namespace_id,
        group_id,
        member_id,
        generation: group.generation,
    })
}

fn publish(
    machine: &mut BrokerStateMachine,
    worker: &ReadyWorker,
    task_id: &str,
    created_at_ms: u64,
    max_namespace_tasks: usize,
) -> Result<TaskId, Box<dyn Error>> {
    let task_id = TaskId::new(task_id)?;
    machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
        namespace_id: worker.namespace_id.clone(),
        task_id: task_id.clone(),
        objective: TaskObjective::new(format!("objective-{task_id}"))?,
        created_at_ms: TimestampMs::new(created_at_ms),
        max_namespace_tasks,
    }))?;
    Ok(task_id)
}

fn claim_and_complete(
    machine: &mut BrokerStateMachine,
    worker: &ReadyWorker,
    lease_id: &str,
    now_ms: u64,
    completed_at_ms: u64,
) -> Result<TaskId, Box<dyn Error>> {
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
    let task_id = claim.task_id.ok_or("expected a ready task")?;
    let lease_epoch = claim.lease_epoch.ok_or("claim must contain lease epoch")?;
    machine.apply(BrokerCommand::CompleteTask(CompleteTaskCommand {
        task_id: task_id.clone(),
        group_id: worker.group_id.clone(),
        member_id: worker.member_id.clone(),
        expected_term: Term::INITIAL,
        expected_generation: worker.generation,
        expected_lease_epoch: lease_epoch,
        lease_id,
        result: TaskResult::new(format!("completed-{task_id}"))?,
        completed_at_ms: TimestampMs::new(completed_at_ms),
    }))?;
    Ok(task_id)
}

#[test]
fn completed_task_pruning_is_oldest_first_bounded_and_releases_capacity()
-> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let worker = setup_worker(&mut machine)?;
    let task_a = publish(&mut machine, &worker, "task-a", 1_000, 3)?;
    let task_b = publish(&mut machine, &worker, "task-b", 2_000, 3)?;
    let task_c = publish(&mut machine, &worker, "task-c", 3_000, 3)?;

    assert_eq!(
        claim_and_complete(&mut machine, &worker, "lease-a", 4_000, 5_000)?,
        task_a
    );
    assert_eq!(
        claim_and_complete(&mut machine, &worker, "lease-b", 4_100, 6_000)?,
        task_b
    );
    assert_eq!(
        claim_and_complete(&mut machine, &worker, "lease-c", 4_200, 7_000)?,
        task_c
    );
    assert_eq!(machine.state().task_count(), 3);

    let before_first_prune = machine.state().revision();
    let first = machine.apply(BrokerCommand::PruneCompletedTasks(
        PruneCompletedTasksCommand {
            completed_before_ms: TimestampMs::new(6_500),
            max_tasks: 1,
        },
    ))?;
    assert_eq!(
        machine.state().revision().get(),
        before_first_prune.get() + 1
    );
    let BrokerMutationResult::CompletedTasksPruned(first_result) = first.result else {
        return Err("prune result must be CompletedTasksPruned".into());
    };
    assert_eq!(first_result.pruned_count, 1);
    assert_eq!(
        first.changes.deleted_tasks.as_slice(),
        std::slice::from_ref(&task_a)
    );
    assert!(machine.state().task(&task_a).is_none());
    assert_eq!(machine.state().task_count(), 2);

    let second = machine.apply(BrokerCommand::PruneCompletedTasks(
        PruneCompletedTasksCommand {
            completed_before_ms: TimestampMs::new(6_500),
            max_tasks: 8,
        },
    ))?;
    let BrokerMutationResult::CompletedTasksPruned(second_result) = second.result else {
        return Err("prune result must be CompletedTasksPruned".into());
    };
    assert_eq!(second_result.pruned_count, 1);
    assert_eq!(
        second.changes.deleted_tasks.as_slice(),
        std::slice::from_ref(&task_b)
    );
    assert_eq!(machine.state().task_count(), 1);
    assert_eq!(
        machine
            .state()
            .task(&task_c)
            .map(agent_broker_domain::Task::status),
        Some(TaskStatus::Completed)
    );

    let new_task = publish(&mut machine, &worker, "task-d", 8_000, 2)?;
    assert_eq!(machine.state().task_count(), 2);
    assert_eq!(
        machine
            .state()
            .task(&new_task)
            .map(agent_broker_domain::Task::status),
        Some(TaskStatus::Queued)
    );
    Ok(())
}

#[test]
fn completed_task_pruning_noop_does_not_advance_revision_and_zero_limit_is_rejected()
-> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let worker = setup_worker(&mut machine)?;
    publish(&mut machine, &worker, "task-a", 1_000, 2)?;
    claim_and_complete(&mut machine, &worker, "lease-a", 2_000, 5_000)?;

    let before_noop = machine.state().revision();
    let noop = machine.apply(BrokerCommand::PruneCompletedTasks(
        PruneCompletedTasksCommand {
            completed_before_ms: TimestampMs::new(4_999),
            max_tasks: 8,
        },
    ))?;
    assert_eq!(machine.state().revision(), before_noop);
    assert!(noop.changes.is_empty());
    let BrokerMutationResult::CompletedTasksPruned(noop_result) = noop.result else {
        return Err("prune result must be CompletedTasksPruned".into());
    };
    assert_eq!(noop_result.pruned_count, 0);

    assert_eq!(
        machine.apply(BrokerCommand::PruneCompletedTasks(
            PruneCompletedTasksCommand {
                completed_before_ms: TimestampMs::new(10_000),
                max_tasks: 0,
            },
        )),
        Err(StateMachineError::InvalidCapacity { field: "max_tasks" })
    );
    Ok(())
}

#[test]
fn term_advance_fences_old_term_and_allows_current_term_completion() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let worker = setup_worker(&mut machine)?;
    let task_id = publish(&mut machine, &worker, "task-a", 1_000, 2)?;
    let lease_id = LeaseId::new("lease-a")?;
    let claimed = machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
        group_id: worker.group_id.clone(),
        member_id: worker.member_id.clone(),
        expected_term: Term::INITIAL,
        expected_generation: worker.generation,
        lease_id: lease_id.clone(),
        now_ms: TimestampMs::new(2_000),
        lease_duration_ms: 60_000,
    }))?;
    let BrokerMutationResult::TaskClaim(claim) = claimed.result else {
        return Err("claim result must be TaskClaim".into());
    };
    let lease_epoch = claim.lease_epoch.ok_or("claim must contain lease epoch")?;

    let before_advance = machine.state().revision();
    let advanced = machine.apply(BrokerCommand::AdvanceTerm(AdvanceTermCommand {
        expected_term: Term::INITIAL,
        new_term: Term::new(2)?,
    }))?;
    assert_eq!(machine.state().term().get(), 2);
    assert_eq!(machine.state().revision().get(), before_advance.get() + 1);
    let BrokerMutationResult::TermAdvanced(term_result) = advanced.result else {
        return Err("term result must be TermAdvanced".into());
    };
    assert_eq!(term_result.metadata.term.get(), 2);

    let old_term_completion = machine.apply(BrokerCommand::CompleteTask(CompleteTaskCommand {
        task_id: task_id.clone(),
        group_id: worker.group_id.clone(),
        member_id: worker.member_id.clone(),
        expected_term: Term::INITIAL,
        expected_generation: worker.generation,
        expected_lease_epoch: lease_epoch,
        lease_id: lease_id.clone(),
        result: TaskResult::new("done")?,
        completed_at_ms: TimestampMs::new(3_000),
    }));
    assert!(matches!(
        old_term_completion,
        Err(StateMachineError::StaleTerm { .. })
    ));

    let completed = machine.apply(BrokerCommand::CompleteTask(CompleteTaskCommand {
        task_id,
        group_id: worker.group_id,
        member_id: worker.member_id,
        expected_term: Term::new(2)?,
        expected_generation: worker.generation,
        expected_lease_epoch: lease_epoch,
        lease_id,
        result: TaskResult::new("done")?,
        completed_at_ms: TimestampMs::new(3_000),
    }))?;
    let BrokerMutationResult::TaskCompleted(result) = completed.result else {
        return Err("complete result must be TaskCompleted".into());
    };
    assert_eq!(result.status, TaskStatus::Completed);
    Ok(())
}

#[test]
fn term_advance_rejects_non_increasing_and_stale_expected_terms() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    machine.apply(BrokerCommand::AdvanceTerm(AdvanceTermCommand {
        expected_term: Term::INITIAL,
        new_term: Term::new(2)?,
    }))?;

    let non_increasing = machine.apply(BrokerCommand::AdvanceTerm(AdvanceTermCommand {
        expected_term: Term::new(2)?,
        new_term: Term::new(2)?,
    }));
    assert!(matches!(
        non_increasing,
        Err(StateMachineError::NewTermNotGreater { .. })
    ));

    let stale_expected = machine.apply(BrokerCommand::AdvanceTerm(AdvanceTermCommand {
        expected_term: Term::INITIAL,
        new_term: Term::new(3)?,
    }));
    assert!(matches!(
        stale_expected,
        Err(StateMachineError::StaleTerm { .. })
    ));
    Ok(())
}

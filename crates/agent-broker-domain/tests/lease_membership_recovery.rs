use std::error::Error;

use agent_broker_domain::commands::{
    BrokerCommand, ClaimTaskCommand, CompleteTaskCommand, EnsureConsumerGroupCommand,
    EnsureNamespaceCommand, JoinConsumerGroupCommand, LeaveConsumerGroupCommand,
    PublishTaskCommand, ReapStaleMembersCommand,
};
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{
    BrokerStateMachine, Capabilities, ConsumerGroupId, LeaseEpoch, LeaseId, MemberId, NamespaceId,
    TaskId, TaskObjective, TaskResult, TaskStatus, Term, TimestampMs,
};

fn setup_group(
    machine: &mut BrokerStateMachine,
    first_member: &MemberId,
) -> Result<(ConsumerGroupId, agent_broker_domain::Generation), Box<dyn Error>> {
    let namespace_id = NamespaceId::new("project-a")?;
    machine.apply(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: namespace_id.clone(),
        max_namespaces: 8,
    }))?;
    let group_id = ConsumerGroupId::new("engineering")?;
    machine.apply(BrokerCommand::EnsureConsumerGroup(
        EnsureConsumerGroupCommand {
            namespace_id,
            group_id: group_id.clone(),
            max_namespace_groups: 8,
        },
    ))?;
    let joined = machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
        group_id: group_id.clone(),
        member_id: first_member.clone(),
        capabilities: Capabilities::new(["code"])?,
        now_ms: TimestampMs::new(1_000),
        max_group_members: 256,
    }))?;
    let BrokerMutationResult::ConsumerGroup(group) = joined.result else {
        return Err("join result must be ConsumerGroup".into());
    };
    Ok((group_id, group.generation))
}

fn publish(machine: &mut BrokerStateMachine) -> Result<TaskId, Box<dyn Error>> {
    let task_id = TaskId::new("task-1")?;
    machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
        namespace_id: NamespaceId::new("project-a")?,
        task_id: task_id.clone(),
        objective: TaskObjective::new("recover member-owned lease")?,
        created_at_ms: TimestampMs::new(1_500),
        max_namespace_tasks: 8,
    }))?;
    Ok(task_id)
}

#[test]
fn leave_requeues_member_owned_lease_and_next_claim_advances_epoch() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let worker_a = MemberId::new("worker-a")?;
    let (group_id, generation) = setup_group(&mut machine, &worker_a)?;
    let task_id = publish(&mut machine)?;
    machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
        group_id: group_id.clone(),
        member_id: worker_a.clone(),
        expected_term: Term::INITIAL,
        expected_generation: generation,
        lease_id: LeaseId::new("lease-1")?,
        now_ms: TimestampMs::new(2_000),
        lease_duration_ms: 60_000,
    }))?;

    let before_leave = machine.state().revision();
    let left = machine.apply(BrokerCommand::LeaveConsumerGroup(
        LeaveConsumerGroupCommand {
            group_id: group_id.clone(),
            member_id: worker_a.clone(),
            expected_generation: generation,
        },
    ))?;
    assert_eq!(machine.state().revision().get(), before_leave.get() + 1);
    assert_eq!(
        left.changes.tasks.as_slice(),
        std::slice::from_ref(&task_id)
    );
    assert_eq!(
        machine
            .state()
            .task(&task_id)
            .map(agent_broker_domain::Task::status),
        Some(TaskStatus::Queued)
    );

    let stale = machine.apply(BrokerCommand::CompleteTask(CompleteTaskCommand {
        task_id: task_id.clone(),
        group_id: group_id.clone(),
        member_id: worker_a,
        expected_term: Term::INITIAL,
        expected_generation: generation,
        expected_lease_epoch: LeaseEpoch::new(1),
        lease_id: LeaseId::new("lease-1")?,
        result: TaskResult::new("stale")?,
        completed_at_ms: TimestampMs::new(3_000),
    }));
    assert!(stale.is_err());

    let worker_b = MemberId::new("worker-b")?;
    let joined = machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
        group_id: group_id.clone(),
        member_id: worker_b.clone(),
        capabilities: Capabilities::new(["code"])?,
        now_ms: TimestampMs::new(3_100),
        max_group_members: 256,
    }))?;
    let BrokerMutationResult::ConsumerGroup(group) = joined.result else {
        return Err("join result must be ConsumerGroup".into());
    };
    let reclaimed = machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
        group_id,
        member_id: worker_b,
        expected_term: Term::INITIAL,
        expected_generation: group.generation,
        lease_id: LeaseId::new("lease-2")?,
        now_ms: TimestampMs::new(3_200),
        lease_duration_ms: 60_000,
    }))?;
    let BrokerMutationResult::TaskClaim(reclaimed) = reclaimed.result else {
        return Err("reclaim result must be TaskClaim".into());
    };
    assert_eq!(reclaimed.task_id.as_ref(), Some(&task_id));
    assert_eq!(reclaimed.lease_epoch, Some(LeaseEpoch::new(2)));
    Ok(())
}

#[test]
fn stale_reap_requeues_member_owned_lease_for_surviving_member() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let worker_a = MemberId::new("worker-a")?;
    let worker_b = MemberId::new("worker-b")?;
    let (group_id, _) = setup_group(&mut machine, &worker_a)?;
    let joined = machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
        group_id: group_id.clone(),
        member_id: worker_b.clone(),
        capabilities: Capabilities::new(["code"])?,
        now_ms: TimestampMs::new(5_000),
        max_group_members: 256,
    }))?;
    let BrokerMutationResult::ConsumerGroup(group) = joined.result else {
        return Err("join result must be ConsumerGroup".into());
    };
    let task_id = publish(&mut machine)?;
    machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
        group_id: group_id.clone(),
        member_id: worker_a,
        expected_term: Term::INITIAL,
        expected_generation: group.generation,
        lease_id: LeaseId::new("lease-1")?,
        now_ms: TimestampMs::new(5_100),
        lease_duration_ms: 60_000,
    }))?;

    let before_reap = machine.state().revision();
    let reaped = machine.apply(BrokerCommand::ReapStaleMembers(ReapStaleMembersCommand {
        stale_before_ms: TimestampMs::new(2_000),
        max_members: 1,
    }))?;
    assert_eq!(machine.state().revision().get(), before_reap.get() + 1);
    assert_eq!(
        reaped.changes.tasks.as_slice(),
        std::slice::from_ref(&task_id)
    );
    assert_eq!(
        machine
            .state()
            .task(&task_id)
            .map(agent_broker_domain::Task::status),
        Some(TaskStatus::Queued)
    );

    let current_generation = machine
        .state()
        .group(&group_id)
        .ok_or("group must exist")?
        .generation();
    let reclaimed = machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
        group_id,
        member_id: worker_b,
        expected_term: Term::INITIAL,
        expected_generation: current_generation,
        lease_id: LeaseId::new("lease-2")?,
        now_ms: TimestampMs::new(5_200),
        lease_duration_ms: 60_000,
    }))?;
    let BrokerMutationResult::TaskClaim(reclaimed) = reclaimed.result else {
        return Err("reclaim result must be TaskClaim".into());
    };
    assert_eq!(reclaimed.task_id.as_ref(), Some(&task_id));
    assert_eq!(reclaimed.lease_epoch, Some(LeaseEpoch::new(2)));
    Ok(())
}

use agent_broker_application::{BrokerError, BrokerErrorCode, BrokerErrorDisposition};
use agent_broker_consensus::{ReplicatedBrokerCommandV1, ReplicatedBrokerResponseV1};
use agent_broker_domain::commands::{
    AdvanceTermCommand, BrokerCommand, ClaimTaskCommand, CompleteTaskCommand,
    EnsureConsumerGroupCommand, EnsureNamespaceCommand, HeartbeatCommand, JoinConsumerGroupCommand,
    LeaveConsumerGroupCommand, PruneCompletedTasksCommand, PublishTaskCommand,
    ReapStaleMembersCommand, RenewTaskLeaseCommand,
};
use agent_broker_domain::results::{
    BrokerMutationResult, CompletedTasksPrunedResult, ConsumerGroupResult, HeartbeatResult,
    MutationMetadata, NamespaceResult, StaleMembersReapedResult, TaskClaimResult,
    TaskCompletedResult, TaskLeaseRenewedResult, TaskPublishedResult, TermAdvancedResult,
};
use agent_broker_domain::{
    Capabilities, ConsumerGroupId, ConsumerId, Generation, LeaseEpoch, LeaseId, NamespaceId,
    Revision, TaskId, TaskObjective, TaskStatus, Term, TimestampMs,
};

#[test]
fn every_broker_command_round_trips_through_replicated_schema()
-> Result<(), Box<dyn std::error::Error>> {
    let commands = sample_commands()?;

    for command in commands {
        let replicated = ReplicatedBrokerCommandV1::try_from(command.clone())?;
        let encoded = serde_json::to_vec(&replicated)?;
        let decoded: ReplicatedBrokerCommandV1 = serde_json::from_slice(&encoded)?;
        let recovered = BrokerCommand::try_from(decoded)?;
        assert_eq!(recovered, command);
    }
    Ok(())
}

#[test]
fn every_broker_mutation_result_round_trips_through_replicated_response()
-> Result<(), Box<dyn std::error::Error>> {
    for result in sample_results()? {
        let response = ReplicatedBrokerResponseV1::from_application_result(Ok(result.clone()))?;
        let encoded = serde_json::to_vec(&response)?;
        let decoded: ReplicatedBrokerResponseV1 = serde_json::from_slice(&encoded)?;
        let recovered = decoded.into_application_result()??;
        assert_eq!(recovered, result);
    }
    Ok(())
}

#[test]
fn broker_business_error_round_trips_as_application_data() -> Result<(), Box<dyn std::error::Error>>
{
    let error = BrokerError::new(BrokerErrorCode::StaleFence, "stale lease holder");
    let response = ReplicatedBrokerResponseV1::from_application_result(Err(error.clone()))?;
    let encoded = serde_json::to_vec(&response)?;
    let decoded: ReplicatedBrokerResponseV1 = serde_json::from_slice(&encoded)?;
    let recovered = decoded.into_application_result()?;
    assert_eq!(
        recovered,
        Err(error.with_disposition(BrokerErrorDisposition::Committed))
    );
    Ok(())
}

#[test]
fn replicated_claim_rejects_partial_optional_payload() -> Result<(), Box<dyn std::error::Error>> {
    let malformed = serde_json::json!({
        "status": "success",
        "payload": {
            "result_type": "task_claim",
            "metadata": {"term": 1, "revision": 7},
            "task_id": "task-1",
            "objective": null,
            "task_revision": 7,
            "lease_id": "lease-1",
            "lease_epoch": 1,
            "lease_expires_at_ms": 2000,
            "generation": 1
        }
    });
    let decoded: ReplicatedBrokerResponseV1 = serde_json::from_value(malformed)?;
    assert!(decoded.into_application_result().is_err());
    Ok(())
}

fn sample_commands() -> Result<Vec<BrokerCommand>, Box<dyn std::error::Error>> {
    let namespace_id = NamespaceId::new("project-a")?;
    let task_id = TaskId::new("task-1")?;
    let group_id = ConsumerGroupId::new("workers")?;
    let member_id = ConsumerId::new("worker-1")?;
    let lease_id = LeaseId::new("lease-1")?;

    Ok(vec![
        BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: namespace_id.clone(),
            max_namespaces: 64,
        }),
        BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: namespace_id.clone(),
            task_id: task_id.clone(),
            objective: TaskObjective::new("compile the release")?,
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
            capabilities: Capabilities::new(["rust", "linux"])?,
            now_ms: TimestampMs::new(1_100),
            max_group_members: 256,
        }),
        BrokerCommand::Heartbeat(HeartbeatCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_generation: Generation::new(1),
            now_ms: TimestampMs::new(1_200),
        }),
        BrokerCommand::LeaveConsumerGroup(LeaveConsumerGroupCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_generation: Generation::new(1),
        }),
        BrokerCommand::ReapStaleMembers(ReapStaleMembersCommand {
            stale_before_ms: TimestampMs::new(1_300),
            max_members: 16,
        }),
        BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: Generation::new(1),
            lease_id: lease_id.clone(),
            now_ms: TimestampMs::new(1_400),
            lease_duration_ms: 30_000,
        }),
        BrokerCommand::RenewTaskLease(RenewTaskLeaseCommand {
            task_id: task_id.clone(),
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: Generation::new(1),
            expected_lease_epoch: LeaseEpoch::new(1),
            lease_id: lease_id.clone(),
            now_ms: TimestampMs::new(1_500),
            lease_duration_ms: 30_000,
        }),
        BrokerCommand::CompleteTask(CompleteTaskCommand {
            task_id,
            group_id,
            member_id,
            expected_term: Term::INITIAL,
            expected_generation: Generation::new(1),
            expected_lease_epoch: LeaseEpoch::new(1),
            lease_id,
            result: agent_broker_domain::TaskResult::new("done")?,
            completed_at_ms: TimestampMs::new(1_600),
        }),
        BrokerCommand::PruneCompletedTasks(PruneCompletedTasksCommand {
            completed_before_ms: TimestampMs::new(2_000),
            max_tasks: 128,
        }),
        BrokerCommand::AdvanceTerm(AdvanceTermCommand {
            expected_term: Term::INITIAL,
            new_term: Term::new(2)?,
        }),
    ])
}

fn sample_results() -> Result<Vec<BrokerMutationResult>, Box<dyn std::error::Error>> {
    let metadata = MutationMetadata {
        term: Term::INITIAL,
        revision: Revision::new(7),
    };
    Ok(vec![
        BrokerMutationResult::Namespace(NamespaceResult {
            metadata,
            namespace_id: NamespaceId::new("project-a")?,
            namespace_revision: Revision::new(1),
        }),
        BrokerMutationResult::TaskPublished(TaskPublishedResult {
            metadata,
            task_id: TaskId::new("task-1")?,
            task_revision: Revision::new(2),
            status: TaskStatus::Queued,
        }),
        BrokerMutationResult::ConsumerGroup(ConsumerGroupResult {
            metadata,
            group_id: ConsumerGroupId::new("workers")?,
            generation: Generation::new(1),
            group_revision: Revision::new(3),
            member_count: 2,
        }),
        BrokerMutationResult::Heartbeat(HeartbeatResult {
            metadata,
            group_id: ConsumerGroupId::new("workers")?,
            member_id: ConsumerId::new("worker-1")?,
            generation: Generation::new(1),
            member_revision: Revision::new(4),
        }),
        BrokerMutationResult::StaleMembersReaped(StaleMembersReapedResult {
            metadata,
            reaped_count: 2,
            affected_group_count: 1,
        }),
        BrokerMutationResult::TaskClaim(TaskClaimResult {
            metadata,
            task_id: Some(TaskId::new("task-1")?),
            objective: Some(TaskObjective::new("compile the release")?),
            task_revision: Some(Revision::new(5)),
            lease_id: Some(LeaseId::new("lease-1")?),
            lease_epoch: Some(LeaseEpoch::new(1)),
            lease_expires_at_ms: Some(TimestampMs::new(31_400)),
            generation: Generation::new(1),
        }),
        BrokerMutationResult::TaskLeaseRenewed(TaskLeaseRenewedResult {
            metadata,
            task_id: TaskId::new("task-1")?,
            task_revision: Revision::new(6),
            lease_id: LeaseId::new("lease-1")?,
            lease_epoch: LeaseEpoch::new(1),
            lease_expires_at_ms: TimestampMs::new(31_500),
            generation: Generation::new(1),
        }),
        BrokerMutationResult::TaskCompleted(TaskCompletedResult {
            metadata,
            task_id: TaskId::new("task-1")?,
            task_revision: Revision::new(7),
            status: TaskStatus::Completed,
        }),
        BrokerMutationResult::CompletedTasksPruned(CompletedTasksPrunedResult {
            metadata,
            pruned_count: 3,
        }),
        BrokerMutationResult::TermAdvanced(TermAdvancedResult { metadata }),
    ])
}

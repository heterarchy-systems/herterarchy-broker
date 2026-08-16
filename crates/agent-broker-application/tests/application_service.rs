use std::error::Error;

use agent_broker_application::{
    BrokerApplicationService, BrokerError, BrokerErrorCode, ClaimTaskInput, CompleteTaskInput,
    ConsensusAdapter,
};
use agent_broker_domain::commands::BrokerCommand;
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{
    BrokerCapacityPolicy, BrokerStateMachine, Capabilities, ConsumerGroupId, Generation,
    LeaseDurationMs, LeaseId, MemberId, NamespaceId, Revision, TaskId, TaskObjective, TaskResult,
    TaskStatus, Term, TimestampMs,
};

#[derive(Debug, Default)]
struct MemoryConsensus {
    machine: BrokerStateMachine,
}

impl ConsensusAdapter for MemoryConsensus {
    fn term(&self) -> Term {
        self.machine.state().term()
    }

    fn revision(&self) -> Revision {
        self.machine.state().revision()
    }

    fn propose(&mut self, command: BrokerCommand) -> Result<BrokerMutationResult, BrokerError> {
        self.machine
            .apply(command)
            .map(|applied| applied.result)
            .map_err(BrokerError::from)
    }
}

#[test]
fn application_service_runs_typed_task_lifecycle_through_consensus_boundary()
-> Result<(), Box<dyn Error>> {
    let mut service =
        BrokerApplicationService::new(MemoryConsensus::default(), BrokerCapacityPolicy::default());
    assert_eq!(service.health().term, Term::INITIAL);
    assert_eq!(service.health().revision.get(), 0);
    assert_eq!(service.health().protocol_version, 1);

    let namespace_id = NamespaceId::new("project-a")?;
    let group_id = ConsumerGroupId::new("engineering")?;
    let member_id = MemberId::new("worker-a")?;
    let task_id = TaskId::new("task-1")?;
    service.ensure_namespace(namespace_id.clone())?;
    service.ensure_consumer_group(namespace_id.clone(), group_id.clone())?;
    let joined = service.join_consumer_group(
        group_id.clone(),
        member_id.clone(),
        Capabilities::new(["review", "code", "review"])?,
        TimestampMs::new(1_000),
    )?;
    service.publish_task(
        namespace_id,
        task_id.clone(),
        TaskObjective::new("implement typed application boundary")?,
        TimestampMs::new(1_001),
    )?;

    let lease_id = LeaseId::new("lease-1")?;
    let claimed = service.claim_task(ClaimTaskInput {
        group_id: group_id.clone(),
        member_id: member_id.clone(),
        expected_term: Term::INITIAL,
        expected_generation: joined.generation,
        lease_id: lease_id.clone(),
        now_ms: TimestampMs::new(2_000),
        lease_duration: LeaseDurationMs::new(1_000)?,
    })?;
    assert_eq!(claimed.task_id.as_ref(), Some(&task_id));
    let lease_epoch = claimed.lease_epoch.ok_or("claim must return lease epoch")?;

    let completed = service.complete_task(CompleteTaskInput {
        task_id,
        group_id,
        member_id,
        expected_term: Term::INITIAL,
        expected_generation: joined.generation,
        expected_lease_epoch: lease_epoch,
        lease_id,
        result: TaskResult::new("done")?,
        completed_at_ms: TimestampMs::new(2_500),
    })?;
    assert_eq!(completed.status, TaskStatus::Completed);
    assert_eq!(service.health().revision, completed.metadata.revision);
    Ok(())
}

#[test]
fn application_service_preserves_stable_stale_fence_code() -> Result<(), Box<dyn Error>> {
    let mut service =
        BrokerApplicationService::new(MemoryConsensus::default(), BrokerCapacityPolicy::default());
    let namespace_id = NamespaceId::new("project-a")?;
    let group_id = ConsumerGroupId::new("engineering")?;
    let member_id = MemberId::new("worker-a")?;
    service.ensure_namespace(namespace_id.clone())?;
    service.ensure_consumer_group(namespace_id, group_id.clone())?;
    let joined = service.join_consumer_group(
        group_id.clone(),
        member_id.clone(),
        Capabilities::new(["code"])?,
        TimestampMs::new(1_000),
    )?;

    let stale_generation = Generation::new(joined.generation.get().saturating_sub(1));
    let Err(error) = service.heartbeat(
        group_id,
        member_id,
        stale_generation,
        TimestampMs::new(2_000),
    ) else {
        return Err("stale generation must be rejected".into());
    };
    assert_eq!(error.code(), BrokerErrorCode::StaleFence);
    assert_eq!(error.code().as_str(), "STALE_FENCE");
    Ok(())
}

#[test]
fn application_service_capacity_policy_maps_to_stable_capacity_error() -> Result<(), Box<dyn Error>>
{
    let capacity = BrokerCapacityPolicy::new(1, 1, 1, 1)?;
    let mut service = BrokerApplicationService::new(MemoryConsensus::default(), capacity);
    service.ensure_namespace(NamespaceId::new("project-a")?)?;

    let Err(error) = service.ensure_namespace(NamespaceId::new("project-b")?) else {
        return Err("second namespace must exceed configured capacity".into());
    };
    assert_eq!(error.code(), BrokerErrorCode::CapacityExceeded);
    assert_eq!(error.code().as_str(), "CAPACITY_EXCEEDED");
    Ok(())
}

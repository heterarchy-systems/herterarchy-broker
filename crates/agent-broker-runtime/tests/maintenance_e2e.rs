use std::error::Error;

use agent_broker_application::BrokerApplicationService;
use agent_broker_consensus::StandaloneConsensusAdapter;
use agent_broker_domain::{
    BrokerCapacityPolicy, ConsumerGroupId, Generation, LeaseDurationMs, LeaseEpoch, LeaseId,
    MemberId, NamespaceId, TaskId, TaskObjective, TaskResult, TimestampMs,
};
use agent_broker_protocol::{
    BrokerRequest, BrokerRequestDispatcher, BrokerResponse, ClaimTaskRequest, CompleteTaskRequest,
    DeclaredCapabilities, EnsureConsumerGroupRequest, EnsureNamespaceRequest,
    JoinConsumerGroupRequest, PublishTaskRequest, RequestId, SuccessPayload,
};
use agent_broker_runtime::{StandaloneMaintenancePolicy, StateOwnerHandle};
use agent_broker_storage::{JournalCompactionPolicy, JournaledBrokerStateRepository};
use tempfile::{TempDir, tempdir};

fn state_owner() -> Result<(StateOwnerHandle, TempDir), Box<dyn Error>> {
    let directory = tempdir()?;
    let state_path = directory.path().join("broker-state.json");
    let repository = JournaledBrokerStateRepository::new(
        state_path,
        None,
        JournalCompactionPolicy::new(10_000, 64 * 1024 * 1024)?,
    );
    let consensus = StandaloneConsensusAdapter::new(repository)?;
    let service = BrokerApplicationService::new(consensus, BrokerCapacityPolicy::default());
    let owner = StateOwnerHandle::spawn(BrokerRequestDispatcher::new(service), 16)?;
    Ok((owner, directory))
}

fn request_id(value: &str) -> Result<RequestId, Box<dyn Error>> {
    Ok(RequestId::new(value)?)
}

fn dispatch(
    owner: &StateOwnerHandle,
    request: BrokerRequest,
    observed_at_ms: u64,
) -> Result<SuccessPayload, Box<dyn Error>> {
    match owner.dispatch(request, TimestampMs::new(observed_at_ms))? {
        BrokerResponse::Success { result, .. } => Ok(result),
        BrokerResponse::Error { error, .. } => Err(format!(
            "unexpected Broker error {}: {}",
            error.code.as_str(),
            error.message
        )
        .into()),
    }
}

fn bootstrap_member(
    owner: &StateOwnerHandle,
) -> Result<(NamespaceId, ConsumerGroupId, MemberId, Generation), Box<dyn Error>> {
    let namespace_id = NamespaceId::new("project-a")?;
    dispatch(
        owner,
        BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
            request_id: request_id("ns")?,
            namespace_id: namespace_id.clone(),
        }),
        500,
    )?;
    let group_id = ConsumerGroupId::new("engineering")?;
    dispatch(
        owner,
        BrokerRequest::EnsureConsumerGroup(EnsureConsumerGroupRequest {
            request_id: request_id("group")?,
            namespace_id: namespace_id.clone(),
            group_id: group_id.clone(),
        }),
        600,
    )?;
    let member_id = MemberId::new("worker-a")?;
    let joined = dispatch(
        owner,
        BrokerRequest::JoinConsumerGroup(JoinConsumerGroupRequest {
            request_id: request_id("join-a")?,
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            capabilities: DeclaredCapabilities::new(["code"])?,
        }),
        1_000,
    )?;
    let SuccessPayload::ConsumerGroup { generation, .. } = joined else {
        return Err("join must return ConsumerGroup payload".into());
    };
    Ok((namespace_id, group_id, member_id, generation))
}

#[test]
fn maintenance_reaps_stale_member_and_requeues_owned_lease() -> Result<(), Box<dyn Error>> {
    let (owner, _directory) = state_owner()?;
    let (namespace_id, group_id, member_a, generation_a) = bootstrap_member(&owner)?;
    let task_id = TaskId::new("task-1")?;
    dispatch(
        &owner,
        BrokerRequest::PublishTask(PublishTaskRequest {
            request_id: request_id("publish")?,
            namespace_id,
            task_id: task_id.clone(),
            objective: TaskObjective::new("requeue after stale member")?,
        }),
        1_500,
    )?;
    dispatch(
        &owner,
        BrokerRequest::ClaimTask(ClaimTaskRequest {
            request_id: request_id("claim-a")?,
            group_id: group_id.clone(),
            member_id: member_a,
            expected_term: agent_broker_domain::Term::INITIAL,
            expected_generation: generation_a,
            lease_id: LeaseId::new("lease-a")?,
            lease_duration: LeaseDurationMs::new(60_000)?,
        }),
        2_000,
    )?;

    let policy =
        StandaloneMaintenancePolicy::new(24 * 60 * 60 * 1_000, 45_000, 5_000, 1_024, 4, 1_024, 4)?;
    let result = owner.run_maintenance(policy, TimestampMs::new(50_000))?;
    assert_eq!(result.reaped_stale_members, 1);
    assert_eq!(result.pruned_completed_tasks, 0);

    let member_b = MemberId::new("worker-b")?;
    let joined_b = dispatch(
        &owner,
        BrokerRequest::JoinConsumerGroup(JoinConsumerGroupRequest {
            request_id: request_id("join-b")?,
            group_id: group_id.clone(),
            member_id: member_b.clone(),
            capabilities: DeclaredCapabilities::new(["code"])?,
        }),
        51_000,
    )?;
    let SuccessPayload::ConsumerGroup {
        generation: generation_b,
        ..
    } = joined_b
    else {
        return Err("second join must return ConsumerGroup payload".into());
    };
    assert_eq!(generation_b, Generation::new(3));
    let reclaimed = dispatch(
        &owner,
        BrokerRequest::ClaimTask(ClaimTaskRequest {
            request_id: request_id("claim-b")?,
            group_id,
            member_id: member_b,
            expected_term: agent_broker_domain::Term::INITIAL,
            expected_generation: generation_b,
            lease_id: LeaseId::new("lease-b")?,
            lease_duration: LeaseDurationMs::new(60_000)?,
        }),
        52_000,
    )?;
    let SuccessPayload::TaskClaimed {
        task_id: reclaimed_id,
        lease_epoch,
        ..
    } = reclaimed
    else {
        return Err("reclaim must return TaskClaimed payload".into());
    };
    assert_eq!(reclaimed_id.as_ref(), Some(&task_id));
    assert_eq!(lease_epoch, Some(LeaseEpoch::new(2)));
    Ok(())
}

#[test]
fn maintenance_prunes_expired_completed_task_and_releases_identity() -> Result<(), Box<dyn Error>> {
    let (owner, _directory) = state_owner()?;
    let (namespace_id, group_id, member_id, generation) = bootstrap_member(&owner)?;
    let task_id = TaskId::new("task-prune")?;
    dispatch(
        &owner,
        BrokerRequest::PublishTask(PublishTaskRequest {
            request_id: request_id("publish-prune")?,
            namespace_id: namespace_id.clone(),
            task_id: task_id.clone(),
            objective: TaskObjective::new("prune completed task")?,
        }),
        1_500,
    )?;
    let lease_id = LeaseId::new("lease-prune")?;
    let claim = dispatch(
        &owner,
        BrokerRequest::ClaimTask(ClaimTaskRequest {
            request_id: request_id("claim-prune")?,
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: agent_broker_domain::Term::INITIAL,
            expected_generation: generation,
            lease_id: lease_id.clone(),
            lease_duration: LeaseDurationMs::new(60_000)?,
        }),
        2_000,
    )?;
    let SuccessPayload::TaskClaimed {
        lease_epoch: Some(lease_epoch),
        ..
    } = claim
    else {
        return Err("claim must return lease epoch".into());
    };
    dispatch(
        &owner,
        BrokerRequest::CompleteTask(CompleteTaskRequest {
            request_id: request_id("complete-prune")?,
            task_id: task_id.clone(),
            group_id,
            member_id,
            expected_term: agent_broker_domain::Term::INITIAL,
            expected_generation: generation,
            expected_lease_epoch: lease_epoch,
            lease_id,
            result: TaskResult::new("done")?,
        }),
        3_000,
    )?;

    let policy = StandaloneMaintenancePolicy::new(1_000, 1_000_000, 5_000, 1_024, 4, 1_024, 4)?;
    let result = owner.run_maintenance(policy, TimestampMs::new(10_000))?;
    assert_eq!(result.reaped_stale_members, 0);
    assert_eq!(result.pruned_completed_tasks, 1);

    let republished = dispatch(
        &owner,
        BrokerRequest::PublishTask(PublishTaskRequest {
            request_id: request_id("republish-pruned")?,
            namespace_id,
            task_id,
            objective: TaskObjective::new("identity released after prune")?,
        }),
        11_000,
    )?;
    assert!(matches!(republished, SuccessPayload::TaskPublished { .. }));
    Ok(())
}

#[test]
fn maintenance_policy_rejects_unbounded_or_zero_batches() {
    assert!(StandaloneMaintenancePolicy::new(0, 0, 99, 1, 1, 1, 1).is_err());
    assert!(StandaloneMaintenancePolicy::new(0, 0, 100, 0, 1, 1, 1).is_err());
    assert!(StandaloneMaintenancePolicy::new(0, 0, 100, 1, 65, 1, 1).is_err());
    assert!(StandaloneMaintenancePolicy::new(0, 0, 100, 1, 1, 4_097, 1).is_err());
}

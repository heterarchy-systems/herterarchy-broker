use std::error::Error;

use agent_broker_application::{BrokerApplicationService, BrokerError, ConsensusAdapter};
use agent_broker_domain::commands::BrokerCommand;
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{
    BrokerCapacityPolicy, BrokerStateMachine, ConsumerGroupId, Generation, LeaseDurationMs,
    LeaseEpoch, LeaseId, MemberId, NamespaceId, Revision, TaskId, TaskObjective, TaskResult,
    TaskStatus, Term, TimestampMs,
};
use agent_broker_protocol::{
    BrokerRequest, BrokerRequestDispatcher, ClaimTaskRequest, CompleteTaskRequest,
    DeclaredCapabilities, DispatchResult, EnsureConsumerGroupRequest, EnsureNamespaceRequest,
    HealthRequest, HeartbeatRequest, JoinConsumerGroupRequest, LeaveConsumerGroupRequest,
    PublishTaskRequest, RenewTaskLeaseRequest, RequestId,
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

struct Fixture {
    dispatcher: BrokerRequestDispatcher<MemoryConsensus>,
    namespace_id: NamespaceId,
    group_id: ConsumerGroupId,
    member_id: MemberId,
    task_id: TaskId,
}

fn fixture() -> Result<Fixture, Box<dyn Error>> {
    let service =
        BrokerApplicationService::new(MemoryConsensus::default(), BrokerCapacityPolicy::default());
    Ok(Fixture {
        dispatcher: BrokerRequestDispatcher::new(service),
        namespace_id: NamespaceId::new("project-a")?,
        group_id: ConsumerGroupId::new("engineering")?,
        member_id: MemberId::new("worker-a")?,
        task_id: TaskId::new("task-1")?,
    })
}

fn request_id(value: &str) -> Result<RequestId, Box<dyn Error>> {
    Ok(RequestId::new(value)?)
}

fn bootstrap_group_and_task(fixture: &mut Fixture) -> Result<Generation, Box<dyn Error>> {
    let DispatchResult::Health(health) = fixture.dispatcher.dispatch(
        BrokerRequest::Health(HealthRequest {
            request_id: request_id("health")?,
        }),
        TimestampMs::new(100),
    )?
    else {
        return Err("health request returned the wrong result variant".into());
    };
    assert_eq!(health.term, Term::INITIAL);

    let DispatchResult::Namespace(_) = fixture.dispatcher.dispatch(
        BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
            request_id: request_id("namespace")?,
            namespace_id: fixture.namespace_id.clone(),
        }),
        TimestampMs::new(200),
    )?
    else {
        return Err("namespace request returned the wrong result variant".into());
    };

    let DispatchResult::ConsumerGroup(_) = fixture.dispatcher.dispatch(
        BrokerRequest::EnsureConsumerGroup(EnsureConsumerGroupRequest {
            request_id: request_id("group-ensure")?,
            namespace_id: fixture.namespace_id.clone(),
            group_id: fixture.group_id.clone(),
        }),
        TimestampMs::new(300),
    )?
    else {
        return Err("group ensure returned the wrong result variant".into());
    };

    let DispatchResult::ConsumerGroup(joined) = fixture.dispatcher.dispatch(
        BrokerRequest::JoinConsumerGroup(JoinConsumerGroupRequest {
            request_id: request_id("group-join")?,
            group_id: fixture.group_id.clone(),
            member_id: fixture.member_id.clone(),
            capabilities: DeclaredCapabilities::new(["code", "review", "code"])?,
        }),
        TimestampMs::new(1_000),
    )?
    else {
        return Err("group join returned the wrong result variant".into());
    };
    assert_eq!(joined.member_count, 1);

    let DispatchResult::TaskPublished(published) = fixture.dispatcher.dispatch(
        BrokerRequest::PublishTask(PublishTaskRequest {
            request_id: request_id("publish")?,
            namespace_id: fixture.namespace_id.clone(),
            task_id: fixture.task_id.clone(),
            objective: TaskObjective::new("Implement Rust protocol parity")?,
        }),
        TimestampMs::new(1_100),
    )?
    else {
        return Err("publish returned the wrong result variant".into());
    };
    assert_eq!(published.status, TaskStatus::Queued);

    let DispatchResult::Heartbeat(heartbeat) = fixture.dispatcher.dispatch(
        BrokerRequest::Heartbeat(HeartbeatRequest {
            request_id: request_id("heartbeat")?,
            group_id: fixture.group_id.clone(),
            member_id: fixture.member_id.clone(),
            expected_generation: joined.generation,
        }),
        TimestampMs::new(1_500),
    )?
    else {
        return Err("heartbeat returned the wrong result variant".into());
    };
    assert_eq!(heartbeat.generation, joined.generation);
    Ok(joined.generation)
}

fn run_lease_lifecycle(
    fixture: &mut Fixture,
    generation: Generation,
) -> Result<LeaseEpoch, Box<dyn Error>> {
    let lease_id = LeaseId::new("lease-1")?;
    let DispatchResult::TaskClaimed(claimed) = fixture.dispatcher.dispatch(
        BrokerRequest::ClaimTask(ClaimTaskRequest {
            request_id: request_id("claim")?,
            group_id: fixture.group_id.clone(),
            member_id: fixture.member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: generation,
            lease_id: lease_id.clone(),
            lease_duration: LeaseDurationMs::new(1_000)?,
        }),
        TimestampMs::new(2_000),
    )?
    else {
        return Err("claim returned the wrong result variant".into());
    };
    assert_eq!(claimed.task_id.as_ref(), Some(&fixture.task_id));
    assert_eq!(claimed.lease_expires_at_ms, Some(TimestampMs::new(3_000)));
    let lease_epoch = claimed.lease_epoch.ok_or("claim must return lease epoch")?;

    let DispatchResult::TaskLeaseRenewed(renewed) = fixture.dispatcher.dispatch(
        BrokerRequest::RenewTaskLease(RenewTaskLeaseRequest {
            request_id: request_id("renew")?,
            task_id: fixture.task_id.clone(),
            group_id: fixture.group_id.clone(),
            member_id: fixture.member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: generation,
            expected_lease_epoch: lease_epoch,
            lease_id: lease_id.clone(),
            lease_duration: LeaseDurationMs::new(1_000)?,
        }),
        TimestampMs::new(2_500),
    )?
    else {
        return Err("renew returned the wrong result variant".into());
    };
    assert_eq!(renewed.lease_expires_at_ms, TimestampMs::new(3_500));
    assert_eq!(renewed.lease_epoch, lease_epoch);

    let DispatchResult::TaskCompleted(completed) = fixture.dispatcher.dispatch(
        BrokerRequest::CompleteTask(CompleteTaskRequest {
            request_id: request_id("complete")?,
            task_id: fixture.task_id.clone(),
            group_id: fixture.group_id.clone(),
            member_id: fixture.member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: generation,
            expected_lease_epoch: lease_epoch,
            lease_id,
            result: TaskResult::new("done")?,
        }),
        TimestampMs::new(3_000),
    )?
    else {
        return Err("complete returned the wrong result variant".into());
    };
    assert_eq!(completed.status, TaskStatus::Completed);
    Ok(lease_epoch)
}

fn leave_group(fixture: &mut Fixture, generation: Generation) -> Result<(), Box<dyn Error>> {
    let DispatchResult::ConsumerGroup(left) = fixture.dispatcher.dispatch(
        BrokerRequest::LeaveConsumerGroup(LeaveConsumerGroupRequest {
            request_id: request_id("leave")?,
            group_id: fixture.group_id.clone(),
            member_id: fixture.member_id.clone(),
            expected_generation: generation,
        }),
        TimestampMs::new(4_000),
    )?
    else {
        return Err("leave returned the wrong result variant".into());
    };
    assert_eq!(left.member_count, 0);
    assert!(left.generation > generation);
    Ok(())
}

#[test]
fn dispatcher_matches_python_request_to_application_semantics() -> Result<(), Box<dyn Error>> {
    let mut fixture = fixture()?;
    let generation = bootstrap_group_and_task(&mut fixture)?;
    let _lease_epoch = run_lease_lifecycle(&mut fixture, generation)?;
    leave_group(&mut fixture, generation)
}

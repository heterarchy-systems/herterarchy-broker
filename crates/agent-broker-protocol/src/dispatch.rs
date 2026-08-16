use agent_broker_application::{
    BrokerApplicationService, BrokerError, BrokerErrorCode, BrokerHealth, ClaimTaskInput,
    CompleteTaskInput, ConsensusAdapter, RenewTaskLeaseInput,
};
use agent_broker_domain::TimestampMs;
use agent_broker_domain::results::{
    ConsumerGroupResult, HeartbeatResult, NamespaceResult, TaskClaimResult, TaskCompletedResult,
    TaskLeaseRenewedResult, TaskPublishedResult,
};

use crate::BrokerRequest;

/// Typed result returned after a validated protocol request reaches the application boundary.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DispatchResult {
    Health(BrokerHealth),
    Namespace(NamespaceResult),
    TaskPublished(TaskPublishedResult),
    ConsumerGroup(ConsumerGroupResult),
    Heartbeat(HeartbeatResult),
    TaskClaimed(TaskClaimResult),
    TaskLeaseRenewed(TaskLeaseRenewedResult),
    TaskCompleted(TaskCompletedResult),
}

/// Maps validated protocol requests into provider-neutral Broker application use cases.
///
/// The network/runtime boundary supplies one authoritative observation timestamp per request. The
/// dispatcher never reads wall-clock time itself, keeping protocol-to-application mapping fully
/// deterministic in tests and reusable by standalone or future replicated runtimes.
pub struct BrokerRequestDispatcher<C> {
    service: BrokerApplicationService<C>,
}

impl<C> BrokerRequestDispatcher<C>
where
    C: ConsensusAdapter,
{
    #[must_use]
    pub const fn new(service: BrokerApplicationService<C>) -> Self {
        Self { service }
    }

    #[must_use]
    pub fn into_service(self) -> BrokerApplicationService<C> {
        self.service
    }

    /// Borrow the application service for runtime-owned maintenance proposals that are deliberately
    /// not exposed as protocol-v1 operations.
    #[must_use]
    pub fn application_service_mut(&mut self) -> &mut BrokerApplicationService<C> {
        &mut self.service
    }

    /// Dispatch one validated protocol request using the runtime-supplied observation time.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when application, fencing, capacity, or consensus validation rejects
    /// the request.
    pub fn dispatch(
        &mut self,
        request: BrokerRequest,
        observed_at_ms: TimestampMs,
    ) -> Result<DispatchResult, BrokerError> {
        match request {
            BrokerRequest::Health(_) => Ok(DispatchResult::Health(self.service.health())),
            BrokerRequest::EnsureNamespace(request) => self
                .service
                .ensure_namespace(request.namespace_id)
                .map(DispatchResult::Namespace),
            BrokerRequest::PublishTask(request) => self
                .service
                .publish_task(
                    request.namespace_id,
                    request.task_id,
                    request.objective,
                    observed_at_ms,
                )
                .map(DispatchResult::TaskPublished),
            BrokerRequest::EnsureConsumerGroup(request) => self
                .service
                .ensure_consumer_group(request.namespace_id, request.group_id)
                .map(DispatchResult::ConsumerGroup),
            BrokerRequest::JoinConsumerGroup(request) => {
                let capabilities = request.capabilities.into_normalized().map_err(|error| {
                    BrokerError::new(BrokerErrorCode::InvalidRequest, error.to_string())
                })?;
                self.service
                    .join_consumer_group(
                        request.group_id,
                        request.member_id,
                        capabilities,
                        observed_at_ms,
                    )
                    .map(DispatchResult::ConsumerGroup)
            }
            BrokerRequest::Heartbeat(request) => self
                .service
                .heartbeat(
                    request.group_id,
                    request.member_id,
                    request.expected_generation,
                    observed_at_ms,
                )
                .map(DispatchResult::Heartbeat),
            BrokerRequest::LeaveConsumerGroup(request) => self
                .service
                .leave_consumer_group(
                    request.group_id,
                    request.member_id,
                    request.expected_generation,
                )
                .map(DispatchResult::ConsumerGroup),
            BrokerRequest::ClaimTask(request) => self
                .service
                .claim_task(ClaimTaskInput {
                    group_id: request.group_id,
                    member_id: request.member_id,
                    expected_term: request.expected_term,
                    expected_generation: request.expected_generation,
                    lease_id: request.lease_id,
                    now_ms: observed_at_ms,
                    lease_duration: request.lease_duration,
                })
                .map(DispatchResult::TaskClaimed),
            BrokerRequest::RenewTaskLease(request) => self
                .service
                .renew_task_lease(RenewTaskLeaseInput {
                    task_id: request.task_id,
                    group_id: request.group_id,
                    member_id: request.member_id,
                    expected_term: request.expected_term,
                    expected_generation: request.expected_generation,
                    expected_lease_epoch: request.expected_lease_epoch,
                    lease_id: request.lease_id,
                    now_ms: observed_at_ms,
                    lease_duration: request.lease_duration,
                })
                .map(DispatchResult::TaskLeaseRenewed),
            BrokerRequest::CompleteTask(request) => self
                .service
                .complete_task(CompleteTaskInput {
                    task_id: request.task_id,
                    group_id: request.group_id,
                    member_id: request.member_id,
                    expected_term: request.expected_term,
                    expected_generation: request.expected_generation,
                    expected_lease_epoch: request.expected_lease_epoch,
                    lease_id: request.lease_id,
                    result: request.result,
                    completed_at_ms: observed_at_ms,
                })
                .map(DispatchResult::TaskCompleted),
        }
    }
}

use std::error::Error;
use std::fmt;

use agent_broker_application::{BrokerError, BrokerErrorCode};
use agent_broker_domain::results::{
    BrokerMutationResult, CompletedTasksPrunedResult, ConsumerGroupResult, HeartbeatResult,
    MutationMetadata, NamespaceResult, StaleMembersReapedResult, TaskClaimResult,
    TaskCompletedResult, TaskLeaseRenewedResult, TaskPublishedResult, TermAdvancedResult,
};
use agent_broker_domain::{
    ConsumerGroupId, Generation, LeaseEpoch, LeaseId, MemberId, NamespaceId, Revision, TaskId,
    TaskObjective, TaskStatus, Term, TimestampMs,
};
use serde::{Deserialize, Serialize};

/// Versioned response returned by the Raft state-machine apply path.
///
/// Business/domain failures are application data, not Raft transport/storage failures. Keeping them
/// inside the committed response means the caller observes the same deterministic result that every
/// replica produced while applying the committed command.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "payload", rename_all = "snake_case")]
pub enum ReplicatedBrokerResponseV1 {
    /// Response placeholder for committed Raft blank/membership entries.
    RaftControl,
    Success(ReplicatedBrokerMutationResultV1),
    ApplicationError(ReplicatedBrokerErrorV1),
}

impl ReplicatedBrokerResponseV1 {
    /// Convert the deterministic Broker apply result into Raft application response data.
    pub fn from_application_result(
        result: Result<BrokerMutationResult, BrokerError>,
    ) -> Result<Self, ReplicatedResponseError> {
        match result {
            Ok(result) => Ok(Self::Success(result.try_into()?)),
            Err(error) => Ok(Self::ApplicationError(error.into())),
        }
    }

    /// Recover the application-layer result after a committed Raft write completes.
    pub fn into_application_result(
        self,
    ) -> Result<Result<BrokerMutationResult, BrokerError>, ReplicatedResponseError> {
        match self {
            Self::RaftControl => Err(ReplicatedResponseError(
                "Raft control entry does not contain a Broker application result".to_owned(),
            )),
            Self::Success(result) => Ok(Ok(result.try_into()?)),
            Self::ApplicationError(error) => Ok(Err(error.into())),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicatedBrokerErrorV1 {
    code: ReplicatedBrokerErrorCodeV1,
    message: String,
}

impl From<BrokerError> for ReplicatedBrokerErrorV1 {
    fn from(error: BrokerError) -> Self {
        Self {
            code: error.code().into(),
            message: error.message().to_owned(),
        }
    }
}

impl From<ReplicatedBrokerErrorV1> for BrokerError {
    fn from(error: ReplicatedBrokerErrorV1) -> Self {
        Self::new(error.code.into(), error.message)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ReplicatedBrokerErrorCodeV1 {
    InvalidRequest,
    NotFound,
    Conflict,
    CapacityExceeded,
    StaleFence,
    PersistenceError,
    TransportError,
    InternalError,
}

impl From<BrokerErrorCode> for ReplicatedBrokerErrorCodeV1 {
    fn from(code: BrokerErrorCode) -> Self {
        match code {
            BrokerErrorCode::InvalidRequest => Self::InvalidRequest,
            BrokerErrorCode::NotFound => Self::NotFound,
            BrokerErrorCode::Conflict => Self::Conflict,
            BrokerErrorCode::CapacityExceeded => Self::CapacityExceeded,
            BrokerErrorCode::StaleFence => Self::StaleFence,
            BrokerErrorCode::PersistenceError => Self::PersistenceError,
            BrokerErrorCode::TransportError => Self::TransportError,
            BrokerErrorCode::InternalError => Self::InternalError,
        }
    }
}

impl From<ReplicatedBrokerErrorCodeV1> for BrokerErrorCode {
    fn from(code: ReplicatedBrokerErrorCodeV1) -> Self {
        match code {
            ReplicatedBrokerErrorCodeV1::InvalidRequest => Self::InvalidRequest,
            ReplicatedBrokerErrorCodeV1::NotFound => Self::NotFound,
            ReplicatedBrokerErrorCodeV1::Conflict => Self::Conflict,
            ReplicatedBrokerErrorCodeV1::CapacityExceeded => Self::CapacityExceeded,
            ReplicatedBrokerErrorCodeV1::StaleFence => Self::StaleFence,
            ReplicatedBrokerErrorCodeV1::PersistenceError => Self::PersistenceError,
            ReplicatedBrokerErrorCodeV1::TransportError => Self::TransportError,
            ReplicatedBrokerErrorCodeV1::InternalError => Self::InternalError,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result_type", rename_all = "snake_case")]
pub enum ReplicatedBrokerMutationResultV1 {
    Namespace {
        metadata: ReplicatedMutationMetadataV1,
        namespace_id: String,
        namespace_revision: u64,
    },
    TaskPublished {
        metadata: ReplicatedMutationMetadataV1,
        task_id: String,
        task_revision: u64,
        status: ReplicatedTaskStatusV1,
    },
    ConsumerGroup {
        metadata: ReplicatedMutationMetadataV1,
        group_id: String,
        generation: u64,
        group_revision: u64,
        member_count: u64,
    },
    Heartbeat {
        metadata: ReplicatedMutationMetadataV1,
        group_id: String,
        member_id: String,
        generation: u64,
        member_revision: u64,
    },
    StaleMembersReaped {
        metadata: ReplicatedMutationMetadataV1,
        reaped_count: u64,
        affected_group_count: u64,
    },
    TaskClaim {
        metadata: ReplicatedMutationMetadataV1,
        task_id: Option<String>,
        objective: Option<String>,
        task_revision: Option<u64>,
        lease_id: Option<String>,
        lease_epoch: Option<u64>,
        lease_expires_at_ms: Option<u64>,
        generation: u64,
    },
    TaskLeaseRenewed {
        metadata: ReplicatedMutationMetadataV1,
        task_id: String,
        task_revision: u64,
        lease_id: String,
        lease_epoch: u64,
        lease_expires_at_ms: u64,
        generation: u64,
    },
    TaskCompleted {
        metadata: ReplicatedMutationMetadataV1,
        task_id: String,
        task_revision: u64,
        status: ReplicatedTaskStatusV1,
    },
    CompletedTasksPruned {
        metadata: ReplicatedMutationMetadataV1,
        pruned_count: u64,
    },
    TermAdvanced {
        metadata: ReplicatedMutationMetadataV1,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicatedMutationMetadataV1 {
    term: u64,
    revision: u64,
}

impl From<MutationMetadata> for ReplicatedMutationMetadataV1 {
    fn from(metadata: MutationMetadata) -> Self {
        Self {
            term: metadata.term.get(),
            revision: metadata.revision.get(),
        }
    }
}

impl TryFrom<ReplicatedMutationMetadataV1> for MutationMetadata {
    type Error = ReplicatedResponseError;

    fn try_from(metadata: ReplicatedMutationMetadataV1) -> Result<Self, Self::Error> {
        Ok(Self {
            term: Term::new(metadata.term).map_err(validation_error)?,
            revision: Revision::new(metadata.revision),
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicatedTaskStatusV1 {
    Queued,
    Leased,
    Completed,
}

impl From<TaskStatus> for ReplicatedTaskStatusV1 {
    fn from(status: TaskStatus) -> Self {
        match status {
            TaskStatus::Queued => Self::Queued,
            TaskStatus::Leased => Self::Leased,
            TaskStatus::Completed => Self::Completed,
        }
    }
}

impl From<ReplicatedTaskStatusV1> for TaskStatus {
    fn from(status: ReplicatedTaskStatusV1) -> Self {
        match status {
            ReplicatedTaskStatusV1::Queued => Self::Queued,
            ReplicatedTaskStatusV1::Leased => Self::Leased,
            ReplicatedTaskStatusV1::Completed => Self::Completed,
        }
    }
}

impl TryFrom<BrokerMutationResult> for ReplicatedBrokerMutationResultV1 {
    type Error = ReplicatedResponseError;

    fn try_from(result: BrokerMutationResult) -> Result<Self, Self::Error> {
        match result {
            BrokerMutationResult::Namespace(result) => Ok(Self::Namespace {
                metadata: result.metadata.into(),
                namespace_id: result.namespace_id.as_str().to_owned(),
                namespace_revision: result.namespace_revision.get(),
            }),
            BrokerMutationResult::TaskPublished(result) => Ok(Self::TaskPublished {
                metadata: result.metadata.into(),
                task_id: result.task_id.as_str().to_owned(),
                task_revision: result.task_revision.get(),
                status: result.status.into(),
            }),
            BrokerMutationResult::ConsumerGroup(result) => Ok(Self::ConsumerGroup {
                metadata: result.metadata.into(),
                group_id: result.group_id.as_str().to_owned(),
                generation: result.generation.get(),
                group_revision: result.group_revision.get(),
                member_count: usize_to_u64(result.member_count, "member_count")?,
            }),
            BrokerMutationResult::Heartbeat(result) => Ok(Self::Heartbeat {
                metadata: result.metadata.into(),
                group_id: result.group_id.as_str().to_owned(),
                member_id: result.member_id.as_str().to_owned(),
                generation: result.generation.get(),
                member_revision: result.member_revision.get(),
            }),
            BrokerMutationResult::StaleMembersReaped(result) => Ok(Self::StaleMembersReaped {
                metadata: result.metadata.into(),
                reaped_count: usize_to_u64(result.reaped_count, "reaped_count")?,
                affected_group_count: usize_to_u64(
                    result.affected_group_count,
                    "affected_group_count",
                )?,
            }),
            BrokerMutationResult::TaskClaim(result) => Ok(Self::TaskClaim {
                metadata: result.metadata.into(),
                task_id: result.task_id.map(|value| value.as_str().to_owned()),
                objective: result.objective.map(|value| value.as_str().to_owned()),
                task_revision: result.task_revision.map(Revision::get),
                lease_id: result.lease_id.map(|value| value.as_str().to_owned()),
                lease_epoch: result.lease_epoch.map(LeaseEpoch::get),
                lease_expires_at_ms: result.lease_expires_at_ms.map(TimestampMs::get),
                generation: result.generation.get(),
            }),
            BrokerMutationResult::TaskLeaseRenewed(result) => Ok(Self::TaskLeaseRenewed {
                metadata: result.metadata.into(),
                task_id: result.task_id.as_str().to_owned(),
                task_revision: result.task_revision.get(),
                lease_id: result.lease_id.as_str().to_owned(),
                lease_epoch: result.lease_epoch.get(),
                lease_expires_at_ms: result.lease_expires_at_ms.get(),
                generation: result.generation.get(),
            }),
            BrokerMutationResult::TaskCompleted(result) => Ok(Self::TaskCompleted {
                metadata: result.metadata.into(),
                task_id: result.task_id.as_str().to_owned(),
                task_revision: result.task_revision.get(),
                status: result.status.into(),
            }),
            BrokerMutationResult::CompletedTasksPruned(result) => Ok(Self::CompletedTasksPruned {
                metadata: result.metadata.into(),
                pruned_count: usize_to_u64(result.pruned_count, "pruned_count")?,
            }),
            BrokerMutationResult::TermAdvanced(result) => Ok(Self::TermAdvanced {
                metadata: result.metadata.into(),
            }),
        }
    }
}

impl TryFrom<ReplicatedBrokerMutationResultV1> for BrokerMutationResult {
    type Error = ReplicatedResponseError;

    fn try_from(result: ReplicatedBrokerMutationResultV1) -> Result<Self, Self::Error> {
        match result {
            ReplicatedBrokerMutationResultV1::Namespace {
                metadata,
                namespace_id,
                namespace_revision,
            } => Ok(Self::Namespace(NamespaceResult {
                metadata: metadata.try_into()?,
                namespace_id: NamespaceId::new(namespace_id).map_err(validation_error)?,
                namespace_revision: Revision::new(namespace_revision),
            })),
            ReplicatedBrokerMutationResultV1::TaskPublished {
                metadata,
                task_id,
                task_revision,
                status,
            } => Ok(Self::TaskPublished(TaskPublishedResult {
                metadata: metadata.try_into()?,
                task_id: TaskId::new(task_id).map_err(validation_error)?,
                task_revision: Revision::new(task_revision),
                status: status.into(),
            })),
            ReplicatedBrokerMutationResultV1::ConsumerGroup {
                metadata,
                group_id,
                generation,
                group_revision,
                member_count,
            } => Ok(Self::ConsumerGroup(ConsumerGroupResult {
                metadata: metadata.try_into()?,
                group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
                generation: Generation::new(generation),
                group_revision: Revision::new(group_revision),
                member_count: u64_to_usize(member_count, "member_count")?,
            })),
            ReplicatedBrokerMutationResultV1::Heartbeat {
                metadata,
                group_id,
                member_id,
                generation,
                member_revision,
            } => Ok(Self::Heartbeat(HeartbeatResult {
                metadata: metadata.try_into()?,
                group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
                member_id: MemberId::new(member_id).map_err(validation_error)?,
                generation: Generation::new(generation),
                member_revision: Revision::new(member_revision),
            })),
            ReplicatedBrokerMutationResultV1::StaleMembersReaped {
                metadata,
                reaped_count,
                affected_group_count,
            } => Ok(Self::StaleMembersReaped(StaleMembersReapedResult {
                metadata: metadata.try_into()?,
                reaped_count: u64_to_usize(reaped_count, "reaped_count")?,
                affected_group_count: u64_to_usize(affected_group_count, "affected_group_count")?,
            })),
            ReplicatedBrokerMutationResultV1::TaskClaim {
                metadata,
                task_id,
                objective,
                task_revision,
                lease_id,
                lease_epoch,
                lease_expires_at_ms,
                generation,
            } => {
                validate_claim_option_shape(
                    &task_id,
                    &objective,
                    &task_revision,
                    &lease_id,
                    &lease_epoch,
                    &lease_expires_at_ms,
                )?;
                Ok(Self::TaskClaim(TaskClaimResult {
                    metadata: metadata.try_into()?,
                    task_id: task_id
                        .map(TaskId::new)
                        .transpose()
                        .map_err(validation_error)?,
                    objective: objective
                        .map(TaskObjective::new)
                        .transpose()
                        .map_err(validation_error)?,
                    task_revision: task_revision.map(Revision::new),
                    lease_id: lease_id
                        .map(LeaseId::new)
                        .transpose()
                        .map_err(validation_error)?,
                    lease_epoch: lease_epoch.map(LeaseEpoch::new),
                    lease_expires_at_ms: lease_expires_at_ms.map(TimestampMs::new),
                    generation: Generation::new(generation),
                }))
            }
            ReplicatedBrokerMutationResultV1::TaskLeaseRenewed {
                metadata,
                task_id,
                task_revision,
                lease_id,
                lease_epoch,
                lease_expires_at_ms,
                generation,
            } => Ok(Self::TaskLeaseRenewed(TaskLeaseRenewedResult {
                metadata: metadata.try_into()?,
                task_id: TaskId::new(task_id).map_err(validation_error)?,
                task_revision: Revision::new(task_revision),
                lease_id: LeaseId::new(lease_id).map_err(validation_error)?,
                lease_epoch: LeaseEpoch::new(lease_epoch),
                lease_expires_at_ms: TimestampMs::new(lease_expires_at_ms),
                generation: Generation::new(generation),
            })),
            ReplicatedBrokerMutationResultV1::TaskCompleted {
                metadata,
                task_id,
                task_revision,
                status,
            } => Ok(Self::TaskCompleted(TaskCompletedResult {
                metadata: metadata.try_into()?,
                task_id: TaskId::new(task_id).map_err(validation_error)?,
                task_revision: Revision::new(task_revision),
                status: status.into(),
            })),
            ReplicatedBrokerMutationResultV1::CompletedTasksPruned {
                metadata,
                pruned_count,
            } => Ok(Self::CompletedTasksPruned(CompletedTasksPrunedResult {
                metadata: metadata.try_into()?,
                pruned_count: u64_to_usize(pruned_count, "pruned_count")?,
            })),
            ReplicatedBrokerMutationResultV1::TermAdvanced { metadata } => {
                Ok(Self::TermAdvanced(TermAdvancedResult {
                    metadata: metadata.try_into()?,
                }))
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReplicatedResponseError(String);

impl fmt::Display for ReplicatedResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ReplicatedResponseError {}

fn validate_claim_option_shape(
    task_id: &Option<String>,
    objective: &Option<String>,
    task_revision: &Option<u64>,
    lease_id: &Option<String>,
    lease_epoch: &Option<u64>,
    lease_expires_at_ms: &Option<u64>,
) -> Result<(), ReplicatedResponseError> {
    let presence = [
        task_id.is_some(),
        objective.is_some(),
        task_revision.is_some(),
        lease_id.is_some(),
        lease_epoch.is_some(),
        lease_expires_at_ms.is_some(),
    ];
    if presence.iter().all(|present| *present) || presence.iter().all(|present| !*present) {
        return Ok(());
    }
    Err(ReplicatedResponseError(
        "replicated TaskClaim response contains a partial lease payload".to_owned(),
    ))
}

fn validation_error(error: impl fmt::Display) -> ReplicatedResponseError {
    ReplicatedResponseError(format!(
        "replicated Broker response failed domain validation: {error}"
    ))
}

fn usize_to_u64(value: usize, field: &'static str) -> Result<u64, ReplicatedResponseError> {
    u64::try_from(value)
        .map_err(|_| ReplicatedResponseError(format!("{field} cannot be represented as u64")))
}

fn u64_to_usize(value: u64, field: &'static str) -> Result<usize, ReplicatedResponseError> {
    usize::try_from(value)
        .map_err(|_| ReplicatedResponseError(format!("{field} exceeds platform usize")))
}

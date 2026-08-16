use agent_broker_application::{BrokerError, BrokerErrorCode};
use agent_broker_domain::{
    ConsumerGroupId, Generation, LeaseEpoch, LeaseId, MemberId, NamespaceId, Revision, TaskId,
    TaskObjective, TaskStatus, Term, TimestampMs,
};

use crate::{DispatchResult, RequestId};

/// Typed protocol-v1 success payload before wire serialization.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SuccessPayload {
    Health {
        protocol_version: u32,
        term: Term,
        revision: Revision,
    },
    Namespace {
        term: Term,
        revision: Revision,
        namespace_id: NamespaceId,
        namespace_revision: Revision,
    },
    TaskPublished {
        term: Term,
        revision: Revision,
        task_id: TaskId,
        task_revision: Revision,
        status: TaskStatus,
    },
    ConsumerGroup {
        term: Term,
        revision: Revision,
        group_id: ConsumerGroupId,
        generation: Generation,
        group_revision: Revision,
        member_count: usize,
    },
    Heartbeat {
        term: Term,
        revision: Revision,
        group_id: ConsumerGroupId,
        member_id: MemberId,
        generation: Generation,
        member_revision: Revision,
    },
    TaskClaimed {
        term: Term,
        revision: Revision,
        task_id: Option<TaskId>,
        objective: Option<TaskObjective>,
        task_revision: Option<Revision>,
        lease_id: Option<LeaseId>,
        lease_epoch: Option<LeaseEpoch>,
        lease_expires_at_ms: Option<TimestampMs>,
        generation: Generation,
    },
    TaskLeaseRenewed {
        term: Term,
        revision: Revision,
        task_id: TaskId,
        task_revision: Revision,
        lease_id: LeaseId,
        lease_epoch: LeaseEpoch,
        lease_expires_at_ms: TimestampMs,
        generation: Generation,
    },
    TaskCompleted {
        term: Term,
        revision: Revision,
        task_id: TaskId,
        task_revision: Revision,
        status: TaskStatus,
    },
}

impl From<DispatchResult> for SuccessPayload {
    fn from(result: DispatchResult) -> Self {
        match result {
            DispatchResult::Health(result) => Self::Health {
                protocol_version: result.protocol_version,
                term: result.term,
                revision: result.revision,
            },
            DispatchResult::Namespace(result) => Self::Namespace {
                term: result.metadata.term,
                revision: result.metadata.revision,
                namespace_id: result.namespace_id,
                namespace_revision: result.namespace_revision,
            },
            DispatchResult::TaskPublished(result) => Self::TaskPublished {
                term: result.metadata.term,
                revision: result.metadata.revision,
                task_id: result.task_id,
                task_revision: result.task_revision,
                status: result.status,
            },
            DispatchResult::ConsumerGroup(result) => Self::ConsumerGroup {
                term: result.metadata.term,
                revision: result.metadata.revision,
                group_id: result.group_id,
                generation: result.generation,
                group_revision: result.group_revision,
                member_count: result.member_count,
            },
            DispatchResult::Heartbeat(result) => Self::Heartbeat {
                term: result.metadata.term,
                revision: result.metadata.revision,
                group_id: result.group_id,
                member_id: result.member_id,
                generation: result.generation,
                member_revision: result.member_revision,
            },
            DispatchResult::TaskClaimed(result) => Self::TaskClaimed {
                term: result.metadata.term,
                revision: result.metadata.revision,
                task_id: result.task_id,
                objective: result.objective,
                task_revision: result.task_revision,
                lease_id: result.lease_id,
                lease_epoch: result.lease_epoch,
                lease_expires_at_ms: result.lease_expires_at_ms,
                generation: result.generation,
            },
            DispatchResult::TaskLeaseRenewed(result) => Self::TaskLeaseRenewed {
                term: result.metadata.term,
                revision: result.metadata.revision,
                task_id: result.task_id,
                task_revision: result.task_revision,
                lease_id: result.lease_id,
                lease_epoch: result.lease_epoch,
                lease_expires_at_ms: result.lease_expires_at_ms,
                generation: result.generation,
            },
            DispatchResult::TaskCompleted(result) => Self::TaskCompleted {
                term: result.metadata.term,
                revision: result.metadata.revision,
                task_id: result.task_id,
                task_revision: result.task_revision,
                status: result.status,
            },
        }
    }
}

/// Stable protocol error payload independent of the wire codec.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ErrorPayload {
    pub code: BrokerErrorCode,
    pub message: String,
}

impl From<BrokerError> for ErrorPayload {
    fn from(error: BrokerError) -> Self {
        Self {
            code: error.code(),
            message: error.message().to_owned(),
        }
    }
}

/// Typed response envelope used by the runtime before serialization.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BrokerResponse {
    Success {
        request_id: RequestId,
        result: SuccessPayload,
    },
    Error {
        request_id: RequestId,
        error: ErrorPayload,
    },
}

impl BrokerResponse {
    #[must_use]
    pub fn success(request_id: RequestId, result: DispatchResult) -> Self {
        Self::Success {
            request_id,
            result: result.into(),
        }
    }

    #[must_use]
    pub fn error(request_id: RequestId, error: BrokerError) -> Self {
        Self::Error {
            request_id,
            error: error.into(),
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        match self {
            Self::Success { request_id, .. } | Self::Error { request_id, .. } => request_id,
        }
    }
}

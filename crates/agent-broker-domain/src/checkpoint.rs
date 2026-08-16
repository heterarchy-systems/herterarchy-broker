use std::error::Error;
use std::fmt;

use crate::{
    Capabilities, ConsumerGroupId, Generation, LeaseEpoch, LeaseId, MemberId, NamespaceId,
    Revision, TaskId, TaskObjective, TaskResult, Term, TimestampMs,
};

/// Durable logical Broker image independent of any storage encoding.
///
/// Storage and replication adapters serialize this contract rather than Broker-internal maps,
/// heaps, or indexes. Restoring it rebuilds derived indexes deterministically.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrokerCheckpoint {
    pub term: Term,
    pub revision: Revision,
    pub namespaces: Vec<NamespaceCheckpoint>,
    pub tasks: Vec<TaskCheckpoint>,
    pub groups: Vec<ConsumerGroupCheckpoint>,
}

/// Namespace record carried by a logical Broker checkpoint.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NamespaceCheckpoint {
    pub namespace_id: NamespaceId,
    pub revision: Revision,
}

/// Consumer Group record carried by a logical Broker checkpoint.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConsumerGroupCheckpoint {
    pub group_id: ConsumerGroupId,
    pub namespace_id: NamespaceId,
    pub generation: Generation,
    pub revision: Revision,
    pub members: Vec<MemberCheckpoint>,
}

/// Consumer Group member record carried by a logical Broker checkpoint.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemberCheckpoint {
    pub member_id: MemberId,
    pub capabilities: Capabilities,
    pub joined_at_ms: TimestampMs,
    pub last_heartbeat_at_ms: TimestampMs,
    pub revision: Revision,
}

/// Task record carried by a logical Broker checkpoint.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TaskCheckpoint {
    pub task_id: TaskId,
    pub namespace_id: NamespaceId,
    pub objective: TaskObjective,
    pub created_at_ms: TimestampMs,
    pub revision: Revision,
    pub state: TaskCheckpointState,
}

/// Type-safe persisted Task lifecycle payload.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TaskCheckpointState {
    Queued {
        lease_epoch: LeaseEpoch,
    },
    Leased {
        lease_id: LeaseId,
        owner_member_id: MemberId,
        group_id: ConsumerGroupId,
        generation: Generation,
        lease_epoch: LeaseEpoch,
        expires_at_ms: TimestampMs,
    },
    Completed {
        lease_id: LeaseId,
        owner_member_id: MemberId,
        group_id: ConsumerGroupId,
        generation: Generation,
        lease_epoch: LeaseEpoch,
        result: TaskResult,
        completed_at_ms: TimestampMs,
    },
}

/// Recovery rejection for a checkpoint that cannot represent valid authoritative Broker state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CheckpointError {
    ZeroEntityRevision {
        entity: &'static str,
    },
    DuplicateNamespace(NamespaceId),
    DuplicateTask(TaskId),
    DuplicateConsumerGroup(ConsumerGroupId),
    DuplicateMember(MemberId),
    DuplicateActiveLease(LeaseId),
    UnknownNamespace {
        entity: &'static str,
        namespace_id: NamespaceId,
    },
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroEntityRevision { entity } => {
                write!(formatter, "{entity} checkpoint revision must be positive")
            }
            Self::DuplicateNamespace(id) => {
                write!(formatter, "duplicate namespace {id} in checkpoint")
            }
            Self::DuplicateTask(id) => write!(formatter, "duplicate task {id} in checkpoint"),
            Self::DuplicateConsumerGroup(id) => {
                write!(formatter, "duplicate Consumer Group {id} in checkpoint")
            }
            Self::DuplicateMember(id) => write!(formatter, "duplicate member {id} in checkpoint"),
            Self::DuplicateActiveLease(id) => {
                write!(formatter, "duplicate active lease {id} in checkpoint")
            }
            Self::UnknownNamespace {
                entity,
                namespace_id,
            } => write!(
                formatter,
                "{entity} checkpoint references unknown namespace {namespace_id}"
            ),
        }
    }
}

impl Error for CheckpointError {}

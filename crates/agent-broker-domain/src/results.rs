use crate::{
    ConsumerGroupId, Generation, LeaseEpoch, LeaseId, MemberId, NamespaceId, Revision, TaskId,
    TaskObjective, TaskStatus, Term, TimestampMs,
};

/// Authoritative mutation metadata returned after every state-machine command.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct MutationMetadata {
    /// Current Broker term.
    pub term: Term,
    /// Current global Broker state revision.
    pub revision: Revision,
}

/// Result of ensuring a namespace.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NamespaceResult {
    /// Mutation metadata after the command.
    pub metadata: MutationMetadata,
    /// Namespace identity.
    pub namespace_id: NamespaceId,
    /// Namespace-local revision.
    pub namespace_revision: Revision,
}

/// Result of publishing or idempotently re-publishing a task.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TaskPublishedResult {
    /// Mutation metadata after the command.
    pub metadata: MutationMetadata,
    /// Task identity.
    pub task_id: TaskId,
    /// Task-local revision.
    pub task_revision: Revision,
    /// Current Task status.
    pub status: TaskStatus,
}

/// Result of ensuring a Consumer Group.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConsumerGroupResult {
    /// Mutation metadata after the command.
    pub metadata: MutationMetadata,
    /// Consumer Group identity.
    pub group_id: ConsumerGroupId,
    /// Membership generation.
    pub generation: Generation,
    /// Group-local revision.
    pub group_revision: Revision,
    /// Current member count.
    pub member_count: usize,
}

/// Result of an accepted/idempotent member heartbeat.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HeartbeatResult {
    /// Mutation metadata after the command.
    pub metadata: MutationMetadata,
    /// Consumer Group identity.
    pub group_id: ConsumerGroupId,
    /// Member identity.
    pub member_id: MemberId,
    /// Current Consumer Group generation.
    pub generation: Generation,
    /// Current member revision.
    pub member_revision: Revision,
}

/// Result of a bounded global stale-member reap.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StaleMembersReapedResult {
    /// Mutation metadata after the command.
    pub metadata: MutationMetadata,
    /// Number of members removed.
    pub reaped_count: usize,
    /// Number of Consumer Groups whose generation/revision advanced.
    pub affected_group_count: usize,
}

/// Result of a Task claim attempt. Empty optional fields mean no ready Task was available.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TaskClaimResult {
    pub metadata: MutationMetadata,
    pub task_id: Option<TaskId>,
    pub objective: Option<TaskObjective>,
    pub task_revision: Option<Revision>,
    pub lease_id: Option<LeaseId>,
    pub lease_epoch: Option<LeaseEpoch>,
    pub lease_expires_at_ms: Option<TimestampMs>,
    pub generation: Generation,
}

/// Result of renewing an active Task lease.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TaskLeaseRenewedResult {
    pub metadata: MutationMetadata,
    pub task_id: TaskId,
    pub task_revision: Revision,
    pub lease_id: LeaseId,
    pub lease_epoch: LeaseEpoch,
    pub lease_expires_at_ms: TimestampMs,
    pub generation: Generation,
}

/// Result of completing or idempotently re-completing a Task.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TaskCompletedResult {
    pub metadata: MutationMetadata,
    pub task_id: TaskId,
    pub task_revision: Revision,
    pub status: TaskStatus,
}

/// Result of bounded completed-Task pruning.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CompletedTasksPrunedResult {
    pub metadata: MutationMetadata,
    pub pruned_count: usize,
}

/// Result of a Broker term advancement.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct TermAdvancedResult {
    pub metadata: MutationMetadata,
}

/// Explicit changed-entity set emitted for persistence/read-model projection.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct StateChangeSet {
    /// Namespace records that changed.
    pub namespaces: Vec<NamespaceId>,
    /// Task records that changed.
    pub tasks: Vec<TaskId>,
    /// Task tombstones emitted by pruning; empty until pruning is ported.
    pub deleted_tasks: Vec<TaskId>,
    /// Consumer Group records that changed.
    pub groups: Vec<ConsumerGroupId>,
}

impl StateChangeSet {
    /// Return whether the command caused no authoritative entity mutation.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.namespaces.is_empty()
            && self.tasks.is_empty()
            && self.deleted_tasks.is_empty()
            && self.groups.is_empty()
    }
}

/// Typed result variants currently supported by the Rust state machine.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BrokerMutationResult {
    /// Namespace result.
    Namespace(NamespaceResult),
    /// Task publication result.
    TaskPublished(TaskPublishedResult),
    /// Consumer Group result.
    ConsumerGroup(ConsumerGroupResult),
    /// Member heartbeat result.
    Heartbeat(HeartbeatResult),
    /// Global stale-member reap result.
    StaleMembersReaped(StaleMembersReapedResult),
    /// Task claim result.
    TaskClaim(TaskClaimResult),
    /// Task lease-renewal result.
    TaskLeaseRenewed(TaskLeaseRenewedResult),
    /// Task completion result.
    TaskCompleted(TaskCompletedResult),
    /// Completed Task pruning result.
    CompletedTasksPruned(CompletedTasksPrunedResult),
    /// Broker term advancement result.
    TermAdvanced(TermAdvancedResult),
}

/// State-machine output pairing a typed result with the precise changed entity set.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AppliedMutation {
    /// Typed mutation result.
    pub result: BrokerMutationResult,
    /// Changed entity IDs for durable projection.
    pub changes: StateChangeSet,
}

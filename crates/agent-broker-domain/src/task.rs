use std::error::Error;
use std::fmt;

use crate::checkpoint::{TaskCheckpoint, TaskCheckpointState};
use crate::{
    ConsumerGroupId, ConsumerId, Generation, LeaseEpoch, LeaseId, NamespaceId, Revision, TaskId,
};

const MAX_OBJECTIVE_BYTES: usize = 16 * 1024;
const MAX_RESULT_BYTES: usize = 64 * 1024;

/// Milliseconds on the Broker's explicit command timeline.
///
/// The deterministic state machine never reads a wall clock directly; callers provide this value.
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TimestampMs(u64);

impl TimestampMs {
    /// Construct a non-negative millisecond timestamp.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw millisecond value for protocol or persistence boundaries.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Validation error for task objective/result text at the domain boundary.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TaskTextError {
    /// The text is empty after Unicode whitespace trimming.
    Empty { field: &'static str },
    /// UTF-8 encoded text exceeds the Python reference implementation's byte limit.
    TooLarge {
        field: &'static str,
        max_bytes: usize,
    },
}

impl fmt::Display for TaskTextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { field } => write!(formatter, "{field} must be a non-empty string"),
            Self::TooLarge { field, max_bytes } => {
                write!(formatter, "{field} exceeds the {max_bytes}-byte limit")
            }
        }
    }
}

impl Error for TaskTextError {}

/// Error returned when a task lifecycle transition violates the active lease or state invariant.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TaskTransitionError {
    /// The requested transition requires a queued task.
    NotQueued { status: TaskStatus },
    /// The requested transition requires an active lease.
    NotLeased { status: TaskStatus },
    /// The active lease belongs to a different Consumer Group.
    StaleGroup,
    /// The active lease belongs to a different member.
    StaleOwner,
    /// The Consumer Group generation no longer matches the lease.
    StaleGeneration,
    /// The lease epoch no longer matches the active lease.
    StaleLeaseEpoch,
    /// The lease identity no longer matches the active lease.
    StaleLeaseId,
    /// The lease expired at or before the observed command time.
    LeaseExpired,
    /// A completed task was completed again with a different lease or result.
    AlreadyCompleted,
    /// A monotonic revision/epoch could not advance safely.
    FencingValue(crate::FencingValueError),
}

impl fmt::Display for TaskTransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotQueued { status } => write!(formatter, "task is not queued: {status:?}"),
            Self::NotLeased { status } => write!(formatter, "task has no active lease: {status:?}"),
            Self::StaleGroup => formatter.write_str("task lease belongs to another Consumer Group"),
            Self::StaleOwner => formatter.write_str("task lease belongs to another member"),
            Self::StaleGeneration => formatter.write_str("task lease generation is stale"),
            Self::StaleLeaseEpoch => formatter.write_str("task lease epoch is stale"),
            Self::StaleLeaseId => formatter.write_str("task lease identity is stale"),
            Self::LeaseExpired => formatter.write_str("task lease has expired"),
            Self::AlreadyCompleted => formatter.write_str("task is already completed"),
            Self::FencingValue(error) => error.fmt(formatter),
        }
    }
}

impl Error for TaskTransitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::FencingValue(error) => Some(error),
            _ => None,
        }
    }
}

impl From<crate::FencingValueError> for TaskTransitionError {
    fn from(error: crate::FencingValueError) -> Self {
        Self::FencingValue(error)
    }
}

fn validate_text(
    value: String,
    field: &'static str,
    max_bytes: usize,
) -> Result<String, TaskTextError> {
    if value.trim().is_empty() {
        return Err(TaskTextError::Empty { field });
    }
    if value.len() > max_bytes {
        return Err(TaskTextError::TooLarge { field, max_bytes });
    }
    Ok(value)
}

/// Validated task objective, preserving the original UTF-8 text.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TaskObjective(String);

impl TaskObjective {
    /// Validate and construct a task objective.
    ///
    /// # Errors
    ///
    /// Returns [`TaskTextError`] when the value is blank or exceeds 16 KiB encoded as UTF-8.
    pub fn new(value: impl Into<String>) -> Result<Self, TaskTextError> {
        validate_text(value.into(), "objective", MAX_OBJECTIVE_BYTES).map(Self)
    }

    /// Borrow the original objective text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Validated completed-task result, preserving the original UTF-8 text.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TaskResult(String);

impl TaskResult {
    /// Validate and construct a task result.
    ///
    /// # Errors
    ///
    /// Returns [`TaskTextError`] when the value is blank or exceeds 64 KiB encoded as UTF-8.
    pub fn new(value: impl Into<String>) -> Result<Self, TaskTextError> {
        validate_text(value.into(), "result", MAX_RESULT_BYTES).map(Self)
    }

    /// Borrow the original result text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable externally observable task status.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TaskStatus {
    /// Ready to be claimed.
    Queued,
    /// Owned by an active fenced lease.
    Leased,
    /// Completed and retained for idempotency/history until pruning.
    Completed,
}

/// Queued-state payload. The last lease epoch is retained across requeue so reassignment can fence old holders.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct QueuedTask {
    lease_epoch: LeaseEpoch,
}

impl QueuedTask {
    #[must_use]
    pub(crate) const fn new(lease_epoch: LeaseEpoch) -> Self {
        Self { lease_epoch }
    }

    /// Return the latest lease epoch associated with this task.
    #[must_use]
    pub const fn lease_epoch(self) -> LeaseEpoch {
        self.lease_epoch
    }
}

/// Complete lease identity required whenever a Task is in the leased state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LeasedTask {
    lease_id: LeaseId,
    owner_member_id: ConsumerId,
    group_id: ConsumerGroupId,
    generation: Generation,
    lease_epoch: LeaseEpoch,
    expires_at_ms: TimestampMs,
}

/// Complete owned lease data needed to move a queued task into the leased state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LeaseGrant {
    lease_id: LeaseId,
    owner_member_id: ConsumerId,
    group_id: ConsumerGroupId,
    generation: Generation,
    expires_at_ms: TimestampMs,
}

impl LeaseGrant {
    /// Construct a lease grant after group/member admission has been validated by the state machine.
    #[must_use]
    pub const fn new(
        lease_id: LeaseId,
        owner_member_id: ConsumerId,
        group_id: ConsumerGroupId,
        generation: Generation,
        expires_at_ms: TimestampMs,
    ) -> Self {
        Self {
            lease_id,
            owner_member_id,
            group_id,
            generation,
            expires_at_ms,
        }
    }
}

/// Borrowed fencing identity used by renew/complete operations without cloning IDs.
#[derive(Debug, Copy, Clone)]
pub struct LeaseFence<'a> {
    lease_id: &'a LeaseId,
    owner_member_id: &'a ConsumerId,
    group_id: &'a ConsumerGroupId,
    generation: Generation,
    lease_epoch: LeaseEpoch,
    observed_at_ms: TimestampMs,
}

impl<'a> LeaseFence<'a> {
    /// Construct the lease fence carried by a deterministic command.
    #[must_use]
    pub const fn new(
        lease_id: &'a LeaseId,
        owner_member_id: &'a ConsumerId,
        group_id: &'a ConsumerGroupId,
        generation: Generation,
        lease_epoch: LeaseEpoch,
        observed_at_ms: TimestampMs,
    ) -> Self {
        Self {
            lease_id,
            owner_member_id,
            group_id,
            generation,
            lease_epoch,
            observed_at_ms,
        }
    }
}

/// Whether completion mutated the task or matched an already-retained idempotent completion.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CompletionOutcome {
    /// The active lease was completed and the task revision advanced.
    Completed,
    /// The same lease/result pair had already completed this task; no mutation occurred.
    AlreadyCompleted,
}

impl LeasedTask {
    /// Borrow the lease identifier.
    #[must_use]
    pub fn lease_id(&self) -> &LeaseId {
        &self.lease_id
    }

    /// Borrow the member that owns the lease.
    #[must_use]
    pub fn owner_member_id(&self) -> &ConsumerId {
        &self.owner_member_id
    }

    /// Borrow the Consumer Group that owns the lease.
    #[must_use]
    pub fn group_id(&self) -> &ConsumerGroupId {
        &self.group_id
    }

    /// Return the Consumer Group generation captured by this lease.
    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    /// Return the monotonic lease epoch.
    #[must_use]
    pub const fn lease_epoch(&self) -> LeaseEpoch {
        self.lease_epoch
    }

    /// Return the explicit lease expiry timestamp.
    #[must_use]
    pub const fn expires_at_ms(&self) -> TimestampMs {
        self.expires_at_ms
    }
}

/// Completed-state payload. The completed lease identity is retained to preserve idempotent completion semantics.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompletedTask {
    lease_id: LeaseId,
    owner_member_id: ConsumerId,
    group_id: ConsumerGroupId,
    generation: Generation,
    lease_epoch: LeaseEpoch,
    result: TaskResult,
    completed_at_ms: TimestampMs,
}

impl CompletedTask {
    /// Borrow the lease ID whose completion was accepted.
    #[must_use]
    pub fn lease_id(&self) -> &LeaseId {
        &self.lease_id
    }

    /// Borrow the member whose completion was accepted.
    #[must_use]
    pub fn owner_member_id(&self) -> &ConsumerId {
        &self.owner_member_id
    }

    /// Borrow the Consumer Group whose completion was accepted.
    #[must_use]
    pub fn group_id(&self) -> &ConsumerGroupId {
        &self.group_id
    }

    /// Return the generation fenced into the completed lease.
    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    /// Return the lease epoch fenced into the completed lease.
    #[must_use]
    pub const fn lease_epoch(&self) -> LeaseEpoch {
        self.lease_epoch
    }

    /// Borrow the retained task result.
    #[must_use]
    pub const fn result(&self) -> &TaskResult {
        &self.result
    }

    /// Return the completion timestamp supplied by the command boundary.
    #[must_use]
    pub const fn completed_at_ms(&self) -> TimestampMs {
        self.completed_at_ms
    }
}

/// Task lifecycle state with state-specific mandatory payloads.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TaskState {
    /// Task is ready and has no active lease identity.
    Queued(QueuedTask),
    /// Task has a complete active lease identity.
    Leased(LeasedTask),
    /// Task has a result and the lease identity that produced it.
    Completed(CompletedTask),
}

impl TaskState {
    /// Return the stable status corresponding to the state variant.
    #[must_use]
    pub const fn status(&self) -> TaskStatus {
        match self {
            Self::Queued(_) => TaskStatus::Queued,
            Self::Leased(_) => TaskStatus::Leased,
            Self::Completed(_) => TaskStatus::Completed,
        }
    }
}

/// Authoritative task record. Mutually exclusive lease/result fields live inside [`TaskState`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Task {
    id: TaskId,
    namespace_id: NamespaceId,
    objective: TaskObjective,
    created_at_ms: TimestampMs,
    revision: Revision,
    state: TaskState,
}

impl Task {
    /// Create a newly published queued Task matching the Python reference defaults.
    #[must_use]
    pub const fn new(
        task_id: TaskId,
        namespace_id: NamespaceId,
        objective: TaskObjective,
        created_at_ms: TimestampMs,
    ) -> Self {
        Self {
            id: task_id,
            namespace_id,
            objective,
            created_at_ms,
            revision: Revision::new(1),
            state: TaskState::Queued(QueuedTask::new(LeaseEpoch::new(0))),
        }
    }

    /// Borrow the Task identifier.
    #[must_use]
    pub const fn task_id(&self) -> &TaskId {
        &self.id
    }

    /// Borrow the owning namespace identifier.
    #[must_use]
    pub const fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Borrow the validated Task objective.
    #[must_use]
    pub const fn objective(&self) -> &TaskObjective {
        &self.objective
    }

    /// Return the explicit publication timestamp.
    #[must_use]
    pub const fn created_at_ms(&self) -> TimestampMs {
        self.created_at_ms
    }

    /// Return the Task revision.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Borrow the type-safe lifecycle state.
    #[must_use]
    pub const fn state(&self) -> &TaskState {
        &self.state
    }

    /// Return the stable externally observable status.
    #[must_use]
    pub const fn status(&self) -> TaskStatus {
        self.state.status()
    }

    /// Export the logical Task state used by persistence and replication checkpoints.
    #[must_use]
    pub fn checkpoint(&self) -> TaskCheckpoint {
        let state = match &self.state {
            TaskState::Queued(queued) => TaskCheckpointState::Queued {
                lease_epoch: queued.lease_epoch(),
            },
            TaskState::Leased(lease) => TaskCheckpointState::Leased {
                lease_id: lease.lease_id.clone(),
                owner_member_id: lease.owner_member_id.clone(),
                group_id: lease.group_id.clone(),
                generation: lease.generation,
                lease_epoch: lease.lease_epoch,
                expires_at_ms: lease.expires_at_ms,
            },
            TaskState::Completed(completed) => TaskCheckpointState::Completed {
                lease_id: completed.lease_id.clone(),
                owner_member_id: completed.owner_member_id.clone(),
                group_id: completed.group_id.clone(),
                generation: completed.generation,
                lease_epoch: completed.lease_epoch,
                result: completed.result.clone(),
                completed_at_ms: completed.completed_at_ms,
            },
        };
        TaskCheckpoint {
            task_id: self.id.clone(),
            namespace_id: self.namespace_id.clone(),
            objective: self.objective.clone(),
            created_at_ms: self.created_at_ms,
            revision: self.revision,
            state,
        }
    }

    pub(crate) fn from_checkpoint(checkpoint: TaskCheckpoint) -> Self {
        let state = match checkpoint.state {
            TaskCheckpointState::Queued { lease_epoch } => {
                TaskState::Queued(QueuedTask::new(lease_epoch))
            }
            TaskCheckpointState::Leased {
                lease_id,
                owner_member_id,
                group_id,
                generation,
                lease_epoch,
                expires_at_ms,
            } => TaskState::Leased(LeasedTask {
                lease_id,
                owner_member_id,
                group_id,
                generation,
                lease_epoch,
                expires_at_ms,
            }),
            TaskCheckpointState::Completed {
                lease_id,
                owner_member_id,
                group_id,
                generation,
                lease_epoch,
                result,
                completed_at_ms,
            } => TaskState::Completed(CompletedTask {
                lease_id,
                owner_member_id,
                group_id,
                generation,
                lease_epoch,
                result,
                completed_at_ms,
            }),
        };
        Self {
            id: checkpoint.task_id,
            namespace_id: checkpoint.namespace_id,
            objective: checkpoint.objective,
            created_at_ms: checkpoint.created_at_ms,
            revision: checkpoint.revision,
            state,
        }
    }

    /// Claim a queued task with a newly admitted lease.
    ///
    /// The lease epoch and task revision advance exactly once on success.
    ///
    /// # Errors
    ///
    /// Returns [`TaskTransitionError`] when the task is not queued or a monotonic counter cannot
    /// advance without overflow.
    pub fn claim(&mut self, grant: LeaseGrant) -> Result<(), TaskTransitionError> {
        let TaskState::Queued(queued) = &self.state else {
            return Err(TaskTransitionError::NotQueued {
                status: self.status(),
            });
        };
        let next_epoch = queued.lease_epoch().next()?;
        let next_revision = self.revision.next()?;
        self.state = TaskState::Leased(LeasedTask {
            lease_id: grant.lease_id,
            owner_member_id: grant.owner_member_id,
            group_id: grant.group_id,
            generation: grant.generation,
            lease_epoch: next_epoch,
            expires_at_ms: grant.expires_at_ms,
        });
        self.revision = next_revision;
        Ok(())
    }

    /// Renew a matching unexpired task lease without changing its lease epoch.
    ///
    /// # Errors
    ///
    /// Returns [`TaskTransitionError`] when any lease fence is stale, the lease has expired, or
    /// the task revision cannot advance safely.
    pub fn renew(
        &mut self,
        fence: LeaseFence<'_>,
        new_expires_at_ms: TimestampMs,
    ) -> Result<(), TaskTransitionError> {
        self.require_active_lease(fence)?;
        let next_revision = self.revision.next()?;
        let TaskState::Leased(lease) = &mut self.state else {
            return Err(TaskTransitionError::NotLeased {
                status: self.status(),
            });
        };
        lease.expires_at_ms = new_expires_at_ms;
        self.revision = next_revision;
        Ok(())
    }

    /// Complete a matching active lease or recognize an identical retained completion retry.
    ///
    /// # Errors
    ///
    /// Returns [`TaskTransitionError`] for stale lease fences, expired leases, conflicting
    /// completion retries, or revision overflow.
    pub fn complete(
        &mut self,
        fence: LeaseFence<'_>,
        result: TaskResult,
    ) -> Result<CompletionOutcome, TaskTransitionError> {
        if let TaskState::Completed(completed) = &self.state {
            if completed.lease_id() == fence.lease_id && completed.result() == &result {
                return Ok(CompletionOutcome::AlreadyCompleted);
            }
            return Err(TaskTransitionError::AlreadyCompleted);
        }

        let lease = self.require_active_lease(fence)?.clone();
        let next_revision = self.revision.next()?;
        self.state = TaskState::Completed(CompletedTask {
            lease_id: lease.lease_id,
            owner_member_id: lease.owner_member_id,
            group_id: lease.group_id,
            generation: lease.generation,
            lease_epoch: lease.lease_epoch,
            result,
            completed_at_ms: fence.observed_at_ms,
        });
        self.revision = next_revision;
        Ok(CompletionOutcome::Completed)
    }

    /// Requeue an expired lease while retaining its lease epoch for future fencing.
    ///
    /// # Errors
    ///
    /// Returns [`TaskTransitionError`] if the task revision cannot advance safely.
    pub fn requeue_if_expired(&mut self, now_ms: TimestampMs) -> Result<bool, TaskTransitionError> {
        let TaskState::Leased(lease) = &self.state else {
            return Ok(false);
        };
        if lease.expires_at_ms() > now_ms {
            return Ok(false);
        }
        let lease_epoch = lease.lease_epoch();
        let next_revision = self.revision.next()?;
        self.state = TaskState::Queued(QueuedTask::new(lease_epoch));
        self.revision = next_revision;
        Ok(true)
    }

    /// Requeue an active lease when its owning member leaves or is reaped.
    ///
    /// The lease epoch is retained so the next claim advances it and permanently fences the
    /// previous holder. A stale member-lease index entry is ignored instead of corrupting state,
    /// matching the Python reference implementation's defensive recovery behavior.
    ///
    /// # Errors
    ///
    /// Returns [`TaskTransitionError`] if the task revision cannot advance safely.
    pub fn requeue_for_member(
        &mut self,
        group_id: &ConsumerGroupId,
        member_id: &ConsumerId,
    ) -> Result<bool, TaskTransitionError> {
        let TaskState::Leased(lease) = &self.state else {
            return Ok(false);
        };
        if lease.group_id() != group_id || lease.owner_member_id() != member_id {
            return Ok(false);
        }
        let lease_epoch = lease.lease_epoch();
        let next_revision = self.revision.next()?;
        self.state = TaskState::Queued(QueuedTask::new(lease_epoch));
        self.revision = next_revision;
        Ok(true)
    }

    fn require_active_lease(
        &self,
        fence: LeaseFence<'_>,
    ) -> Result<&LeasedTask, TaskTransitionError> {
        let TaskState::Leased(lease) = &self.state else {
            return Err(TaskTransitionError::NotLeased {
                status: self.status(),
            });
        };
        if lease.group_id() != fence.group_id {
            return Err(TaskTransitionError::StaleGroup);
        }
        if lease.owner_member_id() != fence.owner_member_id {
            return Err(TaskTransitionError::StaleOwner);
        }
        if lease.generation() != fence.generation {
            return Err(TaskTransitionError::StaleGeneration);
        }
        if lease.lease_epoch() != fence.lease_epoch {
            return Err(TaskTransitionError::StaleLeaseEpoch);
        }
        if lease.lease_id() != fence.lease_id {
            return Err(TaskTransitionError::StaleLeaseId);
        }
        if lease.expires_at_ms() <= fence.observed_at_ms {
            return Err(TaskTransitionError::LeaseExpired);
        }
        Ok(lease)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CompletionOutcome, LeaseFence, LeaseGrant, MAX_OBJECTIVE_BYTES, MAX_RESULT_BYTES, Task,
        TaskObjective, TaskResult, TaskState, TaskStatus, TaskTransitionError, TimestampMs,
    };
    use crate::{ConsumerGroupId, ConsumerId, Generation, LeaseId, NamespaceId, TaskId};

    fn task() -> Result<Task, Box<dyn std::error::Error>> {
        Ok(Task::new(
            TaskId::new("task-1")?,
            NamespaceId::new("project-a")?,
            TaskObjective::new("Implement Agent Broker")?,
            TimestampMs::new(1_001),
        ))
    }

    #[test]
    fn newly_published_task_matches_python_reference_defaults()
    -> Result<(), Box<dyn std::error::Error>> {
        let task = task()?;
        assert_eq!(task.status(), TaskStatus::Queued);
        assert_eq!(task.revision().get(), 1);
        assert_eq!(task.created_at_ms().get(), 1_001);
        let TaskState::Queued(queued) = task.state() else {
            return Err("new Task must be queued".into());
        };
        assert_eq!(queued.lease_epoch().get(), 0);
        Ok(())
    }

    #[test]
    fn task_text_limits_match_python_reference() {
        assert!(TaskObjective::new("   ").is_err());
        assert!(TaskResult::new("\n\t").is_err());
        assert!(TaskObjective::new("x".repeat(MAX_OBJECTIVE_BYTES)).is_ok());
        assert!(TaskObjective::new("x".repeat(MAX_OBJECTIVE_BYTES + 1)).is_err());
        assert!(TaskResult::new("x".repeat(MAX_RESULT_BYTES)).is_ok());
        assert!(TaskResult::new("x".repeat(MAX_RESULT_BYTES + 1)).is_err());
    }

    #[test]
    fn text_limits_are_utf8_byte_limits_not_character_limits() {
        let korean = "가".repeat((MAX_OBJECTIVE_BYTES / 3) + 1);
        assert!(korean.chars().count() < MAX_OBJECTIVE_BYTES);
        assert!(TaskObjective::new(korean).is_err());
    }

    #[test]
    fn claim_renew_complete_preserve_python_fencing_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut task = task()?;
        let lease_id = LeaseId::new("lease-1")?;
        let member_id = ConsumerId::new("worker-a")?;
        let group_id = ConsumerGroupId::new("engineering")?;
        let generation = Generation::new(1);
        task.claim(LeaseGrant::new(
            lease_id.clone(),
            member_id.clone(),
            group_id.clone(),
            generation,
            TimestampMs::new(31_000),
        ))?;
        assert_eq!(task.status(), TaskStatus::Leased);
        assert_eq!(task.revision().get(), 2);
        let TaskState::Leased(lease) = task.state() else {
            return Err("claimed Task must be leased".into());
        };
        assert_eq!(lease.lease_epoch().get(), 1);

        let fence = LeaseFence::new(
            &lease_id,
            &member_id,
            &group_id,
            generation,
            lease.lease_epoch(),
            TimestampMs::new(2_000),
        );
        task.renew(fence, TimestampMs::new(32_000))?;
        assert_eq!(task.revision().get(), 3);
        let TaskState::Leased(renewed) = task.state() else {
            return Err("renewed Task must remain leased".into());
        };
        assert_eq!(renewed.lease_epoch().get(), 1);
        assert_eq!(renewed.expires_at_ms().get(), 32_000);

        let completion_fence = LeaseFence::new(
            &lease_id,
            &member_id,
            &group_id,
            generation,
            renewed.lease_epoch(),
            TimestampMs::new(3_000),
        );
        let result = TaskResult::new("done")?;
        assert_eq!(
            task.complete(completion_fence, result.clone())?,
            CompletionOutcome::Completed
        );
        assert_eq!(task.status(), TaskStatus::Completed);
        assert_eq!(task.revision().get(), 4);
        assert_eq!(
            task.complete(completion_fence, result)?,
            CompletionOutcome::AlreadyCompleted
        );
        assert_eq!(task.revision().get(), 4);
        Ok(())
    }

    #[test]
    fn stale_fence_and_expired_lease_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut task = task()?;
        let lease_id = LeaseId::new("lease-1")?;
        let member_id = ConsumerId::new("worker-a")?;
        let group_id = ConsumerGroupId::new("engineering")?;
        task.claim(LeaseGrant::new(
            lease_id.clone(),
            member_id.clone(),
            group_id.clone(),
            Generation::new(2),
            TimestampMs::new(5_000),
        ))?;
        let TaskState::Leased(lease) = task.state() else {
            return Err("claimed Task must be leased".into());
        };
        let lease_epoch = lease.lease_epoch();

        let stale_generation = LeaseFence::new(
            &lease_id,
            &member_id,
            &group_id,
            Generation::new(1),
            lease_epoch,
            TimestampMs::new(2_000),
        );
        assert_eq!(
            task.renew(stale_generation, TimestampMs::new(6_000)),
            Err(TaskTransitionError::StaleGeneration)
        );

        let expired = LeaseFence::new(
            &lease_id,
            &member_id,
            &group_id,
            Generation::new(2),
            lease_epoch,
            TimestampMs::new(5_000),
        );
        assert_eq!(
            task.complete(expired, TaskResult::new("too-late")?),
            Err(TaskTransitionError::LeaseExpired)
        );
        Ok(())
    }

    #[test]
    fn expired_requeue_retains_epoch_and_next_claim_advances_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut task = task()?;
        let member_id = ConsumerId::new("worker-a")?;
        let group_id = ConsumerGroupId::new("engineering")?;
        task.claim(LeaseGrant::new(
            LeaseId::new("lease-1")?,
            member_id.clone(),
            group_id.clone(),
            Generation::new(1),
            TimestampMs::new(2_000),
        ))?;
        assert!(task.requeue_if_expired(TimestampMs::new(2_000))?);
        let TaskState::Queued(queued) = task.state() else {
            return Err("expired Task must be requeued".into());
        };
        assert_eq!(queued.lease_epoch().get(), 1);

        task.claim(LeaseGrant::new(
            LeaseId::new("lease-2")?,
            member_id,
            group_id,
            Generation::new(1),
            TimestampMs::new(4_000),
        ))?;
        let TaskState::Leased(reclaimed) = task.state() else {
            return Err("reclaimed Task must be leased".into());
        };
        assert_eq!(reclaimed.lease_epoch().get(), 2);
        Ok(())
    }
}

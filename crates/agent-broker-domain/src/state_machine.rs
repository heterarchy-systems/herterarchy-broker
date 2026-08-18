use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::error::Error;
use std::fmt;

use crate::checkpoint::{
    BrokerCheckpoint, CheckpointError, ConsumerGroupCheckpoint, NamespaceCheckpoint,
};
use crate::commands::{
    AdvanceTermCommand, BrokerCommand, ClaimTaskCommand, CompleteTaskCommand,
    EnsureConsumerGroupCommand, EnsureNamespaceCommand, HeartbeatCommand, JoinConsumerGroupCommand,
    LeaveConsumerGroupCommand, PruneCompletedTasksCommand, PublishTaskCommand,
    ReapStaleMembersCommand, RenewTaskLeaseCommand,
};
use crate::group::{GroupCoordinator, GroupCoordinatorError};
use crate::results::{
    AppliedMutation, BrokerMutationResult, CompletedTasksPrunedResult, ConsumerGroupResult,
    HeartbeatResult, MutationMetadata, NamespaceResult, StaleMembersReapedResult, StateChangeSet,
    TaskClaimResult, TaskCompletedResult, TaskLeaseRenewedResult, TaskPublishedResult,
    TermAdvancedResult,
};
use crate::{
    Capabilities, CompletionOutcome, ConsumerGroup, ConsumerGroupDirectory, ConsumerGroupError,
    ConsumerGroupId, ConsumerGroupSummary, ConsumerId, Generation, HeartbeatOutcome, JoinOutcome,
    LeaseEpoch, LeaseFence, LeaseGrant, LeaseId, NamespaceId, Revision, Task, TaskId, TaskState,
    TaskStatus, TaskTransitionError, Term, TimestampMs,
};

type ReadyTaskHeap = BinaryHeap<Reverse<(TimestampMs, TaskId)>>;
type LeaseExpirationHeap = BinaryHeap<Reverse<(TimestampMs, TaskId, LeaseEpoch)>>;
type CompletedTaskHeap = BinaryHeap<Reverse<(TimestampMs, TaskId)>>;

/// Namespace record retained by the authoritative Broker state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Namespace {
    id: NamespaceId,
    revision: Revision,
}

impl Namespace {
    fn new(id: NamespaceId) -> Self {
        Self {
            id,
            revision: Revision::new(1),
        }
    }

    /// Borrow the namespace ID.
    #[must_use]
    pub const fn namespace_id(&self) -> &NamespaceId {
        &self.id
    }

    /// Return namespace-local revision.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Export this namespace into the logical persistence/replication checkpoint contract.
    #[must_use]
    pub fn checkpoint(&self) -> NamespaceCheckpoint {
        NamespaceCheckpoint {
            namespace_id: self.id.clone(),
            revision: self.revision,
        }
    }

    fn from_checkpoint(checkpoint: NamespaceCheckpoint) -> Self {
        Self {
            id: checkpoint.namespace_id,
            revision: checkpoint.revision,
        }
    }
}

/// Provider-independent authoritative Broker state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrokerState {
    term: crate::Term,
    revision: Revision,
    namespaces: BTreeMap<NamespaceId, Namespace>,
    tasks: BTreeMap<TaskId, Task>,
    groups: BTreeMap<ConsumerGroupId, ConsumerGroup>,
}

impl Default for BrokerState {
    fn default() -> Self {
        Self {
            term: crate::Term::INITIAL,
            revision: Revision::new(0),
            namespaces: BTreeMap::new(),
            tasks: BTreeMap::new(),
            groups: BTreeMap::new(),
        }
    }
}

impl BrokerState {
    /// Return current Broker term.
    #[must_use]
    pub const fn term(&self) -> crate::Term {
        self.term
    }

    /// Return global Broker state revision.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Borrow a namespace by ID.
    #[must_use]
    pub fn namespace(&self, namespace_id: &NamespaceId) -> Option<&Namespace> {
        self.namespaces.get(namespace_id)
    }

    /// Borrow a task by ID.
    #[must_use]
    pub fn task(&self, task_id: &TaskId) -> Option<&Task> {
        self.tasks.get(task_id)
    }

    /// Borrow a Consumer Group by ID.
    #[must_use]
    pub fn group(&self, group_id: &ConsumerGroupId) -> Option<&ConsumerGroup> {
        self.groups.get(group_id)
    }

    /// Return namespace count.
    #[must_use]
    pub fn namespace_count(&self) -> usize {
        self.namespaces.len()
    }

    /// Return retained task count.
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Return Consumer Group count.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Return deterministic Consumer Group summaries ordered by Group ID.
    #[must_use]
    pub fn group_summaries(&self) -> Vec<ConsumerGroupSummary> {
        self.groups.values().map(ConsumerGroup::summary).collect()
    }

    /// Return one Consumer Group summary without exposing mutable Group state.
    #[must_use]
    pub fn group_summary(&self, group_id: &ConsumerGroupId) -> Option<ConsumerGroupSummary> {
        self.groups.get(group_id).map(ConsumerGroup::summary)
    }

    /// Return one internally consistent read-only Group directory snapshot.
    #[must_use]
    pub fn group_directory(&self) -> ConsumerGroupDirectory {
        ConsumerGroupDirectory::new(self.term, self.revision, self.group_summaries())
    }

    /// Export the complete logical authoritative state without derived runtime indexes.
    #[must_use]
    pub fn checkpoint(&self) -> BrokerCheckpoint {
        BrokerCheckpoint {
            term: self.term,
            revision: self.revision,
            namespaces: self
                .namespaces
                .values()
                .map(Namespace::checkpoint)
                .collect(),
            tasks: self.tasks.values().map(Task::checkpoint).collect(),
            groups: self
                .groups
                .values()
                .map(ConsumerGroup::checkpoint)
                .collect(),
        }
    }
}

/// Stable deterministic state-machine failure categories.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StateMachineError {
    /// Capacity argument is invalid for a new resource.
    InvalidCapacity { field: &'static str },
    /// New resource admission exceeded a bounded capacity.
    CapacityExceeded { resource: &'static str, max: usize },
    /// Namespace does not exist.
    NamespaceNotFound(NamespaceId),
    /// Task identity already exists with different content.
    TaskConflict(TaskId),
    /// Requested Task does not exist.
    TaskNotFound(TaskId),
    /// Lease ID is already active for another Task/member/group.
    LeaseIdConflict(LeaseId),
    /// Client observed a stale Broker term.
    StaleTerm { expected: Term, actual: Term },
    /// A proposed new Broker term is not strictly greater than the current term.
    NewTermNotGreater { current: Term, proposed: Term },
    /// Namespace Task accounting would underflow during completed-Task pruning.
    TaskCountUnderflow(NamespaceId),
    /// Timestamp arithmetic overflowed instead of wrapping.
    TimestampOverflow,
    /// Task transition rejected stale lease/state fencing.
    TaskTransition(TaskTransitionError),
    /// Consumer Group identity is already bound to another namespace.
    ConsumerGroupConflict(ConsumerGroupId),
    /// Requested Consumer Group does not exist.
    ConsumerGroupNotFound(ConsumerGroupId),
    /// Consumer Group/member transition rejected the command.
    ConsumerGroupTransition(ConsumerGroupError),
    /// Global Broker revision could not advance safely.
    FencingValue(crate::FencingValueError),
}

impl fmt::Display for StateMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCapacity { field } => write!(formatter, "{field} must be positive"),
            Self::CapacityExceeded { resource, max } => {
                write!(formatter, "{resource} capacity reached ({max})")
            }
            Self::NamespaceNotFound(namespace_id) => {
                write!(formatter, "namespace {namespace_id} does not exist")
            }
            Self::TaskConflict(task_id) => {
                write!(
                    formatter,
                    "task {task_id} already exists with different content"
                )
            }
            Self::TaskNotFound(task_id) => write!(formatter, "task {task_id} does not exist"),
            Self::LeaseIdConflict(lease_id) => {
                write!(formatter, "lease ID {lease_id} is already in use")
            }
            Self::StaleTerm { expected, actual } => write!(
                formatter,
                "Broker term is stale: expected {}, actual {}",
                expected.get(),
                actual.get()
            ),
            Self::NewTermNotGreater { current, proposed } => write!(
                formatter,
                "new Broker term {} must be greater than current term {}",
                proposed.get(),
                current.get()
            ),
            Self::TaskCountUnderflow(namespace_id) => write!(
                formatter,
                "namespace {namespace_id} task count underflow during pruning"
            ),
            Self::TimestampOverflow => formatter.write_str("timestamp arithmetic overflowed"),
            Self::TaskTransition(error) => error.fmt(formatter),
            Self::ConsumerGroupConflict(group_id) => {
                write!(
                    formatter,
                    "Consumer Group {group_id} belongs to another namespace"
                )
            }
            Self::ConsumerGroupNotFound(group_id) => {
                write!(formatter, "Consumer Group {group_id} does not exist")
            }
            Self::ConsumerGroupTransition(error) => error.fmt(formatter),
            Self::FencingValue(error) => error.fmt(formatter),
        }
    }
}

impl Error for StateMachineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConsumerGroupTransition(error) => Some(error),
            Self::TaskTransition(error) => Some(error),
            Self::FencingValue(error) => Some(error),
            _ => None,
        }
    }
}

impl From<crate::FencingValueError> for StateMachineError {
    fn from(error: crate::FencingValueError) -> Self {
        Self::FencingValue(error)
    }
}

impl From<ConsumerGroupError> for StateMachineError {
    fn from(error: ConsumerGroupError) -> Self {
        Self::ConsumerGroupTransition(error)
    }
}

impl From<GroupCoordinatorError> for StateMachineError {
    fn from(error: GroupCoordinatorError) -> Self {
        match error {
            GroupCoordinatorError::GroupNotFound(group_id) => Self::ConsumerGroupNotFound(group_id),
            GroupCoordinatorError::Transition(error) => Self::ConsumerGroupTransition(error),
        }
    }
}

impl From<TaskTransitionError> for StateMachineError {
    fn from(error: TaskTransitionError) -> Self {
        Self::TaskTransition(error)
    }
}

/// Deterministic Broker state machine shared by standalone and future Raft commit paths.
#[derive(Debug, Default)]
pub struct BrokerStateMachine {
    state: BrokerState,
    ready_tasks: BTreeMap<NamespaceId, ReadyTaskHeap>,
    lease_expirations: BTreeMap<NamespaceId, LeaseExpirationHeap>,
    active_lease_tasks: BTreeMap<LeaseId, TaskId>,
    member_lease_tasks: BTreeMap<(ConsumerGroupId, ConsumerId), BTreeSet<TaskId>>,
    completed_tasks: CompletedTaskHeap,
    namespace_task_counts: BTreeMap<NamespaceId, usize>,
    namespace_group_counts: BTreeMap<NamespaceId, usize>,
    changed_namespaces: BTreeSet<NamespaceId>,
    changed_tasks: BTreeSet<TaskId>,
    deleted_tasks: BTreeSet<TaskId>,
    changed_groups: BTreeSet<ConsumerGroupId>,
}

#[derive(Debug, Default)]
struct RestoredIndexes {
    ready_tasks: BTreeMap<NamespaceId, ReadyTaskHeap>,
    lease_expirations: BTreeMap<NamespaceId, LeaseExpirationHeap>,
    active_lease_tasks: BTreeMap<LeaseId, TaskId>,
    member_lease_tasks: BTreeMap<(ConsumerGroupId, ConsumerId), BTreeSet<TaskId>>,
    completed_tasks: CompletedTaskHeap,
    namespace_task_counts: BTreeMap<NamespaceId, usize>,
    namespace_group_counts: BTreeMap<NamespaceId, usize>,
}

impl BrokerStateMachine {
    /// Borrow current authoritative state.
    #[must_use]
    pub const fn state(&self) -> &BrokerState {
        &self.state
    }

    /// Return current logical Consumers in one Consumer Group whose advertised capabilities
    /// satisfy every required capability.
    ///
    /// This read-only query is provider-neutral and deterministic. It does not inspect physical
    /// runtime availability and does not assign Task ownership; callers may use the result as an
    /// input to a later HETERARCHY coordination policy.
    ///
    /// # Errors
    ///
    /// Returns [`StateMachineError::ConsumerGroupNotFound`] when the requested Company/group is
    /// not registered.
    pub fn consumers_matching_capabilities(
        &self,
        group_id: &ConsumerGroupId,
        required_capabilities: &Capabilities,
    ) -> Result<Vec<ConsumerId>, StateMachineError> {
        GroupCoordinator::consumers_matching_capabilities(
            &self.state.groups,
            group_id,
            required_capabilities,
        )
        .map_err(StateMachineError::from)
    }

    /// Restore authoritative state from a logical checkpoint and deterministically rebuild every
    /// derived runtime index.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError`] for duplicate records, zero entity revisions, unknown namespace
    /// references, or duplicate active lease identities that cannot be represented safely.
    pub fn from_checkpoint(checkpoint: BrokerCheckpoint) -> Result<Self, CheckpointError> {
        let state = Self::restore_state(checkpoint)?;
        let indexes = Self::rebuild_indexes(&state)?;
        Ok(Self {
            state,
            ready_tasks: indexes.ready_tasks,
            lease_expirations: indexes.lease_expirations,
            active_lease_tasks: indexes.active_lease_tasks,
            member_lease_tasks: indexes.member_lease_tasks,
            completed_tasks: indexes.completed_tasks,
            namespace_task_counts: indexes.namespace_task_counts,
            namespace_group_counts: indexes.namespace_group_counts,
            changed_namespaces: BTreeSet::new(),
            changed_tasks: BTreeSet::new(),
            deleted_tasks: BTreeSet::new(),
            changed_groups: BTreeSet::new(),
        })
    }

    fn restore_state(checkpoint: BrokerCheckpoint) -> Result<BrokerState, CheckpointError> {
        let BrokerCheckpoint {
            term,
            revision,
            namespaces,
            tasks,
            groups,
        } = checkpoint;
        let mut state = BrokerState {
            term,
            revision,
            namespaces: BTreeMap::new(),
            tasks: BTreeMap::new(),
            groups: BTreeMap::new(),
        };

        for namespace_checkpoint in namespaces {
            Self::restore_namespace(&mut state, namespace_checkpoint)?;
        }
        for group_checkpoint in groups {
            Self::restore_group(&mut state, group_checkpoint)?;
        }
        for task_checkpoint in tasks {
            Self::restore_task(&mut state, task_checkpoint)?;
        }
        Ok(state)
    }

    fn restore_namespace(
        state: &mut BrokerState,
        checkpoint: NamespaceCheckpoint,
    ) -> Result<(), CheckpointError> {
        Self::require_positive_revision("namespace", checkpoint.revision)?;
        let namespace_id = checkpoint.namespace_id.clone();
        if state
            .namespaces
            .insert(namespace_id.clone(), Namespace::from_checkpoint(checkpoint))
            .is_some()
        {
            return Err(CheckpointError::DuplicateNamespace(namespace_id));
        }
        Ok(())
    }

    fn restore_group(
        state: &mut BrokerState,
        checkpoint: ConsumerGroupCheckpoint,
    ) -> Result<(), CheckpointError> {
        Self::require_positive_revision("Consumer Group", checkpoint.revision)?;
        for member in &checkpoint.members {
            Self::require_positive_revision("member", member.revision)?;
        }
        let group_id = checkpoint.group_id.clone();
        let namespace_id = checkpoint.namespace_id.clone();
        if !state.namespaces.contains_key(&namespace_id) {
            return Err(CheckpointError::UnknownNamespace {
                entity: "Consumer Group",
                namespace_id,
            });
        }
        let group = ConsumerGroup::from_checkpoint(checkpoint)?;
        if state.groups.insert(group_id.clone(), group).is_some() {
            return Err(CheckpointError::DuplicateConsumerGroup(group_id));
        }
        Ok(())
    }

    fn restore_task(
        state: &mut BrokerState,
        checkpoint: crate::TaskCheckpoint,
    ) -> Result<(), CheckpointError> {
        Self::require_positive_revision("task", checkpoint.revision)?;
        let task_id = checkpoint.task_id.clone();
        let namespace_id = checkpoint.namespace_id.clone();
        if !state.namespaces.contains_key(&namespace_id) {
            return Err(CheckpointError::UnknownNamespace {
                entity: "task",
                namespace_id,
            });
        }
        if state
            .tasks
            .insert(task_id.clone(), Task::from_checkpoint(checkpoint))
            .is_some()
        {
            return Err(CheckpointError::DuplicateTask(task_id));
        }
        Ok(())
    }

    fn require_positive_revision(
        entity: &'static str,
        revision: Revision,
    ) -> Result<(), CheckpointError> {
        if revision.get() == 0 {
            return Err(CheckpointError::ZeroEntityRevision { entity });
        }
        Ok(())
    }

    fn rebuild_indexes(state: &BrokerState) -> Result<RestoredIndexes, CheckpointError> {
        let mut indexes = RestoredIndexes::default();
        for group in state.groups.values() {
            *indexes
                .namespace_group_counts
                .entry(group.namespace_id().clone())
                .or_default() += 1;
        }
        for task in state.tasks.values() {
            *indexes
                .namespace_task_counts
                .entry(task.namespace_id().clone())
                .or_default() += 1;
            Self::rebuild_task_indexes(&mut indexes, task)?;
        }
        Ok(indexes)
    }

    fn rebuild_task_indexes(
        indexes: &mut RestoredIndexes,
        task: &Task,
    ) -> Result<(), CheckpointError> {
        match task.state() {
            TaskState::Queued(_) => {
                indexes
                    .ready_tasks
                    .entry(task.namespace_id().clone())
                    .or_default()
                    .push(Reverse((task.created_at_ms(), task.task_id().clone())));
            }
            TaskState::Leased(lease) => {
                if indexes
                    .active_lease_tasks
                    .insert(lease.lease_id().clone(), task.task_id().clone())
                    .is_some()
                {
                    return Err(CheckpointError::DuplicateActiveLease(
                        lease.lease_id().clone(),
                    ));
                }
                indexes
                    .member_lease_tasks
                    .entry((lease.group_id().clone(), lease.owner_member_id().clone()))
                    .or_default()
                    .insert(task.task_id().clone());
                indexes
                    .lease_expirations
                    .entry(task.namespace_id().clone())
                    .or_default()
                    .push(Reverse((
                        lease.expires_at_ms(),
                        task.task_id().clone(),
                        lease.lease_epoch(),
                    )));
            }
            TaskState::Completed(completed) => {
                indexes.completed_tasks.push(Reverse((
                    completed.completed_at_ms(),
                    task.task_id().clone(),
                )));
            }
        }
        Ok(())
    }

    /// Apply one deterministic command and return its typed result plus explicit changed entities.
    ///
    /// # Errors
    ///
    /// Returns [`StateMachineError`] when validation, capacity, conflict, or monotonic revision
    /// invariants reject the command. No external side effects occur inside this method.
    pub fn apply(&mut self, command: BrokerCommand) -> Result<AppliedMutation, StateMachineError> {
        self.clear_changes();
        let result = match command {
            BrokerCommand::EnsureNamespace(command) => {
                BrokerMutationResult::Namespace(self.ensure_namespace(command)?)
            }
            BrokerCommand::PublishTask(command) => {
                BrokerMutationResult::TaskPublished(self.publish_task(command)?)
            }
            BrokerCommand::EnsureConsumerGroup(command) => {
                BrokerMutationResult::ConsumerGroup(self.ensure_consumer_group(command)?)
            }
            BrokerCommand::JoinConsumerGroup(command) => {
                BrokerMutationResult::ConsumerGroup(self.join_consumer_group(command)?)
            }
            BrokerCommand::Heartbeat(command) => {
                BrokerMutationResult::Heartbeat(self.heartbeat(command)?)
            }
            BrokerCommand::LeaveConsumerGroup(command) => {
                BrokerMutationResult::ConsumerGroup(self.leave_consumer_group(&command)?)
            }
            BrokerCommand::ReapStaleMembers(command) => {
                BrokerMutationResult::StaleMembersReaped(self.reap_stale_members(command)?)
            }
            BrokerCommand::ClaimTask(command) => {
                BrokerMutationResult::TaskClaim(self.claim_task(command)?)
            }
            BrokerCommand::RenewTaskLease(command) => {
                BrokerMutationResult::TaskLeaseRenewed(self.renew_task_lease(command)?)
            }
            BrokerCommand::CompleteTask(command) => {
                BrokerMutationResult::TaskCompleted(self.complete_task(command)?)
            }
            BrokerCommand::PruneCompletedTasks(command) => {
                BrokerMutationResult::CompletedTasksPruned(self.prune_completed_tasks(command)?)
            }
            BrokerCommand::AdvanceTerm(command) => {
                BrokerMutationResult::TermAdvanced(self.advance_term(command)?)
            }
        };
        Ok(AppliedMutation {
            result,
            changes: self.take_changes(),
        })
    }

    fn metadata(&self) -> MutationMetadata {
        MutationMetadata {
            term: self.state.term,
            revision: self.state.revision,
        }
    }

    fn ensure_namespace(
        &mut self,
        command: EnsureNamespaceCommand,
    ) -> Result<NamespaceResult, StateMachineError> {
        if let Some(namespace) = self.state.namespaces.get(&command.namespace_id) {
            return Ok(NamespaceResult {
                metadata: self.metadata(),
                namespace_id: namespace.namespace_id().clone(),
                namespace_revision: namespace.revision(),
            });
        }
        if command.max_namespaces == 0 {
            return Err(StateMachineError::InvalidCapacity {
                field: "max_namespaces",
            });
        }
        if self.state.namespaces.len() >= command.max_namespaces {
            return Err(StateMachineError::CapacityExceeded {
                resource: "Broker namespace",
                max: command.max_namespaces,
            });
        }

        let next_revision = self.state.revision.next()?;
        let namespace_id = command.namespace_id;
        let namespace = Namespace::new(namespace_id.clone());
        let namespace_revision = namespace.revision();
        self.state
            .namespaces
            .insert(namespace_id.clone(), namespace);
        self.state.revision = next_revision;
        self.changed_namespaces.insert(namespace_id.clone());
        Ok(NamespaceResult {
            metadata: self.metadata(),
            namespace_id,
            namespace_revision,
        })
    }

    fn publish_task(
        &mut self,
        command: PublishTaskCommand,
    ) -> Result<TaskPublishedResult, StateMachineError> {
        self.require_namespace(&command.namespace_id)?;
        if let Some(task) = self.state.tasks.get(&command.task_id) {
            if task.namespace_id() != &command.namespace_id
                || task.objective() != &command.objective
            {
                return Err(StateMachineError::TaskConflict(command.task_id));
            }
            return Ok(TaskPublishedResult {
                metadata: self.metadata(),
                task_id: task.task_id().clone(),
                task_revision: task.revision(),
                status: task.status(),
            });
        }
        if command.max_namespace_tasks == 0 {
            return Err(StateMachineError::InvalidCapacity {
                field: "max_namespace_tasks",
            });
        }
        let current_count = self
            .namespace_task_counts
            .get(&command.namespace_id)
            .copied()
            .unwrap_or(0);
        if current_count >= command.max_namespace_tasks {
            return Err(StateMachineError::CapacityExceeded {
                resource: "Broker Task",
                max: command.max_namespace_tasks,
            });
        }

        let next_revision = self.state.revision.next()?;
        let namespace_id = command.namespace_id;
        let task_id = command.task_id;
        let task = Task::new(
            task_id.clone(),
            namespace_id.clone(),
            command.objective,
            command.created_at_ms,
        );
        let task_revision = task.revision();
        let status = task.status();
        self.state.tasks.insert(task_id.clone(), task);
        self.namespace_task_counts
            .insert(namespace_id.clone(), current_count + 1);
        self.ready_tasks
            .entry(namespace_id)
            .or_default()
            .push(Reverse((command.created_at_ms, task_id.clone())));
        self.state.revision = next_revision;
        self.changed_tasks.insert(task_id.clone());
        Ok(TaskPublishedResult {
            metadata: self.metadata(),
            task_id,
            task_revision,
            status,
        })
    }

    fn ensure_consumer_group(
        &mut self,
        command: EnsureConsumerGroupCommand,
    ) -> Result<ConsumerGroupResult, StateMachineError> {
        self.require_namespace(&command.namespace_id)?;
        if let Some(group) = self.state.groups.get(&command.group_id) {
            if group.namespace_id() != &command.namespace_id {
                return Err(StateMachineError::ConsumerGroupConflict(command.group_id));
            }
            return Ok(Self::consumer_group_result(self.metadata(), group));
        }
        if command.max_namespace_groups == 0 {
            return Err(StateMachineError::InvalidCapacity {
                field: "max_namespace_groups",
            });
        }
        let current_count = self
            .namespace_group_counts
            .get(&command.namespace_id)
            .copied()
            .unwrap_or(0);
        if current_count >= command.max_namespace_groups {
            return Err(StateMachineError::CapacityExceeded {
                resource: "Broker Consumer Group",
                max: command.max_namespace_groups,
            });
        }

        let next_revision = self.state.revision.next()?;
        let namespace_id = command.namespace_id;
        let group_id = command.group_id;
        let group = ConsumerGroup::new(group_id.clone(), namespace_id.clone());
        self.state.groups.insert(group_id.clone(), group);
        self.namespace_group_counts
            .insert(namespace_id, current_count + 1);
        self.state.revision = next_revision;
        self.changed_groups.insert(group_id.clone());
        let group = self
            .state
            .groups
            .get(&group_id)
            .ok_or_else(|| StateMachineError::ConsumerGroupConflict(group_id.clone()))?;
        Ok(Self::consumer_group_result(self.metadata(), group))
    }

    fn join_consumer_group(
        &mut self,
        command: JoinConsumerGroupCommand,
    ) -> Result<ConsumerGroupResult, StateMachineError> {
        let next_global_revision = self.state.revision.next()?;
        let outcome = GroupCoordinator::join(
            &mut self.state.groups,
            &command.group_id,
            command.member_id,
            command.capabilities,
            command.now_ms,
            command.max_group_members,
        )?;
        let (group_id, generation, group_revision, member_count) = {
            let group = self.state.groups.get(&command.group_id).ok_or_else(|| {
                StateMachineError::ConsumerGroupNotFound(command.group_id.clone())
            })?;
            (
                group.group_id().clone(),
                group.generation(),
                group.revision(),
                group.consumer_count(),
            )
        };
        if outcome != JoinOutcome::Unchanged {
            self.state.revision = next_global_revision;
            self.changed_groups.insert(group_id.clone());
        }
        Ok(ConsumerGroupResult {
            metadata: self.metadata(),
            group_id,
            generation,
            group_revision,
            member_count,
        })
    }

    fn heartbeat(
        &mut self,
        command: HeartbeatCommand,
    ) -> Result<HeartbeatResult, StateMachineError> {
        let next_global_revision = self.state.revision.next()?;
        let outcome = GroupCoordinator::heartbeat(
            &mut self.state.groups,
            &command.group_id,
            &command.member_id,
            command.expected_generation,
            command.now_ms,
        )?;
        let (group_id, generation, member_revision) = {
            let group = self.state.groups.get(&command.group_id).ok_or_else(|| {
                StateMachineError::ConsumerGroupNotFound(command.group_id.clone())
            })?;
            let member_revision = group
                .consumer(&command.member_id)
                .map(crate::Consumer::revision)
                .ok_or_else(|| {
                    StateMachineError::ConsumerGroupTransition(ConsumerGroupError::MemberNotFound(
                        command.member_id.clone(),
                    ))
                })?;
            (
                group.group_id().clone(),
                group.generation(),
                member_revision,
            )
        };
        if outcome == HeartbeatOutcome::Updated {
            self.state.revision = next_global_revision;
            self.changed_groups.insert(group_id.clone());
        }
        Ok(HeartbeatResult {
            metadata: self.metadata(),
            group_id,
            member_id: command.member_id,
            generation,
            member_revision,
        })
    }

    fn leave_consumer_group(
        &mut self,
        command: &LeaveConsumerGroupCommand,
    ) -> Result<ConsumerGroupResult, StateMachineError> {
        let next_global_revision = self.state.revision.next()?;
        self.preflight_member_lease_requeue(&command.group_id, &command.member_id)?;
        let _removed_member = GroupCoordinator::leave(
            &mut self.state.groups,
            &command.group_id,
            &command.member_id,
            command.expected_generation,
        )?;
        let (group_id, generation, group_revision, member_count) = {
            let group = self.state.groups.get(&command.group_id).ok_or_else(|| {
                StateMachineError::ConsumerGroupNotFound(command.group_id.clone())
            })?;
            (
                group.group_id().clone(),
                group.generation(),
                group.revision(),
                group.consumer_count(),
            )
        };
        self.requeue_member_leases(&group_id, &command.member_id)?;
        self.state.revision = next_global_revision;
        self.changed_groups.insert(group_id.clone());
        Ok(ConsumerGroupResult {
            metadata: self.metadata(),
            group_id,
            generation,
            group_revision,
            member_count,
        })
    }

    fn reap_stale_members(
        &mut self,
        command: ReapStaleMembersCommand,
    ) -> Result<StaleMembersReapedResult, StateMachineError> {
        if command.max_members == 0 {
            return Err(StateMachineError::InvalidCapacity {
                field: "max_members",
            });
        }

        let mut candidates = self
            .state
            .groups
            .iter()
            .flat_map(|(group_id, group)| {
                group
                    .consumers()
                    .filter(|member| member.last_heartbeat_at_ms() <= command.stale_before_ms)
                    .map(|member| {
                        (
                            member.last_heartbeat_at_ms(),
                            group_id.clone(),
                            member.consumer_id().clone(),
                        )
                    })
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable();
        candidates.truncate(command.max_members);
        if candidates.is_empty() {
            return Ok(StaleMembersReapedResult {
                metadata: self.metadata(),
                reaped_count: 0,
                affected_group_count: 0,
            });
        }

        let mut selected_members = BTreeMap::<ConsumerGroupId, Vec<ConsumerId>>::new();
        for (_, group_id, member_id) in candidates {
            selected_members
                .entry(group_id)
                .or_default()
                .push(member_id);
        }

        let next_global_revision = self.state.revision.next()?;
        for (group_id, member_ids) in &selected_members {
            let group = self
                .state
                .groups
                .get(group_id)
                .ok_or_else(|| StateMachineError::ConsumerGroupNotFound(group_id.clone()))?;
            group.generation().next()?;
            group.revision().next()?;
            for member_id in member_ids {
                self.preflight_member_lease_requeue(group_id, member_id)?;
            }
        }

        let mut reaped_count = 0;
        let mut affected_group_ids = Vec::with_capacity(selected_members.len());
        for (group_id, member_ids) in selected_members {
            let removed = GroupCoordinator::reap_stale_members(
                &mut self.state.groups,
                &group_id,
                command.stale_before_ms,
                member_ids.len(),
            )?;
            if !removed.is_empty() {
                reaped_count += removed.len();
                for member in &removed {
                    self.requeue_member_leases(&group_id, member.consumer_id())?;
                }
                affected_group_ids.push(group_id);
            }
        }

        if reaped_count > 0 {
            self.state.revision = next_global_revision;
            self.changed_groups
                .extend(affected_group_ids.iter().cloned());
        }
        Ok(StaleMembersReapedResult {
            metadata: self.metadata(),
            reaped_count,
            affected_group_count: affected_group_ids.len(),
        })
    }

    fn claim_task(
        &mut self,
        command: ClaimTaskCommand,
    ) -> Result<TaskClaimResult, StateMachineError> {
        self.require_term(command.expected_term)?;
        let (namespace_id, generation) = self.require_group_member(
            &command.group_id,
            &command.member_id,
            command.expected_generation,
        )?;

        if let Some(existing_task_id) = self.active_lease_tasks.get(&command.lease_id) {
            let task = self
                .state
                .tasks
                .get(existing_task_id)
                .ok_or_else(|| StateMachineError::TaskNotFound(existing_task_id.clone()))?;
            if let TaskState::Leased(lease) = task.state()
                && lease.owner_member_id() == &command.member_id
                && lease.group_id() == &command.group_id
            {
                return Ok(self.task_claim_result(generation, Some(task)));
            }
            return Err(StateMachineError::LeaseIdConflict(command.lease_id));
        }

        let recovered = self.recover_expired_leases(&namespace_id, command.now_ms)?;
        let Some(task_id) = self.pop_ready_task(&namespace_id) else {
            if recovered > 0 {
                self.state.revision = self.state.revision.next()?;
            }
            return Ok(self.task_claim_result(generation, None));
        };

        let expires_at_ms = Self::checked_deadline(command.now_ms, command.lease_duration_ms)?;
        let next_global_revision = self.state.revision.next()?;
        let (task_revision, lease_epoch) = {
            let task = self
                .state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| StateMachineError::TaskNotFound(task_id.clone()))?;
            task.claim(LeaseGrant::new(
                command.lease_id.clone(),
                command.member_id.clone(),
                command.group_id.clone(),
                generation,
                expires_at_ms,
            ))?;
            let TaskState::Leased(lease) = task.state() else {
                return Err(StateMachineError::TaskTransition(
                    TaskTransitionError::NotLeased {
                        status: task.status(),
                    },
                ));
            };
            (task.revision(), lease.lease_epoch())
        };

        self.active_lease_tasks
            .insert(command.lease_id.clone(), task_id.clone());
        self.member_lease_tasks
            .entry((command.group_id.clone(), command.member_id.clone()))
            .or_default()
            .insert(task_id.clone());
        self.lease_expirations
            .entry(namespace_id)
            .or_default()
            .push(Reverse((expires_at_ms, task_id.clone(), lease_epoch)));
        self.changed_tasks.insert(task_id.clone());
        self.state.revision = next_global_revision;

        let task = self
            .state
            .tasks
            .get(&task_id)
            .ok_or_else(|| StateMachineError::TaskNotFound(task_id.clone()))?;
        let mut result = self.task_claim_result(generation, Some(task));
        result.task_revision = Some(task_revision);
        Ok(result)
    }

    fn renew_task_lease(
        &mut self,
        command: RenewTaskLeaseCommand,
    ) -> Result<TaskLeaseRenewedResult, StateMachineError> {
        self.require_term(command.expected_term)?;
        let (_namespace_id, generation) = self.require_group_member(
            &command.group_id,
            &command.member_id,
            command.expected_generation,
        )?;
        let expires_at_ms = Self::checked_deadline(command.now_ms, command.lease_duration_ms)?;
        let next_global_revision = self.state.revision.next()?;
        let (namespace_id, task_revision, lease_epoch) = {
            let task = self
                .state
                .tasks
                .get_mut(&command.task_id)
                .ok_or_else(|| StateMachineError::TaskNotFound(command.task_id.clone()))?;
            let namespace_id = task.namespace_id().clone();
            task.renew(
                LeaseFence::new(
                    &command.lease_id,
                    &command.member_id,
                    &command.group_id,
                    command.expected_generation,
                    command.expected_lease_epoch,
                    command.now_ms,
                ),
                expires_at_ms,
            )?;
            let TaskState::Leased(lease) = task.state() else {
                return Err(StateMachineError::TaskTransition(
                    TaskTransitionError::NotLeased {
                        status: task.status(),
                    },
                ));
            };
            (namespace_id, task.revision(), lease.lease_epoch())
        };

        self.lease_expirations
            .entry(namespace_id)
            .or_default()
            .push(Reverse((
                expires_at_ms,
                command.task_id.clone(),
                lease_epoch,
            )));
        self.changed_tasks.insert(command.task_id.clone());
        self.state.revision = next_global_revision;
        Ok(TaskLeaseRenewedResult {
            metadata: self.metadata(),
            task_id: command.task_id,
            task_revision,
            lease_id: command.lease_id,
            lease_epoch,
            lease_expires_at_ms: expires_at_ms,
            generation,
        })
    }

    fn complete_task(
        &mut self,
        command: CompleteTaskCommand,
    ) -> Result<TaskCompletedResult, StateMachineError> {
        self.require_term(command.expected_term)?;
        self.require_group_member(
            &command.group_id,
            &command.member_id,
            command.expected_generation,
        )?;
        let next_global_revision = self.state.revision.next()?;
        let (outcome, task_revision, status) = {
            let task = self
                .state
                .tasks
                .get_mut(&command.task_id)
                .ok_or_else(|| StateMachineError::TaskNotFound(command.task_id.clone()))?;
            let outcome = task.complete(
                LeaseFence::new(
                    &command.lease_id,
                    &command.member_id,
                    &command.group_id,
                    command.expected_generation,
                    command.expected_lease_epoch,
                    command.completed_at_ms,
                ),
                command.result,
            )?;
            (outcome, task.revision(), task.status())
        };

        if outcome == CompletionOutcome::Completed {
            self.active_lease_tasks.remove(&command.lease_id);
            self.remove_member_lease_task(&command.group_id, &command.member_id, &command.task_id);
            self.completed_tasks
                .push(Reverse((command.completed_at_ms, command.task_id.clone())));
            self.changed_tasks.insert(command.task_id.clone());
            self.state.revision = next_global_revision;
        }
        Ok(TaskCompletedResult {
            metadata: self.metadata(),
            task_id: command.task_id,
            task_revision,
            status,
        })
    }

    fn prune_completed_tasks(
        &mut self,
        command: PruneCompletedTasksCommand,
    ) -> Result<CompletedTasksPrunedResult, StateMachineError> {
        if command.max_tasks == 0 {
            return Err(StateMachineError::InvalidCapacity { field: "max_tasks" });
        }

        let mut probe = self.completed_tasks.clone();
        let mut selected = Vec::new();
        while selected.len() < command.max_tasks {
            let Some(Reverse((completed_at_ms, task_id))) = probe.pop() else {
                break;
            };
            if completed_at_ms > command.completed_before_ms {
                break;
            }
            let Some(task) = self.state.tasks.get(&task_id) else {
                continue;
            };
            let TaskState::Completed(completed) = task.state() else {
                continue;
            };
            if completed.completed_at_ms() == completed_at_ms {
                selected.push((completed_at_ms, task_id, task.namespace_id().clone()));
            }
        }
        if selected.is_empty() {
            return Ok(CompletedTasksPrunedResult {
                metadata: self.metadata(),
                pruned_count: 0,
            });
        }

        let mut per_namespace = BTreeMap::<NamespaceId, usize>::new();
        for (_, _, namespace_id) in &selected {
            *per_namespace.entry(namespace_id.clone()).or_default() += 1;
        }
        for (namespace_id, selected_count) in &per_namespace {
            let current_count = self
                .namespace_task_counts
                .get(namespace_id)
                .copied()
                .unwrap_or(0);
            if current_count < *selected_count {
                return Err(StateMachineError::TaskCountUnderflow(namespace_id.clone()));
            }
        }
        let next_global_revision = self.state.revision.next()?;

        let mut pruned_count = 0;
        while pruned_count < selected.len() {
            let Some(Reverse((completed_at_ms, task_id))) = self.completed_tasks.pop() else {
                break;
            };
            if completed_at_ms > command.completed_before_ms {
                self.completed_tasks
                    .push(Reverse((completed_at_ms, task_id)));
                break;
            }
            let namespace_id = self.state.tasks.get(&task_id).and_then(|task| {
                let TaskState::Completed(completed) = task.state() else {
                    return None;
                };
                (completed.completed_at_ms() == completed_at_ms)
                    .then(|| task.namespace_id().clone())
            });
            let Some(namespace_id) = namespace_id else {
                continue;
            };
            self.state.tasks.remove(&task_id);
            let current_count = self
                .namespace_task_counts
                .get(&namespace_id)
                .copied()
                .ok_or_else(|| StateMachineError::TaskCountUnderflow(namespace_id.clone()))?;
            self.namespace_task_counts
                .insert(namespace_id, current_count - 1);
            self.deleted_tasks.insert(task_id);
            pruned_count += 1;
        }
        if pruned_count > 0 {
            self.state.revision = next_global_revision;
        }
        Ok(CompletedTasksPrunedResult {
            metadata: self.metadata(),
            pruned_count,
        })
    }

    fn advance_term(
        &mut self,
        command: AdvanceTermCommand,
    ) -> Result<TermAdvancedResult, StateMachineError> {
        self.require_term(command.expected_term)?;
        if command.new_term <= self.state.term {
            return Err(StateMachineError::NewTermNotGreater {
                current: self.state.term,
                proposed: command.new_term,
            });
        }
        let next_revision = self.state.revision.next()?;
        self.state.term = command.new_term;
        self.state.revision = next_revision;
        Ok(TermAdvancedResult {
            metadata: self.metadata(),
        })
    }

    fn require_term(&self, expected: Term) -> Result<(), StateMachineError> {
        if self.state.term == expected {
            return Ok(());
        }
        Err(StateMachineError::StaleTerm {
            expected,
            actual: self.state.term,
        })
    }

    fn require_group_member(
        &self,
        group_id: &ConsumerGroupId,
        member_id: &ConsumerId,
        expected_generation: Generation,
    ) -> Result<(NamespaceId, Generation), StateMachineError> {
        GroupCoordinator::require_member(
            &self.state.groups,
            group_id,
            member_id,
            expected_generation,
        )
        .map_err(StateMachineError::from)
    }

    fn checked_deadline(
        now_ms: TimestampMs,
        duration_ms: u64,
    ) -> Result<TimestampMs, StateMachineError> {
        now_ms
            .get()
            .checked_add(duration_ms)
            .map(TimestampMs::new)
            .ok_or(StateMachineError::TimestampOverflow)
    }

    fn task_claim_result(&self, generation: Generation, task: Option<&Task>) -> TaskClaimResult {
        let Some(task) = task else {
            return TaskClaimResult {
                metadata: self.metadata(),
                task_id: None,
                objective: None,
                task_revision: None,
                lease_id: None,
                lease_epoch: None,
                lease_expires_at_ms: None,
                generation,
            };
        };
        let TaskState::Leased(lease) = task.state() else {
            return TaskClaimResult {
                metadata: self.metadata(),
                task_id: None,
                objective: None,
                task_revision: None,
                lease_id: None,
                lease_epoch: None,
                lease_expires_at_ms: None,
                generation,
            };
        };
        TaskClaimResult {
            metadata: self.metadata(),
            task_id: Some(task.task_id().clone()),
            objective: Some(task.objective().clone()),
            task_revision: Some(task.revision()),
            lease_id: Some(lease.lease_id().clone()),
            lease_epoch: Some(lease.lease_epoch()),
            lease_expires_at_ms: Some(lease.expires_at_ms()),
            generation,
        }
    }

    fn pop_ready_task(&mut self, namespace_id: &NamespaceId) -> Option<TaskId> {
        let heap = self.ready_tasks.get_mut(namespace_id)?;
        while let Some(Reverse((_created_at_ms, task_id))) = heap.pop() {
            let is_ready = self
                .state
                .tasks
                .get(&task_id)
                .is_some_and(|task| task.status() == TaskStatus::Queued);
            if is_ready {
                return Some(task_id);
            }
        }
        None
    }

    fn recover_expired_leases(
        &mut self,
        namespace_id: &NamespaceId,
        now_ms: TimestampMs,
    ) -> Result<usize, StateMachineError> {
        let Some(heap) = self.lease_expirations.get_mut(namespace_id) else {
            return Ok(0);
        };
        let mut candidates = BTreeSet::new();
        while let Some(Reverse((expires_at_ms, task_id, lease_epoch))) = heap.peek().cloned() {
            if expires_at_ms > now_ms {
                break;
            }
            heap.pop();
            let Some(task) = self.state.tasks.get(&task_id) else {
                continue;
            };
            let TaskState::Leased(lease) = task.state() else {
                continue;
            };
            if lease.lease_epoch() == lease_epoch && lease.expires_at_ms() == expires_at_ms {
                candidates.insert(task_id);
            }
        }
        let mut recovered = 0;
        for task_id in candidates {
            let lease_identity = self.state.tasks.get(&task_id).and_then(|task| {
                let TaskState::Leased(lease) = task.state() else {
                    return None;
                };
                Some((
                    lease.lease_id().clone(),
                    lease.group_id().clone(),
                    lease.owner_member_id().clone(),
                    task.created_at_ms(),
                ))
            });
            let Some((lease_id, group_id, member_id, created_at_ms)) = lease_identity else {
                continue;
            };
            let requeued = self
                .state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| StateMachineError::TaskNotFound(task_id.clone()))?
                .requeue_if_expired(now_ms)?;
            if !requeued {
                continue;
            }
            self.active_lease_tasks.remove(&lease_id);
            self.remove_member_lease_task(&group_id, &member_id, &task_id);
            self.ready_tasks
                .entry(namespace_id.clone())
                .or_default()
                .push(Reverse((created_at_ms, task_id.clone())));
            self.changed_tasks.insert(task_id);
            recovered += 1;
        }
        Ok(recovered)
    }

    fn remove_member_lease_task(
        &mut self,
        group_id: &ConsumerGroupId,
        member_id: &ConsumerId,
        task_id: &TaskId,
    ) {
        let key = (group_id.clone(), member_id.clone());
        let should_remove = self.member_lease_tasks.get_mut(&key).is_some_and(|tasks| {
            tasks.remove(task_id);
            tasks.is_empty()
        });
        if should_remove {
            self.member_lease_tasks.remove(&key);
        }
    }

    fn preflight_member_lease_requeue(
        &self,
        group_id: &ConsumerGroupId,
        member_id: &ConsumerId,
    ) -> Result<(), StateMachineError> {
        let key = (group_id.clone(), member_id.clone());
        let Some(task_ids) = self.member_lease_tasks.get(&key) else {
            return Ok(());
        };
        for task_id in task_ids {
            let Some(task) = self.state.tasks.get(task_id) else {
                continue;
            };
            let TaskState::Leased(lease) = task.state() else {
                continue;
            };
            if lease.group_id() == group_id && lease.owner_member_id() == member_id {
                task.revision().next()?;
            }
        }
        Ok(())
    }

    fn requeue_member_leases(
        &mut self,
        group_id: &ConsumerGroupId,
        member_id: &ConsumerId,
    ) -> Result<usize, StateMachineError> {
        let key = (group_id.clone(), member_id.clone());
        let task_ids = self.member_lease_tasks.remove(&key).unwrap_or_default();
        let mut requeued = 0;
        for task_id in task_ids {
            let lease_identity = self.state.tasks.get(&task_id).and_then(|task| {
                let TaskState::Leased(lease) = task.state() else {
                    return None;
                };
                if lease.group_id() != group_id || lease.owner_member_id() != member_id {
                    return None;
                }
                Some((lease.lease_id().clone(), task.created_at_ms()))
            });
            let Some((lease_id, created_at_ms)) = lease_identity else {
                continue;
            };
            let did_requeue = self
                .state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| StateMachineError::TaskNotFound(task_id.clone()))?
                .requeue_for_member(group_id, member_id)?;
            if !did_requeue {
                continue;
            }
            self.active_lease_tasks.remove(&lease_id);
            let namespace_id = self
                .state
                .tasks
                .get(&task_id)
                .ok_or_else(|| StateMachineError::TaskNotFound(task_id.clone()))?
                .namespace_id()
                .clone();
            self.ready_tasks
                .entry(namespace_id)
                .or_default()
                .push(Reverse((created_at_ms, task_id.clone())));
            self.changed_tasks.insert(task_id);
            requeued += 1;
        }
        Ok(requeued)
    }

    fn require_namespace(&self, namespace_id: &NamespaceId) -> Result<(), StateMachineError> {
        if self.state.namespaces.contains_key(namespace_id) {
            return Ok(());
        }
        Err(StateMachineError::NamespaceNotFound(namespace_id.clone()))
    }

    fn consumer_group_result(
        metadata: MutationMetadata,
        group: &ConsumerGroup,
    ) -> ConsumerGroupResult {
        ConsumerGroupResult {
            metadata,
            group_id: group.group_id().clone(),
            generation: group.generation(),
            group_revision: group.revision(),
            member_count: group.consumer_count(),
        }
    }

    fn clear_changes(&mut self) {
        self.changed_namespaces.clear();
        self.changed_tasks.clear();
        self.deleted_tasks.clear();
        self.changed_groups.clear();
    }

    fn take_changes(&self) -> StateChangeSet {
        StateChangeSet {
            namespaces: self.changed_namespaces.iter().cloned().collect(),
            tasks: self.changed_tasks.iter().cloned().collect(),
            deleted_tasks: self.deleted_tasks.iter().cloned().collect(),
            groups: self.changed_groups.iter().cloned().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{BrokerStateMachine, StateMachineError};
    use crate::commands::{
        BrokerCommand, ClaimTaskCommand, CompleteTaskCommand, EnsureConsumerGroupCommand,
        EnsureNamespaceCommand, HeartbeatCommand, JoinConsumerGroupCommand,
        LeaveConsumerGroupCommand, PublishTaskCommand, ReapStaleMembersCommand,
        RenewTaskLeaseCommand,
    };
    use crate::results::BrokerMutationResult;
    use crate::{
        Capabilities, ConsumerGroupError, ConsumerGroupId, ConsumerId, Generation, LeaseEpoch,
        LeaseId, NamespaceId, TaskId, TaskObjective, TaskResult, TaskState, TaskStatus,
        TaskTransitionError, Term, TimestampMs,
    };

    fn namespace_id() -> Result<NamespaceId, Box<dyn Error>> {
        Ok(NamespaceId::new("project-a")?)
    }

    fn ensure_namespace(
        machine: &mut BrokerStateMachine,
        max_namespaces: usize,
    ) -> Result<(), Box<dyn Error>> {
        machine.apply(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: namespace_id()?,
            max_namespaces,
        }))?;
        Ok(())
    }

    fn ensure_group(machine: &mut BrokerStateMachine) -> Result<ConsumerGroupId, Box<dyn Error>> {
        ensure_namespace(machine, 8)?;
        let group_id = ConsumerGroupId::new("engineering")?;
        machine.apply(BrokerCommand::EnsureConsumerGroup(
            EnsureConsumerGroupCommand {
                namespace_id: namespace_id()?,
                group_id: group_id.clone(),
                max_namespace_groups: 8,
            },
        ))?;
        Ok(group_id)
    }

    fn join_worker(
        machine: &mut BrokerStateMachine,
        member_id: &ConsumerId,
    ) -> Result<(ConsumerGroupId, Generation), Box<dyn Error>> {
        let group_id = ensure_group(machine)?;
        let joined = machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            capabilities: Capabilities::new(["code"])?,
            now_ms: TimestampMs::new(1_000),
            max_group_members: 256,
        }))?;
        let BrokerMutationResult::ConsumerGroup(group) = joined.result else {
            return Err("join result must be ConsumerGroup".into());
        };
        Ok((group_id, group.generation))
    }

    fn publish_task(
        machine: &mut BrokerStateMachine,
        task_id: &str,
        created_at_ms: u64,
    ) -> Result<TaskId, Box<dyn Error>> {
        let task_id = TaskId::new(task_id)?;
        machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: namespace_id()?,
            task_id: task_id.clone(),
            objective: TaskObjective::new(format!("objective-{task_id}"))?,
            created_at_ms: TimestampMs::new(created_at_ms),
            max_namespace_tasks: 64,
        }))?;
        Ok(task_id)
    }

    #[test]
    fn ensure_namespace_is_idempotent_and_capacity_only_applies_to_new_resources()
    -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        let first = machine.apply(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: namespace_id()?,
            max_namespaces: 1,
        }))?;
        assert_eq!(machine.state().revision().get(), 1);
        assert_eq!(first.changes.namespaces.len(), 1);

        let retry = machine.apply(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: namespace_id()?,
            max_namespaces: 0,
        }))?;
        assert_eq!(machine.state().revision().get(), 1);
        assert!(retry.changes.is_empty());

        let result = machine.apply(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: NamespaceId::new("project-b")?,
            max_namespaces: 1,
        }));
        assert!(matches!(
            result,
            Err(StateMachineError::CapacityExceeded {
                resource: "Broker namespace",
                max: 1
            })
        ));
        Ok(())
    }

    #[test]
    fn publish_task_is_idempotent_and_rejects_conflicting_content() -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        ensure_namespace(&mut machine, 8)?;
        let command = PublishTaskCommand {
            namespace_id: namespace_id()?,
            task_id: TaskId::new("task-1")?,
            objective: TaskObjective::new("Implement typed Broker state machine")?,
            created_at_ms: TimestampMs::new(1_001),
            max_namespace_tasks: 1,
        };
        let first = machine.apply(BrokerCommand::PublishTask(command.clone()))?;
        assert_eq!(machine.state().revision().get(), 2);
        assert_eq!(first.changes.tasks, [TaskId::new("task-1")?]);
        let BrokerMutationResult::TaskPublished(first_result) = first.result else {
            return Err("publish result must be TaskPublished".into());
        };
        assert_eq!(first_result.status, TaskStatus::Queued);

        let retry = machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
            max_namespace_tasks: 0,
            ..command.clone()
        }))?;
        assert!(retry.changes.is_empty());
        assert_eq!(machine.state().revision().get(), 2);

        let conflict = machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
            objective: TaskObjective::new("different")?,
            ..command
        }));
        assert!(matches!(conflict, Err(StateMachineError::TaskConflict(_))));
        Ok(())
    }

    #[test]
    fn publish_task_requires_namespace_and_enforces_per_namespace_capacity()
    -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        let missing = machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: namespace_id()?,
            task_id: TaskId::new("task-1")?,
            objective: TaskObjective::new("missing namespace")?,
            created_at_ms: TimestampMs::new(1),
            max_namespace_tasks: 1,
        }));
        assert!(matches!(
            missing,
            Err(StateMachineError::NamespaceNotFound(_))
        ));

        ensure_namespace(&mut machine, 8)?;
        machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: namespace_id()?,
            task_id: TaskId::new("task-1")?,
            objective: TaskObjective::new("one")?,
            created_at_ms: TimestampMs::new(1),
            max_namespace_tasks: 1,
        }))?;
        let full = machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: namespace_id()?,
            task_id: TaskId::new("task-2")?,
            objective: TaskObjective::new("two")?,
            created_at_ms: TimestampMs::new(2),
            max_namespace_tasks: 1,
        }));
        assert!(matches!(
            full,
            Err(StateMachineError::CapacityExceeded {
                resource: "Broker Task",
                max: 1
            })
        ));
        Ok(())
    }

    #[test]
    fn ensure_consumer_group_matches_python_defaults_and_namespace_conflict()
    -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        ensure_namespace(&mut machine, 8)?;
        machine.apply(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: NamespaceId::new("project-b")?,
            max_namespaces: 8,
        }))?;
        let group_id = ConsumerGroupId::new("engineering")?;
        let first = machine.apply(BrokerCommand::EnsureConsumerGroup(
            EnsureConsumerGroupCommand {
                namespace_id: namespace_id()?,
                group_id: group_id.clone(),
                max_namespace_groups: 1,
            },
        ))?;
        let BrokerMutationResult::ConsumerGroup(group) = first.result else {
            return Err("ensure group result must be ConsumerGroup".into());
        };
        assert_eq!(group.generation.get(), 0);
        assert_eq!(group.group_revision.get(), 1);
        assert_eq!(group.member_count, 0);

        let retry = machine.apply(BrokerCommand::EnsureConsumerGroup(
            EnsureConsumerGroupCommand {
                namespace_id: namespace_id()?,
                group_id: group_id.clone(),
                max_namespace_groups: 0,
            },
        ))?;
        assert!(retry.changes.is_empty());

        let conflict = machine.apply(BrokerCommand::EnsureConsumerGroup(
            EnsureConsumerGroupCommand {
                namespace_id: NamespaceId::new("project-b")?,
                group_id,
                max_namespace_groups: 8,
            },
        ));
        assert!(matches!(
            conflict,
            Err(StateMachineError::ConsumerGroupConflict(_))
        ));
        Ok(())
    }

    #[test]
    fn join_retry_and_heartbeat_preserve_python_global_revision_semantics()
    -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        let group_id = ensure_group(&mut machine)?;
        let member_id = ConsumerId::new("worker-a")?;
        let capabilities = Capabilities::new(["review", "code", "review"])?;
        let before_join_revision = machine.state().revision();

        let joined = machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            capabilities: capabilities.clone(),
            now_ms: TimestampMs::new(1_000),
            max_group_members: 256,
        }))?;
        assert_eq!(
            machine.state().revision().get(),
            before_join_revision.get() + 1
        );
        assert_eq!(
            joined.changes.groups.as_slice(),
            std::slice::from_ref(&group_id)
        );
        let BrokerMutationResult::ConsumerGroup(joined_group) = joined.result else {
            return Err("join result must be ConsumerGroup".into());
        };
        assert_eq!(joined_group.generation.get(), 1);
        assert_eq!(joined_group.member_count, 1);

        let retry_revision = machine.state().revision();
        let retry = machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            capabilities: capabilities.clone(),
            now_ms: TimestampMs::new(1_000),
            max_group_members: 0,
        }))?;
        assert_eq!(machine.state().revision(), retry_revision);
        assert!(retry.changes.is_empty());

        let refreshed =
            machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
                group_id: group_id.clone(),
                member_id: member_id.clone(),
                capabilities,
                now_ms: TimestampMs::new(2_000),
                max_group_members: 0,
            }))?;
        assert_eq!(machine.state().revision().get(), retry_revision.get() + 1);
        let BrokerMutationResult::ConsumerGroup(refreshed_group) = refreshed.result else {
            return Err("refresh result must be ConsumerGroup".into());
        };
        assert_eq!(refreshed_group.generation.get(), 1);

        let heartbeat_revision = machine.state().revision();
        let unchanged = machine.apply(BrokerCommand::Heartbeat(HeartbeatCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_generation: Generation::new(1),
            now_ms: TimestampMs::new(2_000),
        }))?;
        assert_eq!(machine.state().revision(), heartbeat_revision);
        assert!(unchanged.changes.is_empty());

        let updated = machine.apply(BrokerCommand::Heartbeat(HeartbeatCommand {
            group_id: group_id.clone(),
            member_id,
            expected_generation: Generation::new(1),
            now_ms: TimestampMs::new(3_000),
        }))?;
        assert_eq!(
            machine.state().revision().get(),
            heartbeat_revision.get() + 1
        );
        assert_eq!(updated.changes.groups, [group_id]);
        let BrokerMutationResult::Heartbeat(heartbeat) = updated.result else {
            return Err("heartbeat result must be Heartbeat".into());
        };
        assert_eq!(heartbeat.generation.get(), 1);
        assert_eq!(heartbeat.member_revision.get(), 3);
        Ok(())
    }

    #[test]
    fn stale_generation_is_rejected_and_leave_advances_generation() -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        let group_id = ensure_group(&mut machine)?;
        let member_id = ConsumerId::new("worker-a")?;
        machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            capabilities: Capabilities::new(["code"])?,
            now_ms: TimestampMs::new(1_000),
            max_group_members: 256,
        }))?;

        let stale = machine.apply(BrokerCommand::Heartbeat(HeartbeatCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_generation: Generation::new(0),
            now_ms: TimestampMs::new(2_000),
        }));
        assert!(matches!(
            stale,
            Err(StateMachineError::ConsumerGroupTransition(
                ConsumerGroupError::StaleGeneration { .. }
            ))
        ));

        let before_leave_revision = machine.state().revision();
        let left = machine.apply(BrokerCommand::LeaveConsumerGroup(
            LeaveConsumerGroupCommand {
                group_id: group_id.clone(),
                member_id,
                expected_generation: Generation::new(1),
            },
        ))?;
        assert_eq!(
            machine.state().revision().get(),
            before_leave_revision.get() + 1
        );
        assert_eq!(left.changes.groups, [group_id]);
        let BrokerMutationResult::ConsumerGroup(left_group) = left.result else {
            return Err("leave result must be ConsumerGroup".into());
        };
        assert_eq!(left_group.generation.get(), 2);
        assert_eq!(left_group.member_count, 0);
        Ok(())
    }

    #[test]
    fn membership_commands_reject_unknown_group() -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        let group_id = ConsumerGroupId::new("missing")?;
        let join = machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
            group_id: group_id.clone(),
            member_id: ConsumerId::new("worker-a")?,
            capabilities: Capabilities::new(["code"])?,
            now_ms: TimestampMs::new(1_000),
            max_group_members: 256,
        }));
        assert_eq!(
            join,
            Err(StateMachineError::ConsumerGroupNotFound(group_id))
        );
        Ok(())
    }

    #[test]
    fn global_stale_reap_uses_oldest_heartbeat_order_and_one_revision_per_command()
    -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        ensure_namespace(&mut machine, 8)?;
        let group_a = ConsumerGroupId::new("group-a")?;
        let group_b = ConsumerGroupId::new("group-b")?;
        for group_id in [&group_a, &group_b] {
            machine.apply(BrokerCommand::EnsureConsumerGroup(
                EnsureConsumerGroupCommand {
                    namespace_id: namespace_id()?,
                    group_id: group_id.clone(),
                    max_namespace_groups: 8,
                },
            ))?;
        }

        for (group_id, member_id, heartbeat_ms) in [
            (&group_a, "a-old", 1_000),
            (&group_b, "b-old", 2_000),
            (&group_a, "a-new", 4_000),
            (&group_b, "b-new", 5_000),
        ] {
            machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
                group_id: group_id.clone(),
                member_id: ConsumerId::new(member_id)?,
                capabilities: Capabilities::new(["code"])?,
                now_ms: TimestampMs::new(heartbeat_ms),
                max_group_members: 256,
            }))?;
        }

        let before_reap = machine.state().revision();
        let reaped = machine.apply(BrokerCommand::ReapStaleMembers(ReapStaleMembersCommand {
            stale_before_ms: TimestampMs::new(5_000),
            max_members: 2,
        }))?;
        assert_eq!(machine.state().revision().get(), before_reap.get() + 1);
        let BrokerMutationResult::StaleMembersReaped(result) = reaped.result else {
            return Err("reap result must be StaleMembersReaped".into());
        };
        assert_eq!(result.reaped_count, 2);
        assert_eq!(result.affected_group_count, 2);
        assert_eq!(reaped.changes.groups, [group_a.clone(), group_b.clone()]);

        let remaining_a = machine
            .state()
            .group(&group_a)
            .map(|group| {
                group
                    .consumers()
                    .map(|member| member.consumer_id().as_str())
                    .collect::<Vec<_>>()
            })
            .ok_or("group-a must exist")?;
        let remaining_b = machine
            .state()
            .group(&group_b)
            .map(|group| {
                group
                    .consumers()
                    .map(|member| member.consumer_id().as_str())
                    .collect::<Vec<_>>()
            })
            .ok_or("group-b must exist")?;
        assert_eq!(remaining_a, ["a-new"]);
        assert_eq!(remaining_b, ["b-new"]);
        Ok(())
    }

    #[test]
    fn stale_reap_observes_refreshed_heartbeat_and_noop_does_not_mutate()
    -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        let group_id = ensure_group(&mut machine)?;
        let member_id = ConsumerId::new("worker-a")?;
        let capabilities = Capabilities::new(["code"])?;
        machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            capabilities: capabilities.clone(),
            now_ms: TimestampMs::new(1_000),
            max_group_members: 256,
        }))?;
        machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
            group_id: group_id.clone(),
            member_id,
            capabilities,
            now_ms: TimestampMs::new(10_000),
            max_group_members: 0,
        }))?;

        let before_reap = machine.state().revision();
        let reaped = machine.apply(BrokerCommand::ReapStaleMembers(ReapStaleMembersCommand {
            stale_before_ms: TimestampMs::new(5_000),
            max_members: 8,
        }))?;
        assert_eq!(machine.state().revision(), before_reap);
        assert!(reaped.changes.is_empty());
        let BrokerMutationResult::StaleMembersReaped(result) = reaped.result else {
            return Err("reap result must be StaleMembersReaped".into());
        };
        assert_eq!(result.reaped_count, 0);
        assert_eq!(result.affected_group_count, 0);
        assert_eq!(
            machine
                .state()
                .group(&group_id)
                .map(crate::ConsumerGroup::consumer_count),
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn claim_uses_oldest_ready_task_and_is_idempotent_for_same_active_lease()
    -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        let member_id = ConsumerId::new("worker-a")?;
        let (group_id, generation) = join_worker(&mut machine, &member_id)?;
        let newer = publish_task(&mut machine, "task-newer", 2_000)?;
        let older = publish_task(&mut machine, "task-older", 1_000)?;
        let lease_id = LeaseId::new("lease-1")?;
        let before_claim = machine.state().revision();
        let command = ClaimTaskCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: generation,
            lease_id: lease_id.clone(),
            now_ms: TimestampMs::new(3_000),
            lease_duration_ms: 10_000,
        };

        let claimed = machine.apply(BrokerCommand::ClaimTask(command.clone()))?;
        assert_eq!(machine.state().revision().get(), before_claim.get() + 1);
        let BrokerMutationResult::TaskClaim(result) = claimed.result else {
            return Err("claim result must be TaskClaim".into());
        };
        assert_eq!(result.task_id.as_ref(), Some(&older));
        assert_eq!(result.lease_epoch, Some(LeaseEpoch::new(1)));
        assert_eq!(
            claimed.changes.tasks.as_slice(),
            std::slice::from_ref(&older)
        );
        assert_eq!(
            machine.state().task(&newer).map(crate::Task::status),
            Some(TaskStatus::Queued)
        );

        let revision_after_claim = machine.state().revision();
        let retry = machine.apply(BrokerCommand::ClaimTask(command))?;
        assert_eq!(machine.state().revision(), revision_after_claim);
        assert!(retry.changes.is_empty());
        let BrokerMutationResult::TaskClaim(retry_result) = retry.result else {
            return Err("retry result must be TaskClaim".into());
        };
        assert_eq!(retry_result.task_id.as_ref(), Some(&older));
        assert_eq!(retry_result.lease_id.as_ref(), Some(&lease_id));
        Ok(())
    }

    #[test]
    fn claim_rejects_stale_term_generation_and_duplicate_lease_owner() -> Result<(), Box<dyn Error>>
    {
        let mut machine = BrokerStateMachine::default();
        let worker_a = ConsumerId::new("worker-a")?;
        let worker_b = ConsumerId::new("worker-b")?;
        let (group_id, _first_generation) = join_worker(&mut machine, &worker_a)?;
        let joined_b =
            machine.apply(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
                group_id: group_id.clone(),
                member_id: worker_b.clone(),
                capabilities: Capabilities::new(["code"])?,
                now_ms: TimestampMs::new(1_100),
                max_group_members: 256,
            }))?;
        let BrokerMutationResult::ConsumerGroup(group) = joined_b.result else {
            return Err("join result must be ConsumerGroup".into());
        };
        let generation = group.generation;
        publish_task(&mut machine, "task-1", 2_000)?;
        let lease_id = LeaseId::new("lease-shared")?;

        let stale_term = machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id: group_id.clone(),
            member_id: worker_a.clone(),
            expected_term: Term::new(2)?,
            expected_generation: generation,
            lease_id: LeaseId::new("lease-stale-term")?,
            now_ms: TimestampMs::new(3_000),
            lease_duration_ms: 10_000,
        }));
        assert!(matches!(
            stale_term,
            Err(StateMachineError::StaleTerm { .. })
        ));

        let stale_generation = machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id: group_id.clone(),
            member_id: worker_a.clone(),
            expected_term: Term::INITIAL,
            expected_generation: Generation::new(generation.get() - 1),
            lease_id: LeaseId::new("lease-stale-generation")?,
            now_ms: TimestampMs::new(3_000),
            lease_duration_ms: 10_000,
        }));
        assert!(matches!(
            stale_generation,
            Err(StateMachineError::ConsumerGroupTransition(
                ConsumerGroupError::StaleGeneration { .. }
            ))
        ));

        machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id: group_id.clone(),
            member_id: worker_a,
            expected_term: Term::INITIAL,
            expected_generation: generation,
            lease_id: lease_id.clone(),
            now_ms: TimestampMs::new(3_000),
            lease_duration_ms: 10_000,
        }))?;
        let duplicate = machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id,
            member_id: worker_b,
            expected_term: Term::INITIAL,
            expected_generation: generation,
            lease_id: lease_id.clone(),
            now_ms: TimestampMs::new(3_001),
            lease_duration_ms: 10_000,
        }));
        assert_eq!(duplicate, Err(StateMachineError::LeaseIdConflict(lease_id)));
        Ok(())
    }

    #[test]
    fn expired_lease_requeues_and_reclaims_with_one_global_revision_and_next_epoch()
    -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        let member_id = ConsumerId::new("worker-a")?;
        let (group_id, generation) = join_worker(&mut machine, &member_id)?;
        let task_id = publish_task(&mut machine, "task-1", 1_500)?;
        machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: generation,
            lease_id: LeaseId::new("lease-1")?,
            now_ms: TimestampMs::new(2_000),
            lease_duration_ms: 1_000,
        }))?;
        let before_reclaim = machine.state().revision();

        let reclaimed = machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id,
            member_id,
            expected_term: Term::INITIAL,
            expected_generation: generation,
            lease_id: LeaseId::new("lease-2")?,
            now_ms: TimestampMs::new(3_000),
            lease_duration_ms: 1_000,
        }))?;
        assert_eq!(machine.state().revision().get(), before_reclaim.get() + 1);
        let BrokerMutationResult::TaskClaim(result) = reclaimed.result else {
            return Err("reclaim result must be TaskClaim".into());
        };
        assert_eq!(result.task_id.as_ref(), Some(&task_id));
        assert_eq!(result.lease_epoch, Some(LeaseEpoch::new(2)));
        let TaskState::Leased(lease) = machine
            .state()
            .task(&task_id)
            .map(crate::Task::state)
            .ok_or("task must exist")?
        else {
            return Err("reclaimed task must be leased".into());
        };
        assert_eq!(lease.lease_epoch().get(), 2);
        Ok(())
    }

    #[test]
    fn renew_keeps_epoch_and_stale_expiration_heap_entry_cannot_requeue()
    -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        let member_id = ConsumerId::new("worker-a")?;
        let (group_id, generation) = join_worker(&mut machine, &member_id)?;
        let task_id = publish_task(&mut machine, "task-1", 1_500)?;
        let lease_id = LeaseId::new("lease-1")?;
        let claimed = machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: generation,
            lease_id: lease_id.clone(),
            now_ms: TimestampMs::new(2_000),
            lease_duration_ms: 1_000,
        }))?;
        let BrokerMutationResult::TaskClaim(claim) = claimed.result else {
            return Err("claim result must be TaskClaim".into());
        };
        let lease_epoch = claim.lease_epoch.ok_or("lease epoch must exist")?;
        let before_renew = machine.state().revision();

        let renewed = machine.apply(BrokerCommand::RenewTaskLease(RenewTaskLeaseCommand {
            task_id: task_id.clone(),
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: generation,
            expected_lease_epoch: lease_epoch,
            lease_id: lease_id.clone(),
            now_ms: TimestampMs::new(2_500),
            lease_duration_ms: 5_000,
        }))?;
        assert_eq!(machine.state().revision().get(), before_renew.get() + 1);
        let BrokerMutationResult::TaskLeaseRenewed(renewed) = renewed.result else {
            return Err("renew result must be TaskLeaseRenewed".into());
        };
        assert_eq!(renewed.lease_epoch, lease_epoch);
        assert_eq!(renewed.lease_expires_at_ms, TimestampMs::new(7_500));

        let no_ready = machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id,
            member_id,
            expected_term: Term::INITIAL,
            expected_generation: generation,
            lease_id: LeaseId::new("lease-2")?,
            now_ms: TimestampMs::new(3_000),
            lease_duration_ms: 1_000,
        }))?;
        let BrokerMutationResult::TaskClaim(no_ready) = no_ready.result else {
            return Err("claim result must be TaskClaim".into());
        };
        assert_eq!(no_ready.task_id, None);
        assert_eq!(
            machine.state().task(&task_id).map(crate::Task::status),
            Some(TaskStatus::Leased)
        );
        Ok(())
    }

    #[test]
    fn complete_is_fenced_and_identical_retry_does_not_mutate() -> Result<(), Box<dyn Error>> {
        let mut machine = BrokerStateMachine::default();
        let member_id = ConsumerId::new("worker-a")?;
        let (group_id, generation) = join_worker(&mut machine, &member_id)?;
        let task_id = publish_task(&mut machine, "task-1", 1_500)?;
        let lease_id = LeaseId::new("lease-1")?;
        let claimed = machine.apply(BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: generation,
            lease_id: lease_id.clone(),
            now_ms: TimestampMs::new(2_000),
            lease_duration_ms: 10_000,
        }))?;
        let BrokerMutationResult::TaskClaim(claim) = claimed.result else {
            return Err("claim result must be TaskClaim".into());
        };
        let lease_epoch = claim.lease_epoch.ok_or("lease epoch must exist")?;

        let stale = machine.apply(BrokerCommand::CompleteTask(CompleteTaskCommand {
            task_id: task_id.clone(),
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: generation,
            expected_lease_epoch: LeaseEpoch::new(lease_epoch.get() + 1),
            lease_id: lease_id.clone(),
            result: TaskResult::new("done")?,
            completed_at_ms: TimestampMs::new(3_000),
        }));
        assert_eq!(
            stale,
            Err(StateMachineError::TaskTransition(
                TaskTransitionError::StaleLeaseEpoch
            ))
        );

        let before_complete = machine.state().revision();
        let completion = CompleteTaskCommand {
            task_id: task_id.clone(),
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: generation,
            expected_lease_epoch: lease_epoch,
            lease_id: lease_id.clone(),
            result: TaskResult::new("done")?,
            completed_at_ms: TimestampMs::new(3_000),
        };
        let completed = machine.apply(BrokerCommand::CompleteTask(completion.clone()))?;
        assert_eq!(machine.state().revision().get(), before_complete.get() + 1);
        let BrokerMutationResult::TaskCompleted(result) = completed.result else {
            return Err("complete result must be TaskCompleted".into());
        };
        assert_eq!(result.status, TaskStatus::Completed);

        let retry_revision = machine.state().revision();
        let retry = machine.apply(BrokerCommand::CompleteTask(completion))?;
        assert_eq!(machine.state().revision(), retry_revision);
        assert!(retry.changes.is_empty());

        let conflict = machine.apply(BrokerCommand::CompleteTask(CompleteTaskCommand {
            task_id,
            group_id,
            member_id,
            expected_term: Term::INITIAL,
            expected_generation: generation,
            expected_lease_epoch: lease_epoch,
            lease_id,
            result: TaskResult::new("different")?,
            completed_at_ms: TimestampMs::new(3_001),
        }));
        assert_eq!(
            conflict,
            Err(StateMachineError::TaskTransition(
                TaskTransitionError::AlreadyCompleted
            ))
        );
        Ok(())
    }
}

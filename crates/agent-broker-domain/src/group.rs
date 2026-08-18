use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use crate::checkpoint::{CheckpointError, ConsumerGroupCheckpoint, MemberCheckpoint};
use crate::{ConsumerGroupId, ConsumerId, Generation, NamespaceId, Revision, Term, TimestampMs};

mod coordinator;
pub(crate) use coordinator::{GroupCoordinator, GroupCoordinatorError};

const MAX_CAPABILITIES: usize = 64;
const MAX_CAPABILITY_BYTES: usize = 64;

/// Validated opaque worker capability used only for provider-neutral routing/admission metadata.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Capability(String);

impl Capability {
    /// Validate a capability against the Python reference identifier contract.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilitiesError`] when the value is empty, too long, or contains unsupported
    /// characters.
    pub fn new(value: impl Into<String>) -> Result<Self, CapabilitiesError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CapabilitiesError::InvalidCapability(
                "capability must not be empty",
            ));
        }
        if value.len() > MAX_CAPABILITY_BYTES {
            return Err(CapabilitiesError::InvalidCapability(
                "capability must be at most 64 ASCII bytes",
            ));
        }
        let mut bytes = value.bytes();
        let Some(first) = bytes.next() else {
            return Err(CapabilitiesError::InvalidCapability(
                "capability must not be empty",
            ));
        };
        if !first.is_ascii_alphanumeric() {
            return Err(CapabilitiesError::InvalidCapability(
                "capability must start with an ASCII alphanumeric character",
            ));
        }
        if !bytes
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-'))
        {
            return Err(CapabilitiesError::InvalidCapability(
                "capability contains unsupported characters",
            ));
        }
        Ok(Self(value))
    }

    /// Borrow the capability string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Sorted, deduplicated capability set matching the Python reference normalization behavior.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct Capabilities(Box<[Capability]>);

impl Capabilities {
    /// Validate, sort, and deduplicate capabilities.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilitiesError`] when the input contains more than 64 values before
    /// normalization or any capability is invalid.
    pub fn new<I, S>(values: I) -> Result<Self, CapabilitiesError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let values = values.into_iter().collect::<Vec<_>>();
        if values.len() > MAX_CAPABILITIES {
            return Err(CapabilitiesError::TooMany {
                max: MAX_CAPABILITIES,
            });
        }
        let mut normalized = BTreeSet::new();
        for value in values {
            normalized.insert(Capability::new(value)?);
        }
        Ok(Self(
            normalized
                .into_iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        ))
    }

    /// Borrow normalized capabilities in deterministic sorted order.
    #[must_use]
    pub fn as_slice(&self) -> &[Capability] {
        &self.0
    }

    /// Return normalized capability count.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Return whether no capabilities were declared.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return whether this advertised capability set contains every required capability.
    ///
    /// Both sets are normalized and sorted at construction time, so this comparison is
    /// deterministic and independent of provider/runtime details.
    #[must_use]
    pub fn contains_all(&self, required: &Self) -> bool {
        required
            .as_slice()
            .iter()
            .all(|capability| self.0.binary_search(capability).is_ok())
    }
}

/// Capability validation failure.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CapabilitiesError {
    /// More capability values were supplied than the bounded request permits.
    TooMany { max: usize },
    /// One capability violates the stable syntax contract.
    InvalidCapability(&'static str),
}

impl fmt::Display for CapabilitiesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooMany { max } => {
                write!(formatter, "capabilities supports at most {max} values")
            }
            Self::InvalidCapability(reason) => formatter.write_str(reason),
        }
    }
}

impl Error for CapabilitiesError {}

/// Provider-neutral logical Consumer participating in one Consumer Group.
///
/// A Consumer is intentionally not a `ChatGPT` conversation, browser tab, CLI process, wake target,
/// or orchestration slot. External runtimes bind those physical resources to this stable logical
/// identity outside Broker authority.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Consumer {
    id: ConsumerId,
    capabilities: Capabilities,
    joined_at_ms: TimestampMs,
    last_heartbeat_at_ms: TimestampMs,
    revision: Revision,
}

impl Consumer {
    fn new(id: ConsumerId, capabilities: Capabilities, now_ms: TimestampMs) -> Self {
        Self {
            id,
            capabilities,
            joined_at_ms: now_ms,
            last_heartbeat_at_ms: now_ms,
            revision: Revision::new(1),
        }
    }

    /// Borrow the logical Consumer identifier.
    #[must_use]
    pub const fn consumer_id(&self) -> &ConsumerId {
        &self.id
    }

    /// Borrow normalized capabilities.
    #[must_use]
    pub const fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// Return original join timestamp.
    #[must_use]
    pub const fn joined_at_ms(&self) -> TimestampMs {
        self.joined_at_ms
    }

    /// Return most recent accepted heartbeat timestamp.
    #[must_use]
    pub const fn last_heartbeat_at_ms(&self) -> TimestampMs {
        self.last_heartbeat_at_ms
    }

    /// Return member revision.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Export this member into the logical persistence/replication checkpoint contract.
    #[must_use]
    pub fn checkpoint(&self) -> MemberCheckpoint {
        MemberCheckpoint {
            member_id: self.id.clone(),
            capabilities: self.capabilities.clone(),
            joined_at_ms: self.joined_at_ms,
            last_heartbeat_at_ms: self.last_heartbeat_at_ms,
            revision: self.revision,
        }
    }

    fn from_checkpoint(checkpoint: MemberCheckpoint) -> Self {
        Self {
            id: checkpoint.member_id,
            capabilities: checkpoint.capabilities,
            joined_at_ms: checkpoint.joined_at_ms,
            last_heartbeat_at_ms: checkpoint.last_heartbeat_at_ms,
            revision: checkpoint.revision,
        }
    }
}

/// Bounded provider-neutral management summary for one Consumer Group / Agent Company.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConsumerGroupSummary {
    group_id: ConsumerGroupId,
    namespace_id: NamespaceId,
    generation: Generation,
    revision: Revision,
    consumer_count: usize,
}

/// One internally consistent read-only directory snapshot of all Consumer Groups.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConsumerGroupDirectory {
    term: Term,
    revision: Revision,
    groups: Box<[ConsumerGroupSummary]>,
}

impl ConsumerGroupDirectory {
    #[must_use]
    pub fn new(term: Term, revision: Revision, groups: Vec<ConsumerGroupSummary>) -> Self {
        Self {
            term,
            revision,
            groups: groups.into_boxed_slice(),
        }
    }

    #[must_use]
    pub const fn term(&self) -> Term {
        self.term
    }

    #[must_use]
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    #[must_use]
    pub fn groups(&self) -> &[ConsumerGroupSummary] {
        &self.groups
    }

    #[must_use]
    pub fn group(&self, group_id: &ConsumerGroupId) -> Option<&ConsumerGroupSummary> {
        self.groups
            .binary_search_by(|group| group.group_id().cmp(group_id))
            .ok()
            .map(|index| &self.groups[index])
    }
}

impl ConsumerGroupSummary {
    fn from_group(group: &ConsumerGroup) -> Self {
        Self {
            group_id: group.group_id().clone(),
            namespace_id: group.namespace_id().clone(),
            generation: group.generation(),
            revision: group.revision(),
            consumer_count: group.consumer_count(),
        }
    }

    #[must_use]
    pub const fn group_id(&self) -> &ConsumerGroupId {
        &self.group_id
    }

    #[must_use]
    pub const fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    #[must_use]
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    #[must_use]
    pub const fn consumer_count(&self) -> usize {
        self.consumer_count
    }
}

/// Outcome of a join retry/new admission.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum JoinOutcome {
    /// New member was added and Consumer Group generation advanced.
    Added,
    /// Existing member had the same capabilities and a newer heartbeat was recorded.
    Refreshed,
    /// Existing member retry carried no newer heartbeat and caused no mutation.
    Unchanged,
}

/// Outcome of a heartbeat command.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum HeartbeatOutcome {
    /// Consumer and group revisions advanced.
    Updated,
    /// The timestamp was not newer, so no mutation occurred.
    Unchanged,
}

/// Consumer Group transition failure.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ConsumerGroupError {
    /// Expected generation does not equal the authoritative group generation.
    StaleGeneration {
        expected: Generation,
        actual: Generation,
    },
    /// Requested member does not exist.
    MemberNotFound(ConsumerId),
    /// An existing member retried join with different capabilities.
    CapabilityConflict(ConsumerId),
    /// A new member would exceed configured group capacity.
    MemberCapacityReached { max_members: usize },
    /// Capacity must be at least one for new-member admission.
    InvalidMemberCapacity,
    /// A monotonic generation/revision could not advance safely.
    FencingValue(crate::FencingValueError),
}

impl fmt::Display for ConsumerGroupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleGeneration { expected, actual } => write!(
                formatter,
                "Consumer Group generation is stale: expected {}, actual {}",
                expected.get(),
                actual.get()
            ),
            Self::MemberNotFound(member_id) => {
                write!(formatter, "member {member_id} does not exist")
            }
            Self::CapabilityConflict(member_id) => {
                write!(
                    formatter,
                    "member {member_id} rejoined with different capabilities"
                )
            }
            Self::MemberCapacityReached { max_members } => {
                write!(
                    formatter,
                    "Consumer Group member capacity reached ({max_members})"
                )
            }
            Self::InvalidMemberCapacity => {
                formatter.write_str("max_group_members must be positive")
            }
            Self::FencingValue(error) => error.fmt(formatter),
        }
    }
}

impl Error for ConsumerGroupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::FencingValue(error) => Some(error),
            _ => None,
        }
    }
}

impl From<crate::FencingValueError> for ConsumerGroupError {
    fn from(error: crate::FencingValueError) -> Self {
        Self::FencingValue(error)
    }
}

/// Provider-neutral Consumer Group membership state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConsumerGroup {
    id: ConsumerGroupId,
    namespace_id: NamespaceId,
    generation: Generation,
    revision: Revision,
    consumers: BTreeMap<ConsumerId, Consumer>,
}

impl ConsumerGroup {
    /// Create an empty Consumer Group matching Python reference defaults.
    #[must_use]
    pub const fn new(id: ConsumerGroupId, namespace_id: NamespaceId) -> Self {
        Self {
            id,
            namespace_id,
            generation: Generation::new(0),
            revision: Revision::new(1),
            consumers: BTreeMap::new(),
        }
    }

    /// Borrow Consumer Group ID.
    #[must_use]
    pub const fn group_id(&self) -> &ConsumerGroupId {
        &self.id
    }

    /// Borrow owning namespace ID.
    #[must_use]
    pub const fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Return membership generation.
    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    /// Return group revision.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Return Consumer count.
    #[must_use]
    pub fn consumer_count(&self) -> usize {
        self.consumers.len()
    }

    /// Borrow a logical Consumer by ID using HETERARCHY domain vocabulary.
    #[must_use]
    pub fn consumer(&self, consumer_id: &ConsumerId) -> Option<&Consumer> {
        self.consumers.get(consumer_id)
    }

    /// Iterate logical Consumers in deterministic Consumer-ID order.
    pub fn consumers(&self) -> impl Iterator<Item = &Consumer> {
        self.consumers.values()
    }

    /// Build one read-only management summary without exposing mutable membership internals.
    #[must_use]
    pub fn summary(&self) -> ConsumerGroupSummary {
        ConsumerGroupSummary::from_group(self)
    }

    /// Export this Consumer Group into the logical persistence/replication checkpoint contract.
    #[must_use]
    pub fn checkpoint(&self) -> ConsumerGroupCheckpoint {
        ConsumerGroupCheckpoint {
            group_id: self.id.clone(),
            namespace_id: self.namespace_id.clone(),
            generation: self.generation,
            revision: self.revision,
            members: self.consumers.values().map(Consumer::checkpoint).collect(),
        }
    }

    pub(crate) fn from_checkpoint(
        checkpoint: ConsumerGroupCheckpoint,
    ) -> Result<Self, CheckpointError> {
        let mut consumers = BTreeMap::new();
        for member_checkpoint in checkpoint.members {
            let member_id = member_checkpoint.member_id.clone();
            if consumers
                .insert(
                    member_id.clone(),
                    Consumer::from_checkpoint(member_checkpoint),
                )
                .is_some()
            {
                return Err(CheckpointError::DuplicateMember(member_id));
            }
        }
        Ok(Self {
            id: checkpoint.group_id,
            namespace_id: checkpoint.namespace_id,
            generation: checkpoint.generation,
            revision: checkpoint.revision,
            consumers,
        })
    }

    /// Join a new member or idempotently refresh an existing member with identical capabilities.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerGroupError`] on capability conflict, capacity violation, or counter
    /// overflow.
    pub fn join(
        &mut self,
        member_id: ConsumerId,
        capabilities: Capabilities,
        now_ms: TimestampMs,
        max_members: usize,
    ) -> Result<JoinOutcome, ConsumerGroupError> {
        if let Some(member) = self.consumers.get_mut(&member_id) {
            if member.capabilities != capabilities {
                return Err(ConsumerGroupError::CapabilityConflict(member_id));
            }
            if now_ms <= member.last_heartbeat_at_ms {
                return Ok(JoinOutcome::Unchanged);
            }
            let next_member_revision = member.revision.next()?;
            let next_group_revision = self.revision.next()?;
            member.last_heartbeat_at_ms = now_ms;
            member.revision = next_member_revision;
            self.revision = next_group_revision;
            return Ok(JoinOutcome::Refreshed);
        }

        if max_members == 0 {
            return Err(ConsumerGroupError::InvalidMemberCapacity);
        }
        if self.consumers.len() >= max_members {
            return Err(ConsumerGroupError::MemberCapacityReached { max_members });
        }
        let next_generation = self.generation.next()?;
        let next_group_revision = self.revision.next()?;
        self.consumers.insert(
            member_id.clone(),
            Consumer::new(member_id, capabilities, now_ms),
        );
        self.generation = next_generation;
        self.revision = next_group_revision;
        Ok(JoinOutcome::Added)
    }

    /// Record a heartbeat when generation and member identity are current.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerGroupError`] for stale generation, missing member, or revision overflow.
    pub fn heartbeat(
        &mut self,
        member_id: &ConsumerId,
        expected_generation: Generation,
        now_ms: TimestampMs,
    ) -> Result<HeartbeatOutcome, ConsumerGroupError> {
        self.require_generation(expected_generation)?;
        let member = self
            .consumers
            .get_mut(member_id)
            .ok_or_else(|| ConsumerGroupError::MemberNotFound(member_id.clone()))?;
        if now_ms <= member.last_heartbeat_at_ms {
            return Ok(HeartbeatOutcome::Unchanged);
        }
        let next_member_revision = member.revision.next()?;
        let next_group_revision = self.revision.next()?;
        member.last_heartbeat_at_ms = now_ms;
        member.revision = next_member_revision;
        self.revision = next_group_revision;
        Ok(HeartbeatOutcome::Updated)
    }

    /// Leave the current generation and return the removed member so the state machine can
    /// requeue that member's leased Tasks.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerGroupError`] for stale generation, missing member, or counter overflow.
    pub fn leave(
        &mut self,
        member_id: &ConsumerId,
        expected_generation: Generation,
    ) -> Result<Consumer, ConsumerGroupError> {
        self.require_generation(expected_generation)?;
        if !self.consumers.contains_key(member_id) {
            return Err(ConsumerGroupError::MemberNotFound(member_id.clone()));
        }
        let next_generation = self.generation.next()?;
        let next_revision = self.revision.next()?;
        let Some(member) = self.consumers.remove(member_id) else {
            return Err(ConsumerGroupError::MemberNotFound(member_id.clone()));
        };
        self.generation = next_generation;
        self.revision = next_revision;
        Ok(member)
    }

    /// Remove up to `max_members` members whose heartbeat is at or before `stale_before_ms`.
    ///
    /// Generation/revision advance once per affected group, matching the Python reap command.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerGroupError`] for zero capacity or counter overflow.
    pub fn reap_stale_members(
        &mut self,
        stale_before_ms: TimestampMs,
        max_members: usize,
    ) -> Result<Vec<Consumer>, ConsumerGroupError> {
        if max_members == 0 {
            return Err(ConsumerGroupError::InvalidMemberCapacity);
        }
        let stale_ids = self
            .consumers
            .values()
            .filter(|member| member.last_heartbeat_at_ms <= stale_before_ms)
            .map(|member| (member.last_heartbeat_at_ms, member.id.clone()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .take(max_members)
            .map(|(_, member_id)| member_id)
            .collect::<Vec<_>>();
        if stale_ids.is_empty() {
            return Ok(Vec::new());
        }

        let next_generation = self.generation.next()?;
        let next_revision = self.revision.next()?;
        let mut removed = Vec::with_capacity(stale_ids.len());
        for member_id in stale_ids {
            if let Some(member) = self.consumers.remove(&member_id) {
                removed.push(member);
            }
        }
        self.generation = next_generation;
        self.revision = next_revision;
        Ok(removed)
    }

    pub(crate) fn require_generation(
        &self,
        expected: Generation,
    ) -> Result<(), ConsumerGroupError> {
        if self.generation != expected {
            return Err(ConsumerGroupError::StaleGeneration {
                expected,
                actual: self.generation,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{Capabilities, ConsumerGroup, ConsumerGroupError, HeartbeatOutcome, JoinOutcome};
    use crate::{ConsumerGroupId, ConsumerId, Generation, NamespaceId, TimestampMs};

    fn group() -> Result<ConsumerGroup, Box<dyn Error>> {
        Ok(ConsumerGroup::new(
            ConsumerGroupId::new("engineering")?,
            NamespaceId::new("project-a")?,
        ))
    }

    #[test]
    fn capability_normalization_matches_python_reference() -> Result<(), Box<dyn Error>> {
        let capabilities = Capabilities::new(["review", "code", "review"])?;
        let values = capabilities
            .as_slice()
            .iter()
            .map(super::Capability::as_str)
            .collect::<Vec<_>>();
        assert_eq!(values, ["code", "review"]);
        Ok(())
    }

    #[test]
    fn join_retry_is_idempotent_and_newer_time_only_refreshes_revision()
    -> Result<(), Box<dyn Error>> {
        let mut group = group()?;
        let member_id = ConsumerId::new("worker-a")?;
        let capabilities = Capabilities::new(["code", "review"])?;
        assert_eq!(
            group.join(
                member_id.clone(),
                capabilities.clone(),
                TimestampMs::new(1_000),
                256,
            )?,
            JoinOutcome::Added
        );
        assert_eq!(group.generation().get(), 1);
        assert_eq!(group.revision().get(), 2);

        assert_eq!(
            group.join(
                member_id.clone(),
                capabilities.clone(),
                TimestampMs::new(1_000),
                256,
            )?,
            JoinOutcome::Unchanged
        );
        assert_eq!(group.generation().get(), 1);
        assert_eq!(group.revision().get(), 2);

        assert_eq!(
            group.join(
                member_id.clone(),
                capabilities,
                TimestampMs::new(2_000),
                256,
            )?,
            JoinOutcome::Refreshed
        );
        assert_eq!(group.generation().get(), 1);
        assert_eq!(group.revision().get(), 3);
        assert_eq!(
            group
                .consumer(&member_id)
                .map(|member| member.revision().get()),
            Some(2)
        );
        Ok(())
    }

    #[test]
    fn join_with_different_capabilities_conflicts() -> Result<(), Box<dyn Error>> {
        let mut group = group()?;
        let member_id = ConsumerId::new("worker-a")?;
        group.join(
            member_id.clone(),
            Capabilities::new(["code"])?,
            TimestampMs::new(1_000),
            256,
        )?;
        assert_eq!(
            group.join(
                member_id.clone(),
                Capabilities::new(["review"])?,
                TimestampMs::new(2_000),
                256,
            ),
            Err(ConsumerGroupError::CapabilityConflict(member_id))
        );
        Ok(())
    }

    #[test]
    fn heartbeat_requires_current_generation_and_only_newer_time_mutates()
    -> Result<(), Box<dyn Error>> {
        let mut group = group()?;
        let member_id = ConsumerId::new("worker-a")?;
        group.join(
            member_id.clone(),
            Capabilities::new(["code"])?,
            TimestampMs::new(1_000),
            256,
        )?;
        assert!(matches!(
            group.heartbeat(&member_id, Generation::new(0), TimestampMs::new(2_000)),
            Err(ConsumerGroupError::StaleGeneration { .. })
        ));
        assert_eq!(
            group.heartbeat(&member_id, Generation::new(1), TimestampMs::new(1_000))?,
            HeartbeatOutcome::Unchanged
        );
        assert_eq!(
            group.heartbeat(&member_id, Generation::new(1), TimestampMs::new(2_000))?,
            HeartbeatOutcome::Updated
        );
        assert_eq!(group.revision().get(), 3);
        Ok(())
    }

    #[test]
    fn leave_and_reap_advance_generation_once_per_membership_change() -> Result<(), Box<dyn Error>>
    {
        let mut group = group()?;
        let worker_a = ConsumerId::new("worker-a")?;
        let worker_b = ConsumerId::new("worker-b")?;
        let worker_c = ConsumerId::new("worker-c")?;
        for (member_id, timestamp) in [
            (worker_a.clone(), 1_000),
            (worker_b.clone(), 1_100),
            (worker_c.clone(), 5_000),
        ] {
            group.join(
                member_id,
                Capabilities::new(["code"])?,
                TimestampMs::new(timestamp),
                256,
            )?;
        }
        assert_eq!(group.generation().get(), 3);
        let removed = group.reap_stale_members(TimestampMs::new(2_000), 8)?;
        assert_eq!(removed.len(), 2);
        assert_eq!(group.generation().get(), 4);
        assert_eq!(group.consumer_count(), 1);

        group.leave(&worker_c, Generation::new(4))?;
        assert_eq!(group.generation().get(), 5);
        assert_eq!(group.consumer_count(), 0);
        Ok(())
    }

    #[test]
    fn capacity_is_checked_only_for_new_members() -> Result<(), Box<dyn Error>> {
        let mut group = group()?;
        let member_id = ConsumerId::new("worker-a")?;
        let capabilities = Capabilities::new(["code"])?;
        group.join(
            member_id.clone(),
            capabilities.clone(),
            TimestampMs::new(1_000),
            1,
        )?;
        assert_eq!(
            group.join(member_id, capabilities, TimestampMs::new(1_001), 0,)?,
            JoinOutcome::Refreshed
        );
        assert!(matches!(
            group.join(
                ConsumerId::new("worker-b")?,
                Capabilities::new(["code"])?,
                TimestampMs::new(1_002),
                1,
            ),
            Err(ConsumerGroupError::MemberCapacityReached { max_members: 1 })
        ));
        Ok(())
    }
}

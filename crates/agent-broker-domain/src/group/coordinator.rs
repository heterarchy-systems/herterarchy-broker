use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use super::{Consumer, ConsumerGroup, ConsumerGroupError, HeartbeatOutcome, JoinOutcome};
use crate::{Capabilities, ConsumerGroupId, ConsumerId, Generation, NamespaceId, TimestampMs};

/// Broker-internal control-plane component for all authoritative Consumer Groups.
///
/// `GroupCoordinator` owns no replicated state of its own. The Broker state machine
/// supplies the authoritative group registry for each operation, and the coordinator
/// selects one group by [`ConsumerGroupId`] before applying provider-neutral membership
/// transitions. Cross-entity atomicity such as member removal plus Task lease requeue
/// remains owned by `BrokerStateMachine` around this component.
#[derive(Debug)]
pub(crate) struct GroupCoordinator;

/// Failure while coordinating one Consumer Group from the Broker-wide registry.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum GroupCoordinatorError {
    /// The requested Consumer Group is not registered in the Broker.
    GroupNotFound(ConsumerGroupId),
    /// The selected Consumer Group rejected the membership transition.
    Transition(ConsumerGroupError),
}

impl fmt::Display for GroupCoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupNotFound(group_id) => {
                write!(formatter, "Consumer Group {group_id} does not exist")
            }
            Self::Transition(error) => error.fmt(formatter),
        }
    }
}

impl Error for GroupCoordinatorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transition(error) => Some(error),
            Self::GroupNotFound(_) => None,
        }
    }
}

impl From<ConsumerGroupError> for GroupCoordinatorError {
    fn from(error: ConsumerGroupError) -> Self {
        Self::Transition(error)
    }
}

impl GroupCoordinator {
    pub(crate) fn join(
        groups: &mut BTreeMap<ConsumerGroupId, ConsumerGroup>,
        group_id: &ConsumerGroupId,
        member_id: ConsumerId,
        capabilities: Capabilities,
        now_ms: TimestampMs,
        max_members: usize,
    ) -> Result<JoinOutcome, GroupCoordinatorError> {
        Self::group_mut(groups, group_id)?
            .join(member_id, capabilities, now_ms, max_members)
            .map_err(Into::into)
    }

    pub(crate) fn heartbeat(
        groups: &mut BTreeMap<ConsumerGroupId, ConsumerGroup>,
        group_id: &ConsumerGroupId,
        member_id: &ConsumerId,
        expected_generation: Generation,
        now_ms: TimestampMs,
    ) -> Result<HeartbeatOutcome, GroupCoordinatorError> {
        Self::group_mut(groups, group_id)?
            .heartbeat(member_id, expected_generation, now_ms)
            .map_err(Into::into)
    }

    pub(crate) fn leave(
        groups: &mut BTreeMap<ConsumerGroupId, ConsumerGroup>,
        group_id: &ConsumerGroupId,
        member_id: &ConsumerId,
        expected_generation: Generation,
    ) -> Result<Consumer, GroupCoordinatorError> {
        Self::group_mut(groups, group_id)?
            .leave(member_id, expected_generation)
            .map_err(Into::into)
    }

    pub(crate) fn reap_stale_members(
        groups: &mut BTreeMap<ConsumerGroupId, ConsumerGroup>,
        group_id: &ConsumerGroupId,
        stale_before_ms: TimestampMs,
        max_members: usize,
    ) -> Result<Vec<Consumer>, GroupCoordinatorError> {
        Self::group_mut(groups, group_id)?
            .reap_stale_members(stale_before_ms, max_members)
            .map_err(Into::into)
    }

    pub(crate) fn require_member(
        groups: &BTreeMap<ConsumerGroupId, ConsumerGroup>,
        group_id: &ConsumerGroupId,
        member_id: &ConsumerId,
        expected_generation: Generation,
    ) -> Result<(NamespaceId, Generation), GroupCoordinatorError> {
        let group = groups
            .get(group_id)
            .ok_or_else(|| GroupCoordinatorError::GroupNotFound(group_id.clone()))?;
        group.require_generation(expected_generation)?;
        if group.consumer(member_id).is_none() {
            return Err(ConsumerGroupError::MemberNotFound(member_id.clone()).into());
        }
        Ok((group.namespace_id().clone(), group.generation()))
    }

    /// Return current logical Consumers in one Company whose advertised capabilities satisfy all
    /// required capabilities.
    ///
    /// This is a deterministic, read-only candidate query. It does not assign work, inspect a
    /// provider runtime, or imply that a physical Chat/CLI process is currently runnable.
    pub(crate) fn consumers_matching_capabilities(
        groups: &BTreeMap<ConsumerGroupId, ConsumerGroup>,
        group_id: &ConsumerGroupId,
        required_capabilities: &Capabilities,
    ) -> Result<Vec<ConsumerId>, GroupCoordinatorError> {
        let group = groups
            .get(group_id)
            .ok_or_else(|| GroupCoordinatorError::GroupNotFound(group_id.clone()))?;
        Ok(group
            .consumers()
            .filter(|consumer| consumer.capabilities().contains_all(required_capabilities))
            .map(|consumer| consumer.consumer_id().clone())
            .collect())
    }

    fn group_mut<'a>(
        groups: &'a mut BTreeMap<ConsumerGroupId, ConsumerGroup>,
        group_id: &ConsumerGroupId,
    ) -> Result<&'a mut ConsumerGroup, GroupCoordinatorError> {
        groups
            .get_mut(group_id)
            .ok_or_else(|| GroupCoordinatorError::GroupNotFound(group_id.clone()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::error::Error;

    use super::GroupCoordinator;
    use crate::{
        Capabilities, ConsumerGroup, ConsumerGroupId, ConsumerId, Generation, NamespaceId,
        TimestampMs,
    };

    fn groups() -> Result<BTreeMap<ConsumerGroupId, ConsumerGroup>, Box<dyn Error>> {
        let backend_id = ConsumerGroupId::new("backend-company")?;
        let research_id = ConsumerGroupId::new("research-company")?;
        Ok(BTreeMap::from([
            (
                backend_id.clone(),
                ConsumerGroup::new(backend_id, NamespaceId::new("project")?),
            ),
            (
                research_id.clone(),
                ConsumerGroup::new(research_id, NamespaceId::new("project")?),
            ),
        ]))
    }

    #[test]
    fn one_coordinator_manages_multiple_groups_without_state_leakage() -> Result<(), Box<dyn Error>>
    {
        let mut groups = groups()?;
        let backend_id = ConsumerGroupId::new("backend-company")?;
        let research_id = ConsumerGroupId::new("research-company")?;

        GroupCoordinator::join(
            &mut groups,
            &backend_id,
            ConsumerId::new("coder")?,
            Capabilities::new(["code"])?,
            TimestampMs::new(1_000),
            8,
        )?;
        GroupCoordinator::join(
            &mut groups,
            &research_id,
            ConsumerId::new("researcher")?,
            Capabilities::new(["research"])?,
            TimestampMs::new(1_100),
            8,
        )?;

        let backend = groups.get(&backend_id).ok_or("backend group missing")?;
        let research = groups.get(&research_id).ok_or("research group missing")?;
        assert_eq!(backend.generation(), Generation::new(1));
        assert_eq!(backend.consumer_count(), 1);
        assert_eq!(research.generation(), Generation::new(1));
        assert_eq!(research.consumer_count(), 1);
        assert!(backend.consumer(&ConsumerId::new("researcher")?).is_none());
        assert!(research.consumer(&ConsumerId::new("coder")?).is_none());
        Ok(())
    }

    #[test]
    fn coordinator_requires_current_generation_and_member_in_selected_group()
    -> Result<(), Box<dyn Error>> {
        let mut groups = groups()?;
        let backend_id = ConsumerGroupId::new("backend-company")?;
        let member_id = ConsumerId::new("coder")?;
        GroupCoordinator::join(
            &mut groups,
            &backend_id,
            member_id.clone(),
            Capabilities::new(["code"])?,
            TimestampMs::new(1_000),
            8,
        )?;

        let (namespace_id, generation) =
            GroupCoordinator::require_member(&groups, &backend_id, &member_id, Generation::new(1))?;

        assert_eq!(namespace_id, NamespaceId::new("project")?);
        assert_eq!(generation, Generation::new(1));
        Ok(())
    }

    #[test]
    fn capability_selection_is_deterministic_and_company_scoped() -> Result<(), Box<dyn Error>> {
        let mut groups = groups()?;
        let backend_id = ConsumerGroupId::new("backend-company")?;
        let research_id = ConsumerGroupId::new("research-company")?;

        for (consumer_id, capabilities) in [
            ("rust-coder", ["code", "rust"]),
            ("reviewer", ["review", "rust"]),
            ("writer", ["docs", "writing"]),
        ] {
            GroupCoordinator::join(
                &mut groups,
                &backend_id,
                ConsumerId::new(consumer_id)?,
                Capabilities::new(capabilities)?,
                TimestampMs::new(1_000),
                8,
            )?;
        }
        GroupCoordinator::join(
            &mut groups,
            &research_id,
            ConsumerId::new("researcher")?,
            Capabilities::new(["research", "rust"])?,
            TimestampMs::new(1_000),
            8,
        )?;

        let rust_consumers = GroupCoordinator::consumers_matching_capabilities(
            &groups,
            &backend_id,
            &Capabilities::new(["rust"])?,
        )?;
        assert_eq!(
            rust_consumers,
            vec![ConsumerId::new("reviewer")?, ConsumerId::new("rust-coder")?]
        );

        let code_and_rust = GroupCoordinator::consumers_matching_capabilities(
            &groups,
            &backend_id,
            &Capabilities::new(["code", "rust"])?,
        )?;
        assert_eq!(code_and_rust, vec![ConsumerId::new("rust-coder")?]);

        let missing = GroupCoordinator::consumers_matching_capabilities(
            &groups,
            &backend_id,
            &Capabilities::new(["research"])?,
        )?;
        assert!(missing.is_empty());
        Ok(())
    }
}

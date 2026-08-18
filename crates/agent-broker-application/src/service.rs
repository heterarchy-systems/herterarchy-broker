use agent_broker_domain::commands::{
    AdvanceTermCommand, BrokerCommand, ClaimTaskCommand, CompleteTaskCommand,
    EnsureConsumerGroupCommand, EnsureNamespaceCommand, HeartbeatCommand, JoinConsumerGroupCommand,
    LeaveConsumerGroupCommand, PruneCompletedTasksCommand, PublishTaskCommand,
    ReapStaleMembersCommand, RenewTaskLeaseCommand,
};
use agent_broker_domain::results::{
    BrokerMutationResult, CompletedTasksPrunedResult, ConsumerGroupResult, HeartbeatResult,
    NamespaceResult, StaleMembersReapedResult, TaskClaimResult, TaskCompletedResult,
    TaskLeaseRenewedResult, TaskPublishedResult, TermAdvancedResult,
};
use agent_broker_domain::{
    BrokerCapacityPolicy, Capabilities, ConsumerGroupDirectory, ConsumerGroupId, ConsumerId,
    Generation, LeaseDurationMs, LeaseEpoch, LeaseId, NamespaceId, PruneTaskLimit, ReapMemberLimit,
    Revision, TaskId, TaskObjective, TaskResult, Term, TimestampMs,
};

use crate::{
    BrokerError, BrokerErrorCode, CommandIdentity, CommandSessionId, ConsensusAdapter,
    SessionOwnerEpoch, SessionOwnerInstanceId,
};

/// Lightweight health metadata returned without mutating Broker state.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BrokerHealth {
    /// Current authoritative Broker term.
    pub term: Term,
    /// Current committed Broker revision.
    pub revision: Revision,
    /// Stable application protocol generation; wire encoding is defined in the protocol crate.
    pub protocol_version: u32,
}

/// Typed application input for Task claim.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimTaskInput {
    pub group_id: ConsumerGroupId,
    pub member_id: ConsumerId,
    pub expected_term: Term,
    pub expected_generation: Generation,
    pub lease_id: LeaseId,
    pub now_ms: TimestampMs,
    pub lease_duration: LeaseDurationMs,
}

/// Typed application input for Task lease renewal.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RenewTaskLeaseInput {
    pub task_id: TaskId,
    pub group_id: ConsumerGroupId,
    pub member_id: ConsumerId,
    pub expected_term: Term,
    pub expected_generation: Generation,
    pub expected_lease_epoch: LeaseEpoch,
    pub lease_id: LeaseId,
    pub now_ms: TimestampMs,
    pub lease_duration: LeaseDurationMs,
}

/// Typed application input for Task completion.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompleteTaskInput {
    pub task_id: TaskId,
    pub group_id: ConsumerGroupId,
    pub member_id: ConsumerId,
    pub expected_term: Term,
    pub expected_generation: Generation,
    pub expected_lease_epoch: LeaseEpoch,
    pub lease_id: LeaseId,
    pub result: TaskResult,
    pub completed_at_ms: TimestampMs,
}

/// Typed application use-case boundary above consensus and the deterministic state machine.
pub struct BrokerApplicationService<C> {
    consensus: C,
    capacity: BrokerCapacityPolicy,
}

impl<C> BrokerApplicationService<C>
where
    C: ConsensusAdapter,
{
    /// Construct the application service with an explicit capacity policy.
    #[must_use]
    pub const fn new(consensus: C, capacity: BrokerCapacityPolicy) -> Self {
        Self {
            consensus,
            capacity,
        }
    }

    /// Borrow the current health metadata without a consensus proposal.
    #[must_use]
    pub fn health(&self) -> BrokerHealth {
        BrokerHealth {
            term: self.consensus.term(),
            revision: self.consensus.revision(),
            protocol_version: 1,
        }
    }

    /// Return a side-effect-free Consumer Group directory from the consensus authority.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when the current topology cannot establish read authority.
    pub fn group_directory(&mut self) -> Result<ConsumerGroupDirectory, BrokerError> {
        self.consensus.group_directory()
    }

    /// Return whether this process currently owns authority to initiate bounded maintenance.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when consensus cannot determine current maintenance authority.
    pub fn maintenance_authority(&mut self) -> Result<bool, BrokerError> {
        self.consensus.maintenance_authority()
    }

    /// Submit one validated domain mutation with a durable command identity.
    ///
    /// Legacy protocol-v1 use cases continue to call the operation-specific methods below. This
    /// boundary exists for newer protocol generations that can safely retry an ambiguous
    /// post-submit consensus response with the same session/sequence identity.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] for application or consensus rejection. In replicated mode a
    /// post-submit response deadline may return `COMMIT_OUTCOME_UNKNOWN`; callers must retry only
    /// with the exact same identity and command content.
    pub fn propose_identified(
        &mut self,
        identity: CommandIdentity,
        command: BrokerCommand,
    ) -> Result<BrokerMutationResult, BrokerError> {
        self.consensus.propose_identified(identity, command)
    }

    /// Acquire command-session ownership through the authoritative consensus path.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when a new session is not bootstrapped at epoch 1, the expected epoch
    /// is stale, capacity is exhausted, the owner-instance conflicts, or consensus/persistence
    /// rejects the acquisition.
    pub fn acquire_command_session_owner(
        &mut self,
        session_id: CommandSessionId,
        expected_owner_epoch: SessionOwnerEpoch,
        owner_instance_id: SessionOwnerInstanceId,
    ) -> Result<SessionOwnerEpoch, BrokerError> {
        self.consensus.acquire_command_session_owner(
            session_id,
            expected_owner_epoch,
            owner_instance_id,
        )
    }

    /// Identified variant of [`Self::ensure_namespace`].
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] under the same conditions as [`Self::ensure_namespace`], including
    /// `COMMIT_OUTCOME_UNKNOWN` after a post-submit response deadline.
    pub fn ensure_namespace_identified(
        &mut self,
        identity: CommandIdentity,
        namespace_id: NamespaceId,
    ) -> Result<NamespaceResult, BrokerError> {
        expect_namespace(self.propose_identified(
            identity,
            BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
                namespace_id,
                max_namespaces: self.capacity.max_namespaces(),
            }),
        )?)
    }

    /// Identified variant of [`Self::publish_task`].
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] under the same conditions as [`Self::publish_task`].
    pub fn publish_task_identified(
        &mut self,
        identity: CommandIdentity,
        namespace_id: NamespaceId,
        task_id: TaskId,
        objective: TaskObjective,
        created_at_ms: TimestampMs,
    ) -> Result<TaskPublishedResult, BrokerError> {
        expect_task_published(self.propose_identified(
            identity,
            BrokerCommand::PublishTask(PublishTaskCommand {
                namespace_id,
                task_id,
                objective,
                created_at_ms,
                max_namespace_tasks: self.capacity.max_tasks_per_namespace(),
            }),
        )?)
    }

    /// Identified variant of [`Self::ensure_consumer_group`].
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] under the same conditions as [`Self::ensure_consumer_group`].
    pub fn ensure_consumer_group_identified(
        &mut self,
        identity: CommandIdentity,
        namespace_id: NamespaceId,
        group_id: ConsumerGroupId,
    ) -> Result<ConsumerGroupResult, BrokerError> {
        expect_consumer_group(self.propose_identified(
            identity,
            BrokerCommand::EnsureConsumerGroup(EnsureConsumerGroupCommand {
                namespace_id,
                group_id,
                max_namespace_groups: self.capacity.max_groups_per_namespace(),
            }),
        )?)
    }

    /// Identified variant of [`Self::join_consumer_group`].
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] under the same conditions as [`Self::join_consumer_group`].
    pub fn join_consumer_group_identified(
        &mut self,
        identity: CommandIdentity,
        group_id: ConsumerGroupId,
        member_id: ConsumerId,
        capabilities: Capabilities,
        now_ms: TimestampMs,
    ) -> Result<ConsumerGroupResult, BrokerError> {
        expect_consumer_group(self.propose_identified(
            identity,
            BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
                group_id,
                member_id,
                capabilities,
                now_ms,
                max_group_members: self.capacity.max_members_per_group(),
            }),
        )?)
    }

    /// Identified variant of [`Self::heartbeat`].
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] under the same conditions as [`Self::heartbeat`].
    pub fn heartbeat_identified(
        &mut self,
        identity: CommandIdentity,
        group_id: ConsumerGroupId,
        member_id: ConsumerId,
        expected_generation: Generation,
        now_ms: TimestampMs,
    ) -> Result<HeartbeatResult, BrokerError> {
        expect_heartbeat(self.propose_identified(
            identity,
            BrokerCommand::Heartbeat(HeartbeatCommand {
                group_id,
                member_id,
                expected_generation,
                now_ms,
            }),
        )?)
    }

    /// Identified variant of [`Self::leave_consumer_group`].
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] under the same conditions as [`Self::leave_consumer_group`].
    pub fn leave_consumer_group_identified(
        &mut self,
        identity: CommandIdentity,
        group_id: ConsumerGroupId,
        member_id: ConsumerId,
        expected_generation: Generation,
    ) -> Result<ConsumerGroupResult, BrokerError> {
        expect_consumer_group(self.propose_identified(
            identity,
            BrokerCommand::LeaveConsumerGroup(LeaveConsumerGroupCommand {
                group_id,
                member_id,
                expected_generation,
            }),
        )?)
    }

    /// Identified variant of [`Self::claim_task`].
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] under the same conditions as [`Self::claim_task`].
    pub fn claim_task_identified(
        &mut self,
        identity: CommandIdentity,
        input: ClaimTaskInput,
    ) -> Result<TaskClaimResult, BrokerError> {
        expect_task_claim(self.propose_identified(
            identity,
            BrokerCommand::ClaimTask(ClaimTaskCommand {
                group_id: input.group_id,
                member_id: input.member_id,
                expected_term: input.expected_term,
                expected_generation: input.expected_generation,
                lease_id: input.lease_id,
                now_ms: input.now_ms,
                lease_duration_ms: input.lease_duration.get(),
            }),
        )?)
    }

    /// Identified variant of [`Self::renew_task_lease`].
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] under the same conditions as [`Self::renew_task_lease`].
    pub fn renew_task_lease_identified(
        &mut self,
        identity: CommandIdentity,
        input: RenewTaskLeaseInput,
    ) -> Result<TaskLeaseRenewedResult, BrokerError> {
        expect_task_lease_renewed(self.propose_identified(
            identity,
            BrokerCommand::RenewTaskLease(RenewTaskLeaseCommand {
                task_id: input.task_id,
                group_id: input.group_id,
                member_id: input.member_id,
                expected_term: input.expected_term,
                expected_generation: input.expected_generation,
                expected_lease_epoch: input.expected_lease_epoch,
                lease_id: input.lease_id,
                now_ms: input.now_ms,
                lease_duration_ms: input.lease_duration.get(),
            }),
        )?)
    }

    /// Identified variant of [`Self::complete_task`].
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] under the same conditions as [`Self::complete_task`].
    pub fn complete_task_identified(
        &mut self,
        identity: CommandIdentity,
        input: CompleteTaskInput,
    ) -> Result<TaskCompletedResult, BrokerError> {
        expect_task_completed(self.propose_identified(
            identity,
            BrokerCommand::CompleteTask(CompleteTaskCommand {
                task_id: input.task_id,
                group_id: input.group_id,
                member_id: input.member_id,
                expected_term: input.expected_term,
                expected_generation: input.expected_generation,
                expected_lease_epoch: input.expected_lease_epoch,
                lease_id: input.lease_id,
                result: input.result,
                completed_at_ms: input.completed_at_ms,
            }),
        )?)
    }

    /// Return the underlying consensus adapter, for runtime composition/shutdown only.
    #[must_use]
    pub fn into_consensus(self) -> C {
        self.consensus
    }

    /// Idempotently ensure a namespace using the configured hot-state capacity.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when consensus or state-machine validation rejects the proposal.
    pub fn ensure_namespace(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<NamespaceResult, BrokerError> {
        expect_namespace(self.consensus.propose(BrokerCommand::EnsureNamespace(
            EnsureNamespaceCommand {
                namespace_id,
                max_namespaces: self.capacity.max_namespaces(),
            },
        ))?)
    }

    /// Idempotently publish a Task using configured per-namespace capacity.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when consensus or state-machine validation rejects the proposal.
    pub fn publish_task(
        &mut self,
        namespace_id: NamespaceId,
        task_id: TaskId,
        objective: TaskObjective,
        created_at_ms: TimestampMs,
    ) -> Result<TaskPublishedResult, BrokerError> {
        expect_task_published(self.consensus.propose(BrokerCommand::PublishTask(
            PublishTaskCommand {
                namespace_id,
                task_id,
                objective,
                created_at_ms,
                max_namespace_tasks: self.capacity.max_tasks_per_namespace(),
            },
        ))?)
    }

    /// Idempotently ensure a Consumer Group using configured per-namespace capacity.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when consensus or state-machine validation rejects the proposal.
    pub fn ensure_consumer_group(
        &mut self,
        namespace_id: NamespaceId,
        group_id: ConsumerGroupId,
    ) -> Result<ConsumerGroupResult, BrokerError> {
        expect_consumer_group(self.consensus.propose(BrokerCommand::EnsureConsumerGroup(
            EnsureConsumerGroupCommand {
                namespace_id,
                group_id,
                max_namespace_groups: self.capacity.max_groups_per_namespace(),
            },
        ))?)
    }

    /// Join or idempotently refresh one Consumer Group member.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] for missing groups, capability conflicts, capacity, or consensus
    /// failures.
    pub fn join_consumer_group(
        &mut self,
        group_id: ConsumerGroupId,
        member_id: ConsumerId,
        capabilities: Capabilities,
        now_ms: TimestampMs,
    ) -> Result<ConsumerGroupResult, BrokerError> {
        expect_consumer_group(self.consensus.propose(BrokerCommand::JoinConsumerGroup(
            JoinConsumerGroupCommand {
                group_id,
                member_id,
                capabilities,
                now_ms,
                max_group_members: self.capacity.max_members_per_group(),
            },
        ))?)
    }

    /// Record a member heartbeat against the current Consumer Group generation.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] for stale generations, missing members/groups, or consensus errors.
    pub fn heartbeat(
        &mut self,
        group_id: ConsumerGroupId,
        member_id: ConsumerId,
        expected_generation: Generation,
        now_ms: TimestampMs,
    ) -> Result<HeartbeatResult, BrokerError> {
        expect_heartbeat(
            self.consensus
                .propose(BrokerCommand::Heartbeat(HeartbeatCommand {
                    group_id,
                    member_id,
                    expected_generation,
                    now_ms,
                }))?,
        )
    }

    /// Leave a member from the current Consumer Group generation.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] for stale generations, missing members/groups, or consensus errors.
    pub fn leave_consumer_group(
        &mut self,
        group_id: ConsumerGroupId,
        member_id: ConsumerId,
        expected_generation: Generation,
    ) -> Result<ConsumerGroupResult, BrokerError> {
        expect_consumer_group(self.consensus.propose(BrokerCommand::LeaveConsumerGroup(
            LeaveConsumerGroupCommand {
                group_id,
                member_id,
                expected_generation,
            },
        ))?)
    }

    /// Reap a bounded number of globally stale members.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when consensus or state-machine invariants reject the proposal.
    pub fn reap_stale_members(
        &mut self,
        stale_before_ms: TimestampMs,
        max_members: ReapMemberLimit,
    ) -> Result<StaleMembersReapedResult, BrokerError> {
        expect_stale_members_reaped(self.consensus.propose(BrokerCommand::ReapStaleMembers(
            ReapStaleMembersCommand {
                stale_before_ms,
                max_members: max_members.get(),
            },
        ))?)
    }

    /// Claim the oldest ready Task in the member's Consumer Group namespace.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] for stale fencing, conflicts, missing membership, or consensus
    /// failures.
    pub fn claim_task(&mut self, input: ClaimTaskInput) -> Result<TaskClaimResult, BrokerError> {
        expect_task_claim(
            self.consensus
                .propose(BrokerCommand::ClaimTask(ClaimTaskCommand {
                    group_id: input.group_id,
                    member_id: input.member_id,
                    expected_term: input.expected_term,
                    expected_generation: input.expected_generation,
                    lease_id: input.lease_id,
                    now_ms: input.now_ms,
                    lease_duration_ms: input.lease_duration.get(),
                }))?,
        )
    }

    /// Renew one matching active Task lease.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] for stale fencing, missing resources, or consensus failures.
    pub fn renew_task_lease(
        &mut self,
        input: RenewTaskLeaseInput,
    ) -> Result<TaskLeaseRenewedResult, BrokerError> {
        expect_task_lease_renewed(self.consensus.propose(BrokerCommand::RenewTaskLease(
            RenewTaskLeaseCommand {
                task_id: input.task_id,
                group_id: input.group_id,
                member_id: input.member_id,
                expected_term: input.expected_term,
                expected_generation: input.expected_generation,
                expected_lease_epoch: input.expected_lease_epoch,
                lease_id: input.lease_id,
                now_ms: input.now_ms,
                lease_duration_ms: input.lease_duration.get(),
            },
        ))?)
    }

    /// Complete one matching active Task lease.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] for stale fencing, conflicts, missing resources, or consensus
    /// failures.
    pub fn complete_task(
        &mut self,
        input: CompleteTaskInput,
    ) -> Result<TaskCompletedResult, BrokerError> {
        expect_task_completed(self.consensus.propose(BrokerCommand::CompleteTask(
            CompleteTaskCommand {
                task_id: input.task_id,
                group_id: input.group_id,
                member_id: input.member_id,
                expected_term: input.expected_term,
                expected_generation: input.expected_generation,
                expected_lease_epoch: input.expected_lease_epoch,
                lease_id: input.lease_id,
                result: input.result,
                completed_at_ms: input.completed_at_ms,
            },
        ))?)
    }

    /// Prune a bounded number of retained completed Tasks.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when consensus or state-machine invariants reject the proposal.
    pub fn prune_completed_tasks(
        &mut self,
        completed_before_ms: TimestampMs,
        max_tasks: PruneTaskLimit,
    ) -> Result<CompletedTasksPrunedResult, BrokerError> {
        expect_completed_tasks_pruned(self.consensus.propose(
            BrokerCommand::PruneCompletedTasks(PruneCompletedTasksCommand {
                completed_before_ms,
                max_tasks: max_tasks.get(),
            }),
        )?)
    }

    /// Advance the Broker term through the consensus authority boundary.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when the expected term is stale, the new term is not increasing, or
    /// consensus rejects the proposal.
    pub fn advance_term(
        &mut self,
        expected_term: Term,
        new_term: Term,
    ) -> Result<TermAdvancedResult, BrokerError> {
        expect_term_advanced(self.consensus.propose(BrokerCommand::AdvanceTerm(
            AdvanceTermCommand {
                expected_term,
                new_term,
            },
        ))?)
    }
}

fn unexpected_result(expected: &'static str) -> BrokerError {
    BrokerError::new(
        BrokerErrorCode::InternalError,
        format!("consensus returned an unexpected result variant; expected {expected}"),
    )
}

macro_rules! expect_result {
    ($name:ident, $variant:ident, $output:ty, $expected:literal) => {
        fn $name(result: BrokerMutationResult) -> Result<$output, BrokerError> {
            let BrokerMutationResult::$variant(result) = result else {
                return Err(unexpected_result($expected));
            };
            Ok(result)
        }
    };
}

expect_result!(expect_namespace, Namespace, NamespaceResult, "Namespace");
expect_result!(
    expect_task_published,
    TaskPublished,
    TaskPublishedResult,
    "TaskPublished"
);
expect_result!(
    expect_consumer_group,
    ConsumerGroup,
    ConsumerGroupResult,
    "ConsumerGroup"
);
expect_result!(expect_heartbeat, Heartbeat, HeartbeatResult, "Heartbeat");
expect_result!(
    expect_stale_members_reaped,
    StaleMembersReaped,
    StaleMembersReapedResult,
    "StaleMembersReaped"
);
expect_result!(expect_task_claim, TaskClaim, TaskClaimResult, "TaskClaim");
expect_result!(
    expect_task_lease_renewed,
    TaskLeaseRenewed,
    TaskLeaseRenewedResult,
    "TaskLeaseRenewed"
);
expect_result!(
    expect_task_completed,
    TaskCompleted,
    TaskCompletedResult,
    "TaskCompleted"
);
expect_result!(
    expect_completed_tasks_pruned,
    CompletedTasksPruned,
    CompletedTasksPrunedResult,
    "CompletedTasksPruned"
);
expect_result!(
    expect_term_advanced,
    TermAdvanced,
    TermAdvancedResult,
    "TermAdvanced"
);

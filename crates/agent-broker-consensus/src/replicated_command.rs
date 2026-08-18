use std::error::Error;
use std::fmt;

use agent_broker_domain::commands::{
    AdvanceTermCommand, BrokerCommand, ClaimTaskCommand, CompleteTaskCommand,
    EnsureConsumerGroupCommand, EnsureNamespaceCommand, HeartbeatCommand, JoinConsumerGroupCommand,
    LeaveConsumerGroupCommand, PruneCompletedTasksCommand, PublishTaskCommand,
    ReapStaleMembersCommand, RenewTaskLeaseCommand,
};
use agent_broker_domain::{
    Capabilities, ConsumerGroupId, ConsumerId, Generation, LeaseEpoch, LeaseId, NamespaceId,
    TaskId, TaskObjective, TaskResult, Term, TimestampMs,
};
use serde::{Deserialize, Serialize};

/// Versioned, consensus-owned durable representation of one Broker command.
///
/// This type intentionally stores only primitive serialized values. Conversion back into the
/// domain re-runs every validated constructor so persisted Raft data cannot bypass Broker
/// invariants.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ReplicatedBrokerCommandV1 {
    EnsureNamespace {
        namespace_id: String,
        max_namespaces: u64,
    },
    PublishTask {
        namespace_id: String,
        task_id: String,
        objective: String,
        created_at_ms: u64,
        max_namespace_tasks: u64,
    },
    EnsureConsumerGroup {
        namespace_id: String,
        group_id: String,
        max_namespace_groups: u64,
    },
    JoinConsumerGroup {
        group_id: String,
        member_id: String,
        capabilities: Vec<String>,
        now_ms: u64,
        max_group_members: u64,
    },
    Heartbeat {
        group_id: String,
        member_id: String,
        expected_generation: u64,
        now_ms: u64,
    },
    LeaveConsumerGroup {
        group_id: String,
        member_id: String,
        expected_generation: u64,
    },
    ReapStaleMembers {
        stale_before_ms: u64,
        max_members: u64,
    },
    ClaimTask {
        group_id: String,
        member_id: String,
        expected_term: u64,
        expected_generation: u64,
        lease_id: String,
        now_ms: u64,
        lease_duration_ms: u64,
    },
    RenewTaskLease {
        task_id: String,
        group_id: String,
        member_id: String,
        expected_term: u64,
        expected_generation: u64,
        expected_lease_epoch: u64,
        lease_id: String,
        now_ms: u64,
        lease_duration_ms: u64,
    },
    CompleteTask {
        task_id: String,
        group_id: String,
        member_id: String,
        expected_term: u64,
        expected_generation: u64,
        expected_lease_epoch: u64,
        lease_id: String,
        result: String,
        completed_at_ms: u64,
    },
    PruneCompletedTasks {
        completed_before_ms: u64,
        max_tasks: u64,
    },
    AdvanceTerm {
        expected_term: u64,
        new_term: u64,
    },
    AcquireCommandSessionOwner {
        session_id: String,
        expected_owner_epoch: u64,
        owner_instance_id: String,
    },
}

impl ReplicatedBrokerCommandV1 {
    /// Compare only caller-stable semantic request content for identified-command retry matching.
    ///
    /// Server-observed timestamps and server-local capacity limits are intentionally excluded: the
    /// first committed command keeps those authoritative values, while a later exact wire retry may
    /// be observed at another time or after a policy reload without becoming a different logical
    /// client command.
    #[must_use]
    pub fn same_identified_request(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::EnsureNamespace { namespace_id, .. },
                Self::EnsureNamespace {
                    namespace_id: other_namespace_id,
                    ..
                },
            ) => namespace_id == other_namespace_id,
            (
                Self::PublishTask {
                    namespace_id,
                    task_id,
                    objective,
                    ..
                },
                Self::PublishTask {
                    namespace_id: other_namespace_id,
                    task_id: other_task_id,
                    objective: other_objective,
                    ..
                },
            ) => {
                namespace_id == other_namespace_id
                    && task_id == other_task_id
                    && objective == other_objective
            }
            (
                Self::EnsureConsumerGroup {
                    namespace_id,
                    group_id,
                    ..
                },
                Self::EnsureConsumerGroup {
                    namespace_id: other_namespace_id,
                    group_id: other_group_id,
                    ..
                },
            ) => namespace_id == other_namespace_id && group_id == other_group_id,
            (
                Self::JoinConsumerGroup {
                    group_id,
                    member_id,
                    capabilities,
                    ..
                },
                Self::JoinConsumerGroup {
                    group_id: other_group_id,
                    member_id: other_member_id,
                    capabilities: other_capabilities,
                    ..
                },
            ) => {
                group_id == other_group_id
                    && member_id == other_member_id
                    && capabilities == other_capabilities
            }
            (
                Self::Heartbeat {
                    group_id,
                    member_id,
                    expected_generation,
                    ..
                },
                Self::Heartbeat {
                    group_id: other_group_id,
                    member_id: other_member_id,
                    expected_generation: other_expected_generation,
                    ..
                },
            ) => {
                group_id == other_group_id
                    && member_id == other_member_id
                    && expected_generation == other_expected_generation
            }
            (Self::LeaveConsumerGroup { .. }, Self::LeaveConsumerGroup { .. })
            | (Self::ReapStaleMembers { .. }, Self::ReapStaleMembers { .. })
            | (Self::PruneCompletedTasks { .. }, Self::PruneCompletedTasks { .. })
            | (Self::AdvanceTerm { .. }, Self::AdvanceTerm { .. })
            | (Self::AcquireCommandSessionOwner { .. }, Self::AcquireCommandSessionOwner { .. }) => {
                self == other
            }
            (Self::ClaimTask { .. }, Self::ClaimTask { .. }) => same_claim_request(self, other),
            (Self::RenewTaskLease { .. }, Self::RenewTaskLease { .. }) => {
                same_renew_request(self, other)
            }
            (Self::CompleteTask { .. }, Self::CompleteTask { .. }) => {
                same_complete_request(self, other)
            }
            _ => false,
        }
    }
}

fn same_claim_request(left: &ReplicatedBrokerCommandV1, right: &ReplicatedBrokerCommandV1) -> bool {
    let (
        ReplicatedBrokerCommandV1::ClaimTask {
            group_id,
            member_id,
            expected_term,
            expected_generation,
            lease_id,
            lease_duration_ms,
            ..
        },
        ReplicatedBrokerCommandV1::ClaimTask {
            group_id: other_group_id,
            member_id: other_member_id,
            expected_term: other_expected_term,
            expected_generation: other_expected_generation,
            lease_id: other_lease_id,
            lease_duration_ms: other_lease_duration_ms,
            ..
        },
    ) = (left, right)
    else {
        return false;
    };
    group_id == other_group_id
        && member_id == other_member_id
        && expected_term == other_expected_term
        && expected_generation == other_expected_generation
        && lease_id == other_lease_id
        && lease_duration_ms == other_lease_duration_ms
}

fn same_renew_request(left: &ReplicatedBrokerCommandV1, right: &ReplicatedBrokerCommandV1) -> bool {
    let (
        ReplicatedBrokerCommandV1::RenewTaskLease {
            task_id,
            group_id,
            member_id,
            expected_term,
            expected_generation,
            expected_lease_epoch,
            lease_id,
            lease_duration_ms,
            ..
        },
        ReplicatedBrokerCommandV1::RenewTaskLease {
            task_id: other_task_id,
            group_id: other_group_id,
            member_id: other_member_id,
            expected_term: other_expected_term,
            expected_generation: other_expected_generation,
            expected_lease_epoch: other_expected_lease_epoch,
            lease_id: other_lease_id,
            lease_duration_ms: other_lease_duration_ms,
            ..
        },
    ) = (left, right)
    else {
        return false;
    };
    task_id == other_task_id
        && group_id == other_group_id
        && member_id == other_member_id
        && expected_term == other_expected_term
        && expected_generation == other_expected_generation
        && expected_lease_epoch == other_expected_lease_epoch
        && lease_id == other_lease_id
        && lease_duration_ms == other_lease_duration_ms
}

fn same_complete_request(
    left: &ReplicatedBrokerCommandV1,
    right: &ReplicatedBrokerCommandV1,
) -> bool {
    let (
        ReplicatedBrokerCommandV1::CompleteTask {
            task_id,
            group_id,
            member_id,
            expected_term,
            expected_generation,
            expected_lease_epoch,
            lease_id,
            result,
            ..
        },
        ReplicatedBrokerCommandV1::CompleteTask {
            task_id: other_task_id,
            group_id: other_group_id,
            member_id: other_member_id,
            expected_term: other_expected_term,
            expected_generation: other_expected_generation,
            expected_lease_epoch: other_expected_lease_epoch,
            lease_id: other_lease_id,
            result: other_result,
            ..
        },
    ) = (left, right)
    else {
        return false;
    };
    task_id == other_task_id
        && group_id == other_group_id
        && member_id == other_member_id
        && expected_term == other_expected_term
        && expected_generation == other_expected_generation
        && expected_lease_epoch == other_expected_lease_epoch
        && lease_id == other_lease_id
        && result == other_result
}

impl TryFrom<BrokerCommand> for ReplicatedBrokerCommandV1 {
    type Error = ReplicatedCommandError;

    fn try_from(command: BrokerCommand) -> Result<Self, Self::Error> {
        match command {
            BrokerCommand::EnsureNamespace(command) => Ok(Self::EnsureNamespace {
                namespace_id: command.namespace_id.as_str().to_owned(),
                max_namespaces: usize_to_u64(command.max_namespaces, "max_namespaces")?,
            }),
            BrokerCommand::PublishTask(command) => Ok(Self::PublishTask {
                namespace_id: command.namespace_id.as_str().to_owned(),
                task_id: command.task_id.as_str().to_owned(),
                objective: command.objective.as_str().to_owned(),
                created_at_ms: command.created_at_ms.get(),
                max_namespace_tasks: usize_to_u64(
                    command.max_namespace_tasks,
                    "max_namespace_tasks",
                )?,
            }),
            BrokerCommand::EnsureConsumerGroup(command) => Ok(Self::EnsureConsumerGroup {
                namespace_id: command.namespace_id.as_str().to_owned(),
                group_id: command.group_id.as_str().to_owned(),
                max_namespace_groups: usize_to_u64(
                    command.max_namespace_groups,
                    "max_namespace_groups",
                )?,
            }),
            BrokerCommand::JoinConsumerGroup(command) => Ok(Self::JoinConsumerGroup {
                group_id: command.group_id.as_str().to_owned(),
                member_id: command.member_id.as_str().to_owned(),
                capabilities: command
                    .capabilities
                    .as_slice()
                    .iter()
                    .map(|capability| capability.as_str().to_owned())
                    .collect(),
                now_ms: command.now_ms.get(),
                max_group_members: usize_to_u64(command.max_group_members, "max_group_members")?,
            }),
            BrokerCommand::Heartbeat(command) => Ok(Self::Heartbeat {
                group_id: command.group_id.as_str().to_owned(),
                member_id: command.member_id.as_str().to_owned(),
                expected_generation: command.expected_generation.get(),
                now_ms: command.now_ms.get(),
            }),
            BrokerCommand::LeaveConsumerGroup(command) => Ok(Self::LeaveConsumerGroup {
                group_id: command.group_id.as_str().to_owned(),
                member_id: command.member_id.as_str().to_owned(),
                expected_generation: command.expected_generation.get(),
            }),
            BrokerCommand::ReapStaleMembers(command) => Ok(Self::ReapStaleMembers {
                stale_before_ms: command.stale_before_ms.get(),
                max_members: usize_to_u64(command.max_members, "max_members")?,
            }),
            BrokerCommand::ClaimTask(command) => Ok(Self::ClaimTask {
                group_id: command.group_id.as_str().to_owned(),
                member_id: command.member_id.as_str().to_owned(),
                expected_term: command.expected_term.get(),
                expected_generation: command.expected_generation.get(),
                lease_id: command.lease_id.as_str().to_owned(),
                now_ms: command.now_ms.get(),
                lease_duration_ms: command.lease_duration_ms,
            }),
            BrokerCommand::RenewTaskLease(command) => Ok(Self::RenewTaskLease {
                task_id: command.task_id.as_str().to_owned(),
                group_id: command.group_id.as_str().to_owned(),
                member_id: command.member_id.as_str().to_owned(),
                expected_term: command.expected_term.get(),
                expected_generation: command.expected_generation.get(),
                expected_lease_epoch: command.expected_lease_epoch.get(),
                lease_id: command.lease_id.as_str().to_owned(),
                now_ms: command.now_ms.get(),
                lease_duration_ms: command.lease_duration_ms,
            }),
            BrokerCommand::CompleteTask(command) => Ok(Self::CompleteTask {
                task_id: command.task_id.as_str().to_owned(),
                group_id: command.group_id.as_str().to_owned(),
                member_id: command.member_id.as_str().to_owned(),
                expected_term: command.expected_term.get(),
                expected_generation: command.expected_generation.get(),
                expected_lease_epoch: command.expected_lease_epoch.get(),
                lease_id: command.lease_id.as_str().to_owned(),
                result: command.result.as_str().to_owned(),
                completed_at_ms: command.completed_at_ms.get(),
            }),
            BrokerCommand::PruneCompletedTasks(command) => Ok(Self::PruneCompletedTasks {
                completed_before_ms: command.completed_before_ms.get(),
                max_tasks: usize_to_u64(command.max_tasks, "max_tasks")?,
            }),
            BrokerCommand::AdvanceTerm(command) => Ok(Self::AdvanceTerm {
                expected_term: command.expected_term.get(),
                new_term: command.new_term.get(),
            }),
        }
    }
}

impl TryFrom<ReplicatedBrokerCommandV1> for BrokerCommand {
    type Error = ReplicatedCommandError;

    fn try_from(command: ReplicatedBrokerCommandV1) -> Result<Self, Self::Error> {
        match command {
            command @ (ReplicatedBrokerCommandV1::EnsureNamespace { .. }
            | ReplicatedBrokerCommandV1::PublishTask { .. }
            | ReplicatedBrokerCommandV1::EnsureConsumerGroup { .. }
            | ReplicatedBrokerCommandV1::JoinConsumerGroup { .. }
            | ReplicatedBrokerCommandV1::Heartbeat { .. }
            | ReplicatedBrokerCommandV1::LeaveConsumerGroup { .. }
            | ReplicatedBrokerCommandV1::ReapStaleMembers { .. }) => {
                convert_coordination_command(command)
            }
            ReplicatedBrokerCommandV1::ClaimTask {
                group_id,
                member_id,
                expected_term,
                expected_generation,
                lease_id,
                now_ms,
                lease_duration_ms,
            } => Ok(Self::ClaimTask(ClaimTaskCommand {
                group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
                member_id: ConsumerId::new(member_id).map_err(validation_error)?,
                expected_term: Term::new(expected_term).map_err(validation_error)?,
                expected_generation: Generation::new(expected_generation),
                lease_id: LeaseId::new(lease_id).map_err(validation_error)?,
                now_ms: TimestampMs::new(now_ms),
                lease_duration_ms,
            })),
            ReplicatedBrokerCommandV1::RenewTaskLease {
                task_id,
                group_id,
                member_id,
                expected_term,
                expected_generation,
                expected_lease_epoch,
                lease_id,
                now_ms,
                lease_duration_ms,
            } => Ok(Self::RenewTaskLease(RenewTaskLeaseCommand {
                task_id: TaskId::new(task_id).map_err(validation_error)?,
                group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
                member_id: ConsumerId::new(member_id).map_err(validation_error)?,
                expected_term: Term::new(expected_term).map_err(validation_error)?,
                expected_generation: Generation::new(expected_generation),
                expected_lease_epoch: LeaseEpoch::new(expected_lease_epoch),
                lease_id: LeaseId::new(lease_id).map_err(validation_error)?,
                now_ms: TimestampMs::new(now_ms),
                lease_duration_ms,
            })),
            ReplicatedBrokerCommandV1::CompleteTask {
                task_id,
                group_id,
                member_id,
                expected_term,
                expected_generation,
                expected_lease_epoch,
                lease_id,
                result,
                completed_at_ms,
            } => Ok(Self::CompleteTask(CompleteTaskCommand {
                task_id: TaskId::new(task_id).map_err(validation_error)?,
                group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
                member_id: ConsumerId::new(member_id).map_err(validation_error)?,
                expected_term: Term::new(expected_term).map_err(validation_error)?,
                expected_generation: Generation::new(expected_generation),
                expected_lease_epoch: LeaseEpoch::new(expected_lease_epoch),
                lease_id: LeaseId::new(lease_id).map_err(validation_error)?,
                result: TaskResult::new(result).map_err(validation_error)?,
                completed_at_ms: TimestampMs::new(completed_at_ms),
            })),
            ReplicatedBrokerCommandV1::PruneCompletedTasks {
                completed_before_ms,
                max_tasks,
            } => Ok(Self::PruneCompletedTasks(PruneCompletedTasksCommand {
                completed_before_ms: TimestampMs::new(completed_before_ms),
                max_tasks: u64_to_usize(max_tasks, "max_tasks")?,
            })),
            ReplicatedBrokerCommandV1::AdvanceTerm {
                expected_term,
                new_term,
            } => Ok(Self::AdvanceTerm(AdvanceTermCommand {
                expected_term: Term::new(expected_term).map_err(validation_error)?,
                new_term: Term::new(new_term).map_err(validation_error)?,
            })),
            ReplicatedBrokerCommandV1::AcquireCommandSessionOwner { .. } => {
                Err(ReplicatedCommandError(
                    "command-session owner acquisition is consensus-owned and cannot become a Broker domain command"
                        .to_owned(),
                ))
            }
        }
    }
}

fn convert_coordination_command(
    command: ReplicatedBrokerCommandV1,
) -> Result<BrokerCommand, ReplicatedCommandError> {
    match command {
        ReplicatedBrokerCommandV1::EnsureNamespace {
            namespace_id,
            max_namespaces,
        } => Ok(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: NamespaceId::new(namespace_id).map_err(validation_error)?,
            max_namespaces: u64_to_usize(max_namespaces, "max_namespaces")?,
        })),
        ReplicatedBrokerCommandV1::PublishTask {
            namespace_id,
            task_id,
            objective,
            created_at_ms,
            max_namespace_tasks,
        } => Ok(BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: NamespaceId::new(namespace_id).map_err(validation_error)?,
            task_id: TaskId::new(task_id).map_err(validation_error)?,
            objective: TaskObjective::new(objective).map_err(validation_error)?,
            created_at_ms: TimestampMs::new(created_at_ms),
            max_namespace_tasks: u64_to_usize(max_namespace_tasks, "max_namespace_tasks")?,
        })),
        ReplicatedBrokerCommandV1::EnsureConsumerGroup {
            namespace_id,
            group_id,
            max_namespace_groups,
        } => Ok(BrokerCommand::EnsureConsumerGroup(
            EnsureConsumerGroupCommand {
                namespace_id: NamespaceId::new(namespace_id).map_err(validation_error)?,
                group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
                max_namespace_groups: u64_to_usize(max_namespace_groups, "max_namespace_groups")?,
            },
        )),
        ReplicatedBrokerCommandV1::JoinConsumerGroup {
            group_id,
            member_id,
            capabilities,
            now_ms,
            max_group_members,
        } => Ok(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
            group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
            member_id: ConsumerId::new(member_id).map_err(validation_error)?,
            capabilities: Capabilities::new(capabilities).map_err(validation_error)?,
            now_ms: TimestampMs::new(now_ms),
            max_group_members: u64_to_usize(max_group_members, "max_group_members")?,
        })),
        ReplicatedBrokerCommandV1::Heartbeat {
            group_id,
            member_id,
            expected_generation,
            now_ms,
        } => Ok(BrokerCommand::Heartbeat(HeartbeatCommand {
            group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
            member_id: ConsumerId::new(member_id).map_err(validation_error)?,
            expected_generation: Generation::new(expected_generation),
            now_ms: TimestampMs::new(now_ms),
        })),
        ReplicatedBrokerCommandV1::LeaveConsumerGroup {
            group_id,
            member_id,
            expected_generation,
        } => Ok(BrokerCommand::LeaveConsumerGroup(
            LeaveConsumerGroupCommand {
                group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
                member_id: ConsumerId::new(member_id).map_err(validation_error)?,
                expected_generation: Generation::new(expected_generation),
            },
        )),
        ReplicatedBrokerCommandV1::ReapStaleMembers {
            stale_before_ms,
            max_members,
        } => Ok(BrokerCommand::ReapStaleMembers(ReapStaleMembersCommand {
            stale_before_ms: TimestampMs::new(stale_before_ms),
            max_members: u64_to_usize(max_members, "max_members")?,
        })),
        _ => Err(ReplicatedCommandError(
            "replicated Broker command was routed to the wrong conversion family".to_owned(),
        )),
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReplicatedCommandError(String);

impl ReplicatedCommandError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ReplicatedCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ReplicatedCommandError {}

fn validation_error(error: impl fmt::Display) -> ReplicatedCommandError {
    ReplicatedCommandError(format!(
        "replicated Broker command failed domain validation: {error}"
    ))
}

fn usize_to_u64(value: usize, field: &'static str) -> Result<u64, ReplicatedCommandError> {
    u64::try_from(value)
        .map_err(|_| ReplicatedCommandError(format!("{field} cannot be represented as u64")))
}

fn u64_to_usize(value: u64, field: &'static str) -> Result<usize, ReplicatedCommandError> {
    usize::try_from(value)
        .map_err(|_| ReplicatedCommandError(format!("{field} exceeds platform usize")))
}

#[cfg(test)]
mod tests {
    use super::ReplicatedBrokerCommandV1 as Command;

    fn assert_equivalent<const N: usize>(pairs: [(Command, Command); N]) {
        for (first, retry) in pairs {
            assert!(first.same_identified_request(&retry));
            assert!(retry.same_identified_request(&first));
        }
    }

    #[test]
    fn identified_retry_ignores_server_capacity_fields() {
        assert_equivalent([
            (
                Command::EnsureNamespace {
                    namespace_id: "ns".to_owned(),
                    max_namespaces: 1,
                },
                Command::EnsureNamespace {
                    namespace_id: "ns".to_owned(),
                    max_namespaces: 999,
                },
            ),
            (
                Command::PublishTask {
                    namespace_id: "ns".to_owned(),
                    task_id: "task".to_owned(),
                    objective: "objective".to_owned(),
                    created_at_ms: 10,
                    max_namespace_tasks: 1,
                },
                Command::PublishTask {
                    namespace_id: "ns".to_owned(),
                    task_id: "task".to_owned(),
                    objective: "objective".to_owned(),
                    created_at_ms: 10,
                    max_namespace_tasks: 999,
                },
            ),
            (
                Command::EnsureConsumerGroup {
                    namespace_id: "ns".to_owned(),
                    group_id: "group".to_owned(),
                    max_namespace_groups: 1,
                },
                Command::EnsureConsumerGroup {
                    namespace_id: "ns".to_owned(),
                    group_id: "group".to_owned(),
                    max_namespace_groups: 999,
                },
            ),
            (
                Command::JoinConsumerGroup {
                    group_id: "group".to_owned(),
                    member_id: "member".to_owned(),
                    capabilities: vec!["cap".to_owned()],
                    now_ms: 10,
                    max_group_members: 1,
                },
                Command::JoinConsumerGroup {
                    group_id: "group".to_owned(),
                    member_id: "member".to_owned(),
                    capabilities: vec!["cap".to_owned()],
                    now_ms: 10,
                    max_group_members: 999,
                },
            ),
        ]);
    }

    #[test]
    fn identified_retry_ignores_server_observed_coordination_timestamps() {
        assert_equivalent([
            (
                Command::PublishTask {
                    namespace_id: "ns".to_owned(),
                    task_id: "task".to_owned(),
                    objective: "objective".to_owned(),
                    created_at_ms: 10,
                    max_namespace_tasks: 8,
                },
                Command::PublishTask {
                    namespace_id: "ns".to_owned(),
                    task_id: "task".to_owned(),
                    objective: "objective".to_owned(),
                    created_at_ms: 99,
                    max_namespace_tasks: 8,
                },
            ),
            (
                Command::JoinConsumerGroup {
                    group_id: "group".to_owned(),
                    member_id: "member".to_owned(),
                    capabilities: vec!["cap".to_owned()],
                    now_ms: 10,
                    max_group_members: 8,
                },
                Command::JoinConsumerGroup {
                    group_id: "group".to_owned(),
                    member_id: "member".to_owned(),
                    capabilities: vec!["cap".to_owned()],
                    now_ms: 99,
                    max_group_members: 8,
                },
            ),
            (
                Command::Heartbeat {
                    group_id: "group".to_owned(),
                    member_id: "member".to_owned(),
                    expected_generation: 2,
                    now_ms: 10,
                },
                Command::Heartbeat {
                    group_id: "group".to_owned(),
                    member_id: "member".to_owned(),
                    expected_generation: 2,
                    now_ms: 99,
                },
            ),
        ]);
    }

    #[test]
    fn identified_retry_ignores_server_observed_task_timestamps() {
        assert_equivalent([
            (
                Command::ClaimTask {
                    group_id: "group".to_owned(),
                    member_id: "member".to_owned(),
                    expected_term: 3,
                    expected_generation: 2,
                    lease_id: "lease".to_owned(),
                    now_ms: 10,
                    lease_duration_ms: 500,
                },
                Command::ClaimTask {
                    group_id: "group".to_owned(),
                    member_id: "member".to_owned(),
                    expected_term: 3,
                    expected_generation: 2,
                    lease_id: "lease".to_owned(),
                    now_ms: 99,
                    lease_duration_ms: 500,
                },
            ),
            (
                Command::RenewTaskLease {
                    task_id: "task".to_owned(),
                    group_id: "group".to_owned(),
                    member_id: "member".to_owned(),
                    expected_term: 3,
                    expected_generation: 2,
                    expected_lease_epoch: 4,
                    lease_id: "lease".to_owned(),
                    now_ms: 10,
                    lease_duration_ms: 500,
                },
                Command::RenewTaskLease {
                    task_id: "task".to_owned(),
                    group_id: "group".to_owned(),
                    member_id: "member".to_owned(),
                    expected_term: 3,
                    expected_generation: 2,
                    expected_lease_epoch: 4,
                    lease_id: "lease".to_owned(),
                    now_ms: 99,
                    lease_duration_ms: 500,
                },
            ),
            (
                Command::CompleteTask {
                    task_id: "task".to_owned(),
                    group_id: "group".to_owned(),
                    member_id: "member".to_owned(),
                    expected_term: 3,
                    expected_generation: 2,
                    expected_lease_epoch: 4,
                    lease_id: "lease".to_owned(),
                    result: "done".to_owned(),
                    completed_at_ms: 10,
                },
                Command::CompleteTask {
                    task_id: "task".to_owned(),
                    group_id: "group".to_owned(),
                    member_id: "member".to_owned(),
                    expected_term: 3,
                    expected_generation: 2,
                    expected_lease_epoch: 4,
                    lease_id: "lease".to_owned(),
                    result: "done".to_owned(),
                    completed_at_ms: 99,
                },
            ),
        ]);
    }

    #[test]
    fn identified_retry_rejects_changed_client_semantics() {
        let base = Command::PublishTask {
            namespace_id: "ns".to_owned(),
            task_id: "task".to_owned(),
            objective: "objective".to_owned(),
            created_at_ms: 10,
            max_namespace_tasks: 1,
        };
        let changed = Command::PublishTask {
            namespace_id: "ns".to_owned(),
            task_id: "task-other".to_owned(),
            objective: "objective".to_owned(),
            created_at_ms: 99,
            max_namespace_tasks: 999,
        };
        assert!(!base.same_identified_request(&changed));

        let base = Command::ClaimTask {
            group_id: "group".to_owned(),
            member_id: "member".to_owned(),
            expected_term: 3,
            expected_generation: 2,
            lease_id: "lease".to_owned(),
            now_ms: 10,
            lease_duration_ms: 500,
        };
        let changed = Command::ClaimTask {
            group_id: "group".to_owned(),
            member_id: "member".to_owned(),
            expected_term: 3,
            expected_generation: 2,
            lease_id: "lease".to_owned(),
            now_ms: 99,
            lease_duration_ms: 501,
        };
        assert!(!base.same_identified_request(&changed));

        let base = Command::CompleteTask {
            task_id: "task".to_owned(),
            group_id: "group".to_owned(),
            member_id: "member".to_owned(),
            expected_term: 3,
            expected_generation: 2,
            expected_lease_epoch: 4,
            lease_id: "lease".to_owned(),
            result: "done".to_owned(),
            completed_at_ms: 10,
        };
        let changed = Command::CompleteTask {
            task_id: "task".to_owned(),
            group_id: "group".to_owned(),
            member_id: "member".to_owned(),
            expected_term: 3,
            expected_generation: 2,
            expected_lease_epoch: 4,
            lease_id: "lease".to_owned(),
            result: "different".to_owned(),
            completed_at_ms: 99,
        };
        assert!(!base.same_identified_request(&changed));
    }
}

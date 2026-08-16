use std::error::Error;
use std::fmt;

use agent_broker_domain::commands::{
    AdvanceTermCommand, BrokerCommand, ClaimTaskCommand, CompleteTaskCommand,
    EnsureConsumerGroupCommand, EnsureNamespaceCommand, HeartbeatCommand, JoinConsumerGroupCommand,
    LeaveConsumerGroupCommand, PruneCompletedTasksCommand, PublishTaskCommand,
    ReapStaleMembersCommand, RenewTaskLeaseCommand,
};
use agent_broker_domain::{
    Capabilities, ConsumerGroupId, Generation, LeaseEpoch, LeaseId, MemberId, NamespaceId, TaskId,
    TaskObjective, TaskResult, Term, TimestampMs,
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
            ReplicatedBrokerCommandV1::EnsureNamespace {
                namespace_id,
                max_namespaces,
            } => Ok(Self::EnsureNamespace(EnsureNamespaceCommand {
                namespace_id: NamespaceId::new(namespace_id).map_err(validation_error)?,
                max_namespaces: u64_to_usize(max_namespaces, "max_namespaces")?,
            })),
            ReplicatedBrokerCommandV1::PublishTask {
                namespace_id,
                task_id,
                objective,
                created_at_ms,
                max_namespace_tasks,
            } => Ok(Self::PublishTask(PublishTaskCommand {
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
            } => Ok(Self::EnsureConsumerGroup(EnsureConsumerGroupCommand {
                namespace_id: NamespaceId::new(namespace_id).map_err(validation_error)?,
                group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
                max_namespace_groups: u64_to_usize(max_namespace_groups, "max_namespace_groups")?,
            })),
            ReplicatedBrokerCommandV1::JoinConsumerGroup {
                group_id,
                member_id,
                capabilities,
                now_ms,
                max_group_members,
            } => Ok(Self::JoinConsumerGroup(JoinConsumerGroupCommand {
                group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
                member_id: MemberId::new(member_id).map_err(validation_error)?,
                capabilities: Capabilities::new(capabilities).map_err(validation_error)?,
                now_ms: TimestampMs::new(now_ms),
                max_group_members: u64_to_usize(max_group_members, "max_group_members")?,
            })),
            ReplicatedBrokerCommandV1::Heartbeat {
                group_id,
                member_id,
                expected_generation,
                now_ms,
            } => Ok(Self::Heartbeat(HeartbeatCommand {
                group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
                member_id: MemberId::new(member_id).map_err(validation_error)?,
                expected_generation: Generation::new(expected_generation),
                now_ms: TimestampMs::new(now_ms),
            })),
            ReplicatedBrokerCommandV1::LeaveConsumerGroup {
                group_id,
                member_id,
                expected_generation,
            } => Ok(Self::LeaveConsumerGroup(LeaveConsumerGroupCommand {
                group_id: ConsumerGroupId::new(group_id).map_err(validation_error)?,
                member_id: MemberId::new(member_id).map_err(validation_error)?,
                expected_generation: Generation::new(expected_generation),
            })),
            ReplicatedBrokerCommandV1::ReapStaleMembers {
                stale_before_ms,
                max_members,
            } => Ok(Self::ReapStaleMembers(ReapStaleMembersCommand {
                stale_before_ms: TimestampMs::new(stale_before_ms),
                max_members: u64_to_usize(max_members, "max_members")?,
            })),
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
                member_id: MemberId::new(member_id).map_err(validation_error)?,
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
                member_id: MemberId::new(member_id).map_err(validation_error)?,
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
                member_id: MemberId::new(member_id).map_err(validation_error)?,
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
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReplicatedCommandError(String);

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

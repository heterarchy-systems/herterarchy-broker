use agent_broker_domain::{
    BrokerCheckpoint, BrokerStateMachine, Capabilities, ConsumerGroupCheckpoint, ConsumerGroupId,
    ConsumerId, Generation, LeaseEpoch, LeaseId, MemberCheckpoint, NamespaceCheckpoint,
    NamespaceId, Revision, TaskCheckpoint, TaskCheckpointState, TaskId, TaskObjective, TaskResult,
    Term, TimestampMs,
};
use serde::{Deserialize, Serialize};

use crate::StorageError;

pub(crate) const SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotWire {
    pub(crate) groups: Vec<GroupWire>,
    pub(crate) namespaces: Vec<NamespaceWire>,
    pub(crate) revision: u64,
    pub(crate) schema_version: u64,
    pub(crate) tasks: Vec<TaskWire>,
    pub(crate) term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NamespaceWire {
    pub(crate) namespace_id: String,
    pub(crate) revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MemberWire {
    pub(crate) capabilities: Vec<String>,
    pub(crate) joined_at_ms: u64,
    pub(crate) last_heartbeat_at_ms: u64,
    pub(crate) member_id: String,
    pub(crate) revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GroupWire {
    pub(crate) generation: u64,
    pub(crate) group_id: String,
    pub(crate) members: Vec<MemberWire>,
    pub(crate) namespace_id: String,
    pub(crate) revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskWire {
    #[serde(default)]
    pub(crate) completed_at_ms: Option<u64>,
    pub(crate) created_at_ms: u64,
    pub(crate) lease_epoch: u64,
    pub(crate) lease_expires_at_ms: Option<u64>,
    pub(crate) lease_generation: Option<u64>,
    pub(crate) lease_group_id: Option<String>,
    pub(crate) lease_id: Option<String>,
    pub(crate) lease_owner_member_id: Option<String>,
    pub(crate) namespace_id: String,
    pub(crate) objective: String,
    pub(crate) result: Option<String>,
    pub(crate) revision: u64,
    pub(crate) status: String,
    pub(crate) task_id: String,
}

/// Encode the logical Broker checkpoint using the Python snapshot schema-v1 representation.
///
/// # Errors
///
/// Returns [`StorageError`] only when JSON serialization fails.
pub fn encode_snapshot(checkpoint: &BrokerCheckpoint) -> Result<Vec<u8>, StorageError> {
    let wire = checkpoint_to_wire(checkpoint);
    serde_json::to_vec(&wire).map_err(|error| StorageError::Serialization(error.to_string()))
}

/// Decode and validate one Python-compatible snapshot schema-v1 image.
///
/// # Errors
///
/// Returns [`StorageError`] for malformed JSON, unsupported schema, invalid domain values, or a
/// logical checkpoint that cannot rebuild a valid Broker state machine.
pub fn decode_snapshot(bytes: &[u8]) -> Result<BrokerCheckpoint, StorageError> {
    let wire: SnapshotWire = serde_json::from_slice(bytes).map_err(|error| {
        StorageError::InvalidFormat(format!("invalid Broker snapshot JSON: {error}"))
    })?;
    snapshot_from_wire(wire)
}

pub(crate) fn checkpoint_to_wire(checkpoint: &BrokerCheckpoint) -> SnapshotWire {
    let mut namespaces = checkpoint
        .namespaces
        .iter()
        .map(namespace_to_wire)
        .collect::<Vec<_>>();
    namespaces.sort_by(|left, right| left.namespace_id.cmp(&right.namespace_id));
    let mut tasks = checkpoint
        .tasks
        .iter()
        .map(task_to_wire)
        .collect::<Vec<_>>();
    tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
    let mut groups = checkpoint
        .groups
        .iter()
        .map(group_to_wire)
        .collect::<Vec<_>>();
    groups.sort_by(|left, right| left.group_id.cmp(&right.group_id));
    SnapshotWire {
        groups,
        namespaces,
        revision: checkpoint.revision.get(),
        schema_version: SCHEMA_VERSION,
        tasks,
        term: checkpoint.term.get(),
    }
}

pub(crate) fn snapshot_from_wire(wire: SnapshotWire) -> Result<BrokerCheckpoint, StorageError> {
    if wire.schema_version != SCHEMA_VERSION {
        return Err(StorageError::UnsupportedSchemaVersion(wire.schema_version));
    }
    let checkpoint = BrokerCheckpoint {
        term: Term::new(wire.term).map_err(invalid_value("term"))?,
        revision: Revision::new(wire.revision),
        namespaces: wire
            .namespaces
            .into_iter()
            .map(namespace_from_wire)
            .collect::<Result<_, _>>()?,
        tasks: wire
            .tasks
            .into_iter()
            .map(task_from_wire)
            .collect::<Result<_, _>>()?,
        groups: wire
            .groups
            .into_iter()
            .map(group_from_wire)
            .collect::<Result<_, _>>()?,
    };
    BrokerStateMachine::from_checkpoint(checkpoint.clone())?;
    Ok(checkpoint)
}

pub(crate) fn namespace_to_wire(namespace: &NamespaceCheckpoint) -> NamespaceWire {
    NamespaceWire {
        namespace_id: namespace.namespace_id.as_str().to_owned(),
        revision: namespace.revision.get(),
    }
}

pub(crate) fn namespace_from_wire(
    wire: NamespaceWire,
) -> Result<NamespaceCheckpoint, StorageError> {
    Ok(NamespaceCheckpoint {
        namespace_id: NamespaceId::new(wire.namespace_id).map_err(invalid_value("namespace_id"))?,
        revision: positive_revision(wire.revision, "namespace.revision")?,
    })
}

pub(crate) fn group_to_wire(group: &ConsumerGroupCheckpoint) -> GroupWire {
    let mut members = group.members.iter().map(member_to_wire).collect::<Vec<_>>();
    members.sort_by(|left, right| left.member_id.cmp(&right.member_id));
    GroupWire {
        generation: group.generation.get(),
        group_id: group.group_id.as_str().to_owned(),
        members,
        namespace_id: group.namespace_id.as_str().to_owned(),
        revision: group.revision.get(),
    }
}

pub(crate) fn group_from_wire(wire: GroupWire) -> Result<ConsumerGroupCheckpoint, StorageError> {
    Ok(ConsumerGroupCheckpoint {
        group_id: ConsumerGroupId::new(wire.group_id).map_err(invalid_value("group_id"))?,
        namespace_id: NamespaceId::new(wire.namespace_id)
            .map_err(invalid_value("group.namespace_id"))?,
        generation: Generation::new(wire.generation),
        revision: positive_revision(wire.revision, "group.revision")?,
        members: wire
            .members
            .into_iter()
            .map(member_from_wire)
            .collect::<Result<_, _>>()?,
    })
}

fn member_to_wire(member: &MemberCheckpoint) -> MemberWire {
    MemberWire {
        capabilities: member
            .capabilities
            .as_slice()
            .iter()
            .map(|capability| capability.as_str().to_owned())
            .collect(),
        joined_at_ms: member.joined_at_ms.get(),
        last_heartbeat_at_ms: member.last_heartbeat_at_ms.get(),
        member_id: member.member_id.as_str().to_owned(),
        revision: member.revision.get(),
    }
}

fn member_from_wire(wire: MemberWire) -> Result<MemberCheckpoint, StorageError> {
    Ok(MemberCheckpoint {
        member_id: ConsumerId::new(wire.member_id).map_err(invalid_value("member_id"))?,
        capabilities: Capabilities::new(wire.capabilities)
            .map_err(invalid_value("member.capabilities"))?,
        joined_at_ms: TimestampMs::new(wire.joined_at_ms),
        last_heartbeat_at_ms: TimestampMs::new(wire.last_heartbeat_at_ms),
        revision: positive_revision(wire.revision, "member.revision")?,
    })
}

pub(crate) fn task_to_wire(task: &TaskCheckpoint) -> TaskWire {
    let mut wire = TaskWire {
        completed_at_ms: None,
        created_at_ms: task.created_at_ms.get(),
        lease_epoch: 0,
        lease_expires_at_ms: None,
        lease_generation: None,
        lease_group_id: None,
        lease_id: None,
        lease_owner_member_id: None,
        namespace_id: task.namespace_id.as_str().to_owned(),
        objective: task.objective.as_str().to_owned(),
        result: None,
        revision: task.revision.get(),
        status: String::new(),
        task_id: task.task_id.as_str().to_owned(),
    };
    match &task.state {
        TaskCheckpointState::Queued { lease_epoch } => {
            wire.lease_epoch = lease_epoch.get();
            "queued".clone_into(&mut wire.status);
        }
        TaskCheckpointState::Leased {
            lease_id,
            owner_member_id,
            group_id,
            generation,
            lease_epoch,
            expires_at_ms,
        } => {
            wire.lease_epoch = lease_epoch.get();
            wire.lease_expires_at_ms = Some(expires_at_ms.get());
            wire.lease_generation = Some(generation.get());
            wire.lease_group_id = Some(group_id.as_str().to_owned());
            wire.lease_id = Some(lease_id.as_str().to_owned());
            wire.lease_owner_member_id = Some(owner_member_id.as_str().to_owned());
            "leased".clone_into(&mut wire.status);
        }
        TaskCheckpointState::Completed {
            lease_id,
            owner_member_id,
            group_id,
            generation,
            lease_epoch,
            result,
            completed_at_ms,
        } => {
            wire.completed_at_ms = Some(completed_at_ms.get());
            wire.lease_epoch = lease_epoch.get();
            wire.lease_generation = Some(generation.get());
            wire.lease_group_id = Some(group_id.as_str().to_owned());
            wire.lease_id = Some(lease_id.as_str().to_owned());
            wire.lease_owner_member_id = Some(owner_member_id.as_str().to_owned());
            wire.result = Some(result.as_str().to_owned());
            "completed".clone_into(&mut wire.status);
        }
    }
    wire
}

pub(crate) fn task_from_wire(wire: TaskWire) -> Result<TaskCheckpoint, StorageError> {
    let state = task_state_from_wire(&wire)?;
    Ok(TaskCheckpoint {
        task_id: TaskId::new(wire.task_id).map_err(invalid_value("task_id"))?,
        namespace_id: NamespaceId::new(wire.namespace_id)
            .map_err(invalid_value("task.namespace_id"))?,
        objective: TaskObjective::new(wire.objective).map_err(invalid_value("task.objective"))?,
        created_at_ms: TimestampMs::new(wire.created_at_ms),
        revision: positive_revision(wire.revision, "task.revision")?,
        state,
    })
}

fn task_state_from_wire(wire: &TaskWire) -> Result<TaskCheckpointState, StorageError> {
    let lease_epoch = LeaseEpoch::new(wire.lease_epoch);
    match wire.status.as_str() {
        "queued" => {
            require_queued_task_fields(wire)?;
            Ok(TaskCheckpointState::Queued { lease_epoch })
        }
        "leased" => {
            require_leased_task_fields(wire)?;
            Ok(TaskCheckpointState::Leased {
                lease_id: required_id(wire.lease_id.as_deref(), "task.lease_id", LeaseId::new)?,
                owner_member_id: required_id(
                    wire.lease_owner_member_id.as_deref(),
                    "task.lease_owner_member_id",
                    ConsumerId::new,
                )?,
                group_id: required_id(
                    wire.lease_group_id.as_deref(),
                    "task.lease_group_id",
                    ConsumerGroupId::new,
                )?,
                generation: Generation::new(required_u64(
                    wire.lease_generation,
                    "task.lease_generation",
                )?),
                lease_epoch,
                expires_at_ms: TimestampMs::new(required_u64(
                    wire.lease_expires_at_ms,
                    "task.lease_expires_at_ms",
                )?),
            })
        }
        "completed" => {
            if wire.lease_expires_at_ms.is_some() {
                return Err(invalid_format(
                    "completed task must not retain lease_expires_at_ms",
                ));
            }
            Ok(TaskCheckpointState::Completed {
                lease_id: required_id(wire.lease_id.as_deref(), "task.lease_id", LeaseId::new)?,
                owner_member_id: required_id(
                    wire.lease_owner_member_id.as_deref(),
                    "task.lease_owner_member_id",
                    ConsumerId::new,
                )?,
                group_id: required_id(
                    wire.lease_group_id.as_deref(),
                    "task.lease_group_id",
                    ConsumerGroupId::new,
                )?,
                generation: Generation::new(required_u64(
                    wire.lease_generation,
                    "task.lease_generation",
                )?),
                lease_epoch,
                result: TaskResult::new(required_string(wire.result.as_deref(), "task.result")?)
                    .map_err(invalid_value("task.result"))?,
                completed_at_ms: TimestampMs::new(required_u64(
                    wire.completed_at_ms,
                    "task.completed_at_ms",
                )?),
            })
        }
        other => Err(invalid_format(format!("unknown task status {other:?}"))),
    }
}

fn require_queued_task_fields(wire: &TaskWire) -> Result<(), StorageError> {
    if wire.lease_id.is_some()
        || wire.lease_owner_member_id.is_some()
        || wire.lease_group_id.is_some()
        || wire.lease_generation.is_some()
        || wire.lease_expires_at_ms.is_some()
        || wire.completed_at_ms.is_some()
        || wire.result.is_some()
    {
        return Err(invalid_format(
            "queued task contains lease/completion fields",
        ));
    }
    Ok(())
}

fn require_leased_task_fields(wire: &TaskWire) -> Result<(), StorageError> {
    if wire.result.is_some() || wire.completed_at_ms.is_some() {
        return Err(invalid_format(
            "leased task contains completion-only fields",
        ));
    }
    Ok(())
}

fn positive_revision(value: u64, field: &str) -> Result<Revision, StorageError> {
    if value == 0 {
        return Err(invalid_format(format!("{field} must be positive")));
    }
    Ok(Revision::new(value))
}

fn required_u64(value: Option<u64>, field: &str) -> Result<u64, StorageError> {
    value.ok_or_else(|| invalid_format(format!("{field} is required")))
}

fn required_string<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str, StorageError> {
    value.ok_or_else(|| invalid_format(format!("{field} is required")))
}

fn required_id<T, E>(
    value: Option<&str>,
    field: &'static str,
    constructor: impl FnOnce(String) -> Result<T, E>,
) -> Result<T, StorageError>
where
    E: std::fmt::Display,
{
    let raw = required_string(value, field)?;
    constructor(raw.to_owned()).map_err(invalid_value(field))
}

fn invalid_format(message: impl Into<String>) -> StorageError {
    StorageError::InvalidFormat(message.into())
}

fn invalid_value<E>(field: &'static str) -> impl FnOnce(E) -> StorageError
where
    E: std::fmt::Display,
{
    move |error| invalid_format(format!("{field} is invalid: {error}"))
}

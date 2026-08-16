use std::collections::BTreeSet;

use agent_broker_domain::results::StateChangeSet;
use agent_broker_domain::{
    BrokerCheckpoint, BrokerState, BrokerStateMachine, ConsumerGroupCheckpoint,
    NamespaceCheckpoint, Revision, TaskCheckpoint, TaskId, Term,
};
use serde::{Deserialize, Serialize};

use crate::StorageError;
use crate::snapshot_v1::{
    GroupWire, NamespaceWire, SCHEMA_VERSION, TaskWire, group_from_wire, group_to_wire,
    namespace_from_wire, namespace_to_wire, task_from_wire, task_to_wire,
};

/// Logical schema-v1 journal mutation after decoding and domain validation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JournalMutation {
    pub term: Term,
    pub revision: Revision,
    pub namespaces: Vec<NamespaceCheckpoint>,
    pub tasks: Vec<TaskCheckpoint>,
    pub deleted_tasks: Vec<TaskId>,
    pub groups: Vec<ConsumerGroupCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalMutationWire {
    #[serde(default)]
    deleted_tasks: Vec<String>,
    groups: Vec<GroupWire>,
    namespaces: Vec<NamespaceWire>,
    revision: u64,
    schema_version: u64,
    tasks: Vec<TaskWire>,
    term: u64,
}

/// Encode one state-machine change set using the Python journal schema-v1 NDJSON representation.
///
/// # Errors
///
/// Returns [`StorageError`] if a changed entity is absent from authoritative state, a Task is both
/// updated and deleted, or JSON serialization fails.
pub fn encode_journal_mutation(
    state: &BrokerState,
    changes: &StateChangeSet,
) -> Result<Vec<u8>, StorageError> {
    let updated_tasks = changes.tasks.iter().cloned().collect::<BTreeSet<_>>();
    if changes
        .deleted_tasks
        .iter()
        .any(|task_id| updated_tasks.contains(task_id))
    {
        return Err(StorageError::InvalidFormat(
            "journal mutation cannot update and delete the same task".to_owned(),
        ));
    }

    let mut namespaces = changes
        .namespaces
        .iter()
        .map(|namespace_id| {
            state
                .namespace(namespace_id)
                .map(agent_broker_domain::Namespace::checkpoint)
                .map(|checkpoint| namespace_to_wire(&checkpoint))
                .ok_or_else(|| missing_changed_entity("namespace"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    namespaces.sort_by(|left, right| left.namespace_id.cmp(&right.namespace_id));

    let mut tasks = changes
        .tasks
        .iter()
        .map(|task_id| {
            state
                .task(task_id)
                .map(agent_broker_domain::Task::checkpoint)
                .map(|checkpoint| task_to_wire(&checkpoint))
                .ok_or_else(|| missing_changed_entity("task"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));

    let mut groups = changes
        .groups
        .iter()
        .map(|group_id| {
            state
                .group(group_id)
                .map(agent_broker_domain::ConsumerGroup::checkpoint)
                .map(|checkpoint| group_to_wire(&checkpoint))
                .ok_or_else(|| missing_changed_entity("Consumer Group"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    groups.sort_by(|left, right| left.group_id.cmp(&right.group_id));

    let mut deleted_tasks = changes
        .deleted_tasks
        .iter()
        .map(|task_id| task_id.as_str().to_owned())
        .collect::<Vec<_>>();
    deleted_tasks.sort();

    let wire = JournalMutationWire {
        deleted_tasks,
        groups,
        namespaces,
        revision: state.revision().get(),
        schema_version: SCHEMA_VERSION,
        tasks,
        term: state.term().get(),
    };
    let mut encoded = serde_json::to_vec(&wire)
        .map_err(|error| StorageError::Serialization(error.to_string()))?;
    encoded.push(b'\n');
    Ok(encoded)
}

/// Decode one strict journal schema-v1 record.
///
/// # Errors
///
/// Returns [`StorageError`] for malformed JSON, unknown root fields, unsupported schema, invalid
/// identities/state payloads, non-positive journal revision, or update/delete overlap.
pub fn decode_journal_mutation(bytes: &[u8]) -> Result<JournalMutation, StorageError> {
    let wire: JournalMutationWire = serde_json::from_slice(bytes).map_err(|error| {
        StorageError::InvalidFormat(format!("invalid journal record JSON: {error}"))
    })?;
    if wire.schema_version != SCHEMA_VERSION {
        return Err(StorageError::UnsupportedSchemaVersion(wire.schema_version));
    }
    if wire.revision == 0 {
        return Err(StorageError::InvalidFormat(
            "journal revision must be positive".to_owned(),
        ));
    }
    let deleted_tasks = wire
        .deleted_tasks
        .into_iter()
        .map(|task_id| {
            TaskId::new(task_id).map_err(|error| {
                StorageError::InvalidFormat(format!("deleted task ID is invalid: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let tasks = wire
        .tasks
        .into_iter()
        .map(task_from_wire)
        .collect::<Result<Vec<_>, _>>()?;
    let updated = tasks
        .iter()
        .map(|task| task.task_id.clone())
        .collect::<BTreeSet<_>>();
    if deleted_tasks
        .iter()
        .any(|task_id| updated.contains(task_id))
    {
        return Err(StorageError::InvalidFormat(
            "journal record updates and deletes the same task".to_owned(),
        ));
    }
    Ok(JournalMutation {
        term: Term::new(wire.term)
            .map_err(|error| StorageError::InvalidFormat(format!("term is invalid: {error}")))?,
        revision: Revision::new(wire.revision),
        namespaces: wire
            .namespaces
            .into_iter()
            .map(namespace_from_wire)
            .collect::<Result<_, _>>()?,
        tasks,
        deleted_tasks,
        groups: wire
            .groups
            .into_iter()
            .map(group_from_wire)
            .collect::<Result<_, _>>()?,
    })
}

/// Apply one decoded journal mutation transactionally to a logical checkpoint.
///
/// Stale pre-compaction records at or below the snapshot revision are ignored like the Python
/// reference. A forward revision gap, backward term, local entity revision rollback, or invalid
/// resulting checkpoint fails closed without mutating the caller's checkpoint.
///
/// # Errors
///
/// Returns [`StorageError`] for any ordering or logical state violation.
pub fn apply_journal_mutation(
    checkpoint: &mut BrokerCheckpoint,
    mutation: JournalMutation,
) -> Result<bool, StorageError> {
    if mutation.revision <= checkpoint.revision {
        return Ok(false);
    }
    let expected = checkpoint
        .revision
        .next()
        .map_err(|error| StorageError::InvalidFormat(error.to_string()))?;
    if mutation.revision != expected {
        return Err(StorageError::RevisionGap {
            expected,
            actual: mutation.revision,
        });
    }
    if mutation.term < checkpoint.term {
        return Err(StorageError::BackwardTerm {
            current: checkpoint.term,
            incoming: mutation.term,
        });
    }

    let mut candidate = checkpoint.clone();
    for namespace in mutation.namespaces {
        upsert_namespace(&mut candidate.namespaces, namespace)?;
    }
    for group in mutation.groups {
        upsert_group(&mut candidate.groups, group)?;
    }
    for task in mutation.tasks {
        upsert_task(&mut candidate.tasks, task)?;
    }
    let deleted = mutation.deleted_tasks.into_iter().collect::<BTreeSet<_>>();
    candidate
        .tasks
        .retain(|task| !deleted.contains(&task.task_id));
    candidate.term = mutation.term;
    candidate.revision = mutation.revision;
    sort_checkpoint(&mut candidate);
    BrokerStateMachine::from_checkpoint(candidate.clone())?;
    *checkpoint = candidate;
    Ok(true)
}

fn upsert_namespace(
    values: &mut Vec<NamespaceCheckpoint>,
    incoming: NamespaceCheckpoint,
) -> Result<(), StorageError> {
    if let Some(existing) = values
        .iter_mut()
        .find(|item| item.namespace_id == incoming.namespace_id)
    {
        ensure_local_revision("namespace", existing.revision, incoming.revision)?;
        *existing = incoming;
    } else {
        values.push(incoming);
    }
    Ok(())
}

fn upsert_group(
    values: &mut Vec<ConsumerGroupCheckpoint>,
    incoming: ConsumerGroupCheckpoint,
) -> Result<(), StorageError> {
    if let Some(existing) = values
        .iter_mut()
        .find(|item| item.group_id == incoming.group_id)
    {
        ensure_local_revision("Consumer Group", existing.revision, incoming.revision)?;
        *existing = incoming;
    } else {
        values.push(incoming);
    }
    Ok(())
}

fn upsert_task(
    values: &mut Vec<TaskCheckpoint>,
    incoming: TaskCheckpoint,
) -> Result<(), StorageError> {
    if let Some(existing) = values
        .iter_mut()
        .find(|item| item.task_id == incoming.task_id)
    {
        ensure_local_revision("task", existing.revision, incoming.revision)?;
        *existing = incoming;
    } else {
        values.push(incoming);
    }
    Ok(())
}

fn ensure_local_revision(
    entity: &'static str,
    current: Revision,
    incoming: Revision,
) -> Result<(), StorageError> {
    if incoming < current {
        return Err(StorageError::EntityRevisionRollback { entity });
    }
    Ok(())
}

fn sort_checkpoint(checkpoint: &mut BrokerCheckpoint) {
    checkpoint
        .namespaces
        .sort_by(|left, right| left.namespace_id.cmp(&right.namespace_id));
    checkpoint
        .tasks
        .sort_by(|left, right| left.task_id.cmp(&right.task_id));
    checkpoint
        .groups
        .sort_by(|left, right| left.group_id.cmp(&right.group_id));
    for group in &mut checkpoint.groups {
        group
            .members
            .sort_by(|left, right| left.member_id.cmp(&right.member_id));
    }
}

fn missing_changed_entity(entity: &'static str) -> StorageError {
    StorageError::InvalidFormat(format!(
        "changed {entity} is absent from authoritative state"
    ))
}

use std::error::Error;

use agent_broker_domain::commands::{
    BrokerCommand, ClaimTaskCommand, CompleteTaskCommand, EnsureConsumerGroupCommand,
    EnsureNamespaceCommand, JoinConsumerGroupCommand, PublishTaskCommand,
};
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{
    BrokerCheckpoint, BrokerStateMachine, Capabilities, ConsumerGroupId, ConsumerId, Generation,
    LeaseId, NamespaceId, Revision, TaskId, TaskObjective, TaskResult, Term, TimestampMs,
};
use agent_broker_storage::{
    StorageError, apply_journal_mutation, decode_journal_mutation, decode_snapshot,
    encode_journal_mutation, encode_snapshot,
};
use serde_json::Value;

const SNAPSHOT_CORPUS: &[u8] = include_bytes!("../../../compatibility/storage-v1/snapshot.json");
const JOURNAL_CORPUS: &[u8] = include_bytes!("../../../compatibility/storage-v1/journal.ndjson");

struct RustArtifacts {
    snapshot: Vec<u8>,
    journal: Vec<u8>,
}

struct ReferenceIds {
    namespace_id: NamespaceId,
    group_id: ConsumerGroupId,
    member_id: ConsumerId,
    generation: Generation,
}

fn apply_and_record(
    machine: &mut BrokerStateMachine,
    command: BrokerCommand,
    journal: &mut Vec<u8>,
) -> Result<BrokerMutationResult, Box<dyn Error>> {
    let applied = machine.apply(command)?;
    journal.extend(encode_journal_mutation(machine.state(), &applied.changes)?);
    Ok(applied.result)
}

fn rust_reference_artifacts() -> Result<RustArtifacts, Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let mut journal = Vec::new();
    let ids = setup_reference(&mut machine, &mut journal)?;
    add_completed_task(&mut machine, &mut journal, &ids)?;
    add_leased_and_queued_tasks(&mut machine, &mut journal, ids)?;
    Ok(RustArtifacts {
        snapshot: encode_snapshot(&machine.state().checkpoint())?,
        journal,
    })
}

fn setup_reference(
    machine: &mut BrokerStateMachine,
    journal: &mut Vec<u8>,
) -> Result<ReferenceIds, Box<dyn Error>> {
    let namespace_id = NamespaceId::new("project-a")?;
    let group_id = ConsumerGroupId::new("engineering")?;
    let member_id = ConsumerId::new("worker-a")?;
    apply_and_record(
        machine,
        BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: namespace_id.clone(),
            max_namespaces: 64,
        }),
        journal,
    )?;
    apply_and_record(
        machine,
        BrokerCommand::EnsureConsumerGroup(EnsureConsumerGroupCommand {
            namespace_id: namespace_id.clone(),
            group_id: group_id.clone(),
            max_namespace_groups: 64,
        }),
        journal,
    )?;
    let joined = apply_and_record(
        machine,
        BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            capabilities: Capabilities::new(["code", "review"])?,
            now_ms: TimestampMs::new(1_000),
            max_group_members: 256,
        }),
        journal,
    )?;
    let BrokerMutationResult::ConsumerGroup(joined) = joined else {
        return Err("join result must be ConsumerGroup".into());
    };
    Ok(ReferenceIds {
        namespace_id,
        group_id,
        member_id,
        generation: joined.generation,
    })
}

fn add_completed_task(
    machine: &mut BrokerStateMachine,
    journal: &mut Vec<u8>,
    ids: &ReferenceIds,
) -> Result<(), Box<dyn Error>> {
    apply_and_record(
        machine,
        BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: ids.namespace_id.clone(),
            task_id: TaskId::new("task-completed")?,
            objective: TaskObjective::new("완료 task")?,
            created_at_ms: TimestampMs::new(1_500),
            max_namespace_tasks: 4_096,
        }),
        journal,
    )?;
    let claim = apply_and_record(
        machine,
        BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id: ids.group_id.clone(),
            member_id: ids.member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: ids.generation,
            lease_id: LeaseId::new("lease-completed")?,
            now_ms: TimestampMs::new(2_000),
            lease_duration_ms: 60_000,
        }),
        journal,
    )?;
    let BrokerMutationResult::TaskClaim(claim) = claim else {
        return Err("claim result must be TaskClaim".into());
    };
    let lease_epoch = claim
        .lease_epoch
        .ok_or("completed claim must return lease epoch")?;
    apply_and_record(
        machine,
        BrokerCommand::CompleteTask(CompleteTaskCommand {
            task_id: TaskId::new("task-completed")?,
            group_id: ids.group_id.clone(),
            member_id: ids.member_id.clone(),
            expected_term: Term::INITIAL,
            expected_generation: ids.generation,
            expected_lease_epoch: lease_epoch,
            lease_id: LeaseId::new("lease-completed")?,
            result: TaskResult::new("완료")?,
            completed_at_ms: TimestampMs::new(2_500),
        }),
        journal,
    )?;
    Ok(())
}

fn add_leased_and_queued_tasks(
    machine: &mut BrokerStateMachine,
    journal: &mut Vec<u8>,
    ids: ReferenceIds,
) -> Result<(), Box<dyn Error>> {
    apply_and_record(
        machine,
        BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: ids.namespace_id.clone(),
            task_id: TaskId::new("task-leased")?,
            objective: TaskObjective::new("leased task")?,
            created_at_ms: TimestampMs::new(3_000),
            max_namespace_tasks: 4_096,
        }),
        journal,
    )?;
    apply_and_record(
        machine,
        BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id: ids.group_id,
            member_id: ids.member_id,
            expected_term: Term::INITIAL,
            expected_generation: ids.generation,
            lease_id: LeaseId::new("lease-active")?,
            now_ms: TimestampMs::new(3_500),
            lease_duration_ms: 60_000,
        }),
        journal,
    )?;
    apply_and_record(
        machine,
        BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: ids.namespace_id,
            task_id: TaskId::new("task-queued")?,
            objective: TaskObjective::new("queued task")?,
            created_at_ms: TimestampMs::new(4_000),
            max_namespace_tasks: 4_096,
        }),
        journal,
    )?;
    Ok(())
}

fn empty_checkpoint() -> BrokerCheckpoint {
    BrokerCheckpoint {
        term: Term::INITIAL,
        revision: Revision::new(0),
        namespaces: Vec::new(),
        tasks: Vec::new(),
        groups: Vec::new(),
    }
}

fn journal_frames() -> impl Iterator<Item = &'static [u8]> {
    JOURNAL_CORPUS
        .split_inclusive(|byte| *byte == b'\n')
        .filter(|frame| !frame.is_empty())
}

#[test]
fn rust_storage_encoder_matches_python_snapshot_and_journal_bytes() -> Result<(), Box<dyn Error>> {
    let artifacts = rust_reference_artifacts()?;
    assert_eq!(artifacts.snapshot, SNAPSHOT_CORPUS);
    assert_eq!(artifacts.journal, JOURNAL_CORPUS);
    Ok(())
}

#[test]
fn python_snapshot_round_trips_and_python_journal_replays_to_same_checkpoint()
-> Result<(), Box<dyn Error>> {
    let expected = decode_snapshot(SNAPSHOT_CORPUS)?;
    assert_eq!(encode_snapshot(&expected)?, SNAPSHOT_CORPUS);
    let mut replayed = empty_checkpoint();
    let mut count = 0;
    for frame in journal_frames() {
        let mutation = decode_journal_mutation(frame)?;
        assert!(apply_journal_mutation(&mut replayed, mutation)?);
        count += 1;
    }
    assert_eq!(count, 9);
    assert_eq!(replayed, expected);
    assert_eq!(encode_snapshot(&replayed)?, SNAPSHOT_CORPUS);
    Ok(())
}

#[test]
fn replay_ignores_stale_records_and_rejects_revision_gaps_transactionally()
-> Result<(), Box<dyn Error>> {
    let frames = journal_frames().collect::<Vec<_>>();
    let first = decode_journal_mutation(frames[0])?;
    let second = decode_journal_mutation(frames[1])?;
    let mut checkpoint = empty_checkpoint();
    assert!(apply_journal_mutation(&mut checkpoint, first.clone())?);
    let after_first = checkpoint.clone();
    assert!(!apply_journal_mutation(&mut checkpoint, first)?);
    assert_eq!(checkpoint, after_first);

    let mut gap_checkpoint = empty_checkpoint();
    let before_gap = gap_checkpoint.clone();
    assert!(matches!(
        apply_journal_mutation(&mut gap_checkpoint, second),
        Err(StorageError::RevisionGap { .. })
    ));
    assert_eq!(gap_checkpoint, before_gap);
    Ok(())
}

#[test]
fn replay_rejects_backward_term_and_entity_revision_rollback_transactionally()
-> Result<(), Box<dyn Error>> {
    let frames = journal_frames().collect::<Vec<_>>();
    let mut checkpoint = empty_checkpoint();
    assert!(apply_journal_mutation(
        &mut checkpoint,
        decode_journal_mutation(frames[0])?
    )?);

    let mut backward = checkpoint.clone();
    backward.term = Term::new(2)?;
    let before_backward = backward.clone();
    assert!(matches!(
        apply_journal_mutation(&mut backward, decode_journal_mutation(frames[1])?),
        Err(StorageError::BackwardTerm { .. })
    ));
    assert_eq!(backward, before_backward);

    assert!(apply_journal_mutation(
        &mut checkpoint,
        decode_journal_mutation(frames[1])?
    )?);
    let mut rollback = decode_journal_mutation(frames[2])?;
    let group = rollback
        .groups
        .first_mut()
        .ok_or("join mutation must contain a Consumer Group")?;
    group.revision = Revision::new(0);
    let before_rollback = checkpoint.clone();
    assert!(matches!(
        apply_journal_mutation(&mut checkpoint, rollback),
        Err(StorageError::EntityRevisionRollback { .. })
    ));
    assert_eq!(checkpoint, before_rollback);
    Ok(())
}

#[test]
fn snapshot_decoder_rejects_invalid_task_type_state_and_journal_unknown_fields()
-> Result<(), Box<dyn Error>> {
    let mut snapshot: Value = serde_json::from_slice(SNAPSHOT_CORPUS)?;
    let tasks = snapshot
        .get_mut("tasks")
        .and_then(Value::as_array_mut)
        .ok_or("snapshot tasks must be an array")?;
    let queued = tasks
        .iter_mut()
        .find(|task| task.get("status").and_then(Value::as_str) == Some("queued"))
        .ok_or("expected queued task")?;
    queued["lease_id"] = Value::String("lease-invalid".to_owned());
    let invalid_snapshot = serde_json::to_vec(&snapshot)?;
    assert!(matches!(
        decode_snapshot(&invalid_snapshot),
        Err(StorageError::InvalidFormat(_))
    ));

    let invalid_journal = br#"{"deleted_tasks":[],"groups":[],"namespaces":[],"revision":1,"schema_version":1,"tasks":[],"term":1,"unexpected":true}\n"#;
    assert!(matches!(
        decode_journal_mutation(invalid_journal),
        Err(StorageError::InvalidFormat(_))
    ));
    Ok(())
}

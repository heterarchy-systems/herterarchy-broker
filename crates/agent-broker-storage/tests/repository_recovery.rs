use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use agent_broker_domain::commands::{
    BrokerCommand, ClaimTaskCommand, EnsureConsumerGroupCommand, EnsureNamespaceCommand,
    JoinConsumerGroupCommand, PublishTaskCommand,
};
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{
    BrokerStateMachine, Capabilities, ConsumerGroupId, ConsumerId, LeaseId, NamespaceId, TaskId,
    TaskObjective, TaskState, Term, TimestampMs,
};
use agent_broker_storage::{
    BrokerStateRepository, JournalCompactionPolicy, JournaledBrokerStateRepository,
};
use tempfile::tempdir;

fn apply_and_commit(
    machine: &mut BrokerStateMachine,
    repository: &mut JournaledBrokerStateRepository,
    command: BrokerCommand,
) -> Result<BrokerMutationResult, Box<dyn Error>> {
    let before = machine.state().revision();
    let applied = machine.apply(command)?;
    if machine.state().revision() != before {
        repository.commit(machine.state(), &applied.changes)?;
    }
    Ok(applied.result)
}

fn high_threshold_policy() -> Result<JournalCompactionPolicy, Box<dyn Error>> {
    Ok(JournalCompactionPolicy::new(10_000, 64 * 1024 * 1024)?)
}

fn bootstrap_group(
    machine: &mut BrokerStateMachine,
    repository: &mut JournaledBrokerStateRepository,
) -> Result<(ConsumerGroupId, ConsumerId, agent_broker_domain::Generation), Box<dyn Error>> {
    let namespace_id = NamespaceId::new("project-a")?;
    apply_and_commit(
        machine,
        repository,
        BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: namespace_id.clone(),
            max_namespaces: 64,
        }),
    )?;
    let group_id = ConsumerGroupId::new("engineering")?;
    apply_and_commit(
        machine,
        repository,
        BrokerCommand::EnsureConsumerGroup(EnsureConsumerGroupCommand {
            namespace_id,
            group_id: group_id.clone(),
            max_namespace_groups: 64,
        }),
    )?;
    let member_id = ConsumerId::new("worker-a")?;
    let joined = apply_and_commit(
        machine,
        repository,
        BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            capabilities: Capabilities::new(["code"])?,
            now_ms: TimestampMs::new(1_000),
            max_group_members: 256,
        }),
    )?;
    let BrokerMutationResult::ConsumerGroup(joined) = joined else {
        return Err("join result must be ConsumerGroup".into());
    };
    Ok((group_id, member_id, joined.generation))
}

fn append_durable(path: &Path, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new().append(true).open(path)?;
    file.write_all(bytes)?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

fn replace_durable(path: &Path, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new().write(true).truncate(true).open(path)?;
    file.write_all(bytes)?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

#[test]
fn fsynced_journal_reloads_leased_state_after_repository_restart() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let snapshot_path = directory.path().join("broker-state.json");
    let policy = high_threshold_policy()?;
    let mut repository = JournaledBrokerStateRepository::new(snapshot_path.clone(), None, policy);
    let mut machine = BrokerStateMachine::default();
    let (group_id, member_id, generation) = bootstrap_group(&mut machine, &mut repository)?;
    let task_id = TaskId::new("task-1")?;
    apply_and_commit(
        &mut machine,
        &mut repository,
        BrokerCommand::PublishTask(PublishTaskCommand {
            namespace_id: NamespaceId::new("project-a")?,
            task_id: task_id.clone(),
            objective: TaskObjective::new("durable across restart")?,
            created_at_ms: TimestampMs::new(1_500),
            max_namespace_tasks: 4_096,
        }),
    )?;
    apply_and_commit(
        &mut machine,
        &mut repository,
        BrokerCommand::ClaimTask(ClaimTaskCommand {
            group_id,
            member_id,
            expected_term: Term::INITIAL,
            expected_generation: generation,
            lease_id: LeaseId::new("lease-1")?,
            now_ms: TimestampMs::new(2_000),
            lease_duration_ms: 60_000,
        }),
    )?;
    let expected = machine.state().checkpoint();
    drop(machine);
    drop(repository);

    let mut restarted = JournaledBrokerStateRepository::new(snapshot_path, None, policy);
    let recovered = restarted.load()?;
    assert_eq!(recovered, expected);
    let restored_machine = BrokerStateMachine::from_checkpoint(recovered)?;
    assert!(matches!(
        restored_machine
            .state()
            .task(&task_id)
            .map(agent_broker_domain::Task::state),
        Some(TaskState::Leased(_))
    ));
    Ok(())
}

#[test]
fn compaction_persists_atomic_snapshot_then_durably_truncates_journal() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let snapshot_path = directory.path().join("broker-state.json");
    let policy = JournalCompactionPolicy::new(3, 64 * 1024 * 1024)?;
    let mut repository = JournaledBrokerStateRepository::new(snapshot_path.clone(), None, policy);
    let mut machine = BrokerStateMachine::default();
    bootstrap_group(&mut machine, &mut repository)?;

    assert!(snapshot_path.exists());
    assert_eq!(fs::metadata(repository.journal_path())?.len(), 0);
    assert!(!repository.compaction_deferred());
    let expected = machine.state().checkpoint();

    let mut restarted = JournaledBrokerStateRepository::new(snapshot_path, None, policy);
    assert_eq!(restarted.load()?, expected);
    Ok(())
}

#[test]
fn torn_final_journal_record_is_truncated_and_valid_prefix_recovers() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let snapshot_path = directory.path().join("broker-state.json");
    let policy = high_threshold_policy()?;
    let mut repository = JournaledBrokerStateRepository::new(snapshot_path.clone(), None, policy);
    let mut machine = BrokerStateMachine::default();
    apply_and_commit(
        &mut machine,
        &mut repository,
        BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: NamespaceId::new("project-a")?,
            max_namespaces: 64,
        }),
    )?;
    let expected = machine.state().checkpoint();
    let journal_path = repository.journal_path().to_path_buf();
    let valid_size = fs::metadata(&journal_path)?.len();
    append_durable(&journal_path, br#"{"schema_version":1,"term":1"#)?;
    drop(repository);

    let mut restarted = JournaledBrokerStateRepository::new(snapshot_path, None, policy);
    assert_eq!(restarted.load()?, expected);
    assert_eq!(fs::metadata(&journal_path)?.len(), valid_size);
    Ok(())
}

#[test]
fn corrupted_middle_journal_record_fails_closed_without_repair() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let snapshot_path = directory.path().join("broker-state.json");
    let policy = high_threshold_policy()?;
    let mut repository = JournaledBrokerStateRepository::new(snapshot_path.clone(), None, policy);
    let mut machine = BrokerStateMachine::default();
    apply_and_commit(
        &mut machine,
        &mut repository,
        BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: NamespaceId::new("project-a")?,
            max_namespaces: 64,
        }),
    )?;
    apply_and_commit(
        &mut machine,
        &mut repository,
        BrokerCommand::EnsureConsumerGroup(EnsureConsumerGroupCommand {
            namespace_id: NamespaceId::new("project-a")?,
            group_id: ConsumerGroupId::new("engineering")?,
            max_namespace_groups: 64,
        }),
    )?;
    let journal_path = repository.journal_path().to_path_buf();
    let original = fs::read(&journal_path)?;
    let first_end = original
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or("expected first journal newline")?
        + 1;
    let mut corrupted = Vec::new();
    corrupted.extend_from_slice(&original[..first_end]);
    corrupted.extend_from_slice(b"{broken}\n");
    corrupted.extend_from_slice(&original[first_end..]);
    replace_durable(&journal_path, &corrupted)?;
    drop(repository);

    let mut restarted = JournaledBrokerStateRepository::new(snapshot_path, None, policy);
    assert!(restarted.load().is_err());
    assert_eq!(fs::read(journal_path)?, corrupted);
    Ok(())
}

#[test]
fn syntactically_valid_but_invalid_final_record_fails_closed() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let snapshot_path = directory.path().join("broker-state.json");
    let policy = high_threshold_policy()?;
    let mut repository = JournaledBrokerStateRepository::new(snapshot_path.clone(), None, policy);
    let mut machine = BrokerStateMachine::default();
    apply_and_commit(
        &mut machine,
        &mut repository,
        BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: NamespaceId::new("project-a")?,
            max_namespaces: 64,
        }),
    )?;
    let journal_path = repository.journal_path().to_path_buf();
    append_durable(&journal_path, b"{\"schema_version\":1}\n")?;
    let corrupted_size = fs::metadata(&journal_path)?.len();
    drop(repository);

    let mut restarted = JournaledBrokerStateRepository::new(snapshot_path, None, policy);
    assert!(restarted.load().is_err());
    assert_eq!(fs::metadata(journal_path)?.len(), corrupted_size);
    Ok(())
}

#[test]
fn deferred_compaction_blocks_further_wal_growth_until_snapshot_can_commit()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let snapshot_path = directory.path().join("blocked-snapshot");
    fs::create_dir(&snapshot_path)?;
    let journal_path = directory.path().join("broker-state.journal");
    let policy = JournalCompactionPolicy::new(1, 64 * 1024 * 1024)?;
    let mut repository = JournaledBrokerStateRepository::new(
        snapshot_path.clone(),
        Some(journal_path.clone()),
        policy,
    );
    let mut machine = BrokerStateMachine::default();
    apply_and_commit(
        &mut machine,
        &mut repository,
        BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
            namespace_id: NamespaceId::new("project-a")?,
            max_namespaces: 64,
        }),
    )?;
    assert!(repository.compaction_deferred());
    let durable_prefix_size = fs::metadata(&journal_path)?.len();

    let applied = machine.apply(BrokerCommand::PublishTask(PublishTaskCommand {
        namespace_id: NamespaceId::new("project-a")?,
        task_id: TaskId::new("task-uncommitted")?,
        objective: TaskObjective::new("must not extend deferred WAL")?,
        created_at_ms: TimestampMs::new(2_000),
        max_namespace_tasks: 4_096,
    }))?;
    assert!(
        repository
            .commit(machine.state(), &applied.changes)
            .is_err()
    );
    assert_eq!(fs::metadata(&journal_path)?.len(), durable_prefix_size);

    fs::remove_dir(&snapshot_path)?;
    let mut restarted =
        JournaledBrokerStateRepository::new(snapshot_path, Some(journal_path), policy);
    let recovered = restarted.load()?;
    assert_eq!(recovered.revision.get(), 1);
    assert!(recovered.tasks.is_empty());
    Ok(())
}

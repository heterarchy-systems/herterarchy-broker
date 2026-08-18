use std::collections::BTreeMap;
use std::io;
use std::io::Cursor;
use std::sync::Arc;

use agent_broker_application::{
    BrokerError, BrokerErrorCode, BrokerErrorDisposition, CommandIdentity, CommandSequence,
    CommandSessionId, SessionOwnerEpoch, SessionOwnerInstanceId,
};
use agent_broker_domain::{BrokerCheckpoint, BrokerState, BrokerStateMachine};
use agent_broker_storage::{decode_snapshot, encode_snapshot};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership,
};
use serde::{Deserialize, Serialize};

use crate::raft_observation::RaftBrokerObservation;
use crate::raft_persistence::RedbRaftPersistence;
use crate::raft_type_config::{AgentBrokerRaftNodeId, AgentBrokerRaftTypeConfig};
use crate::{ReplicatedBrokerCommandV1, ReplicatedBrokerProposalV1, ReplicatedBrokerResponseV1};

const SNAPSHOT_META_KEY: &str = "snapshot_meta_v1";
const SNAPSHOT_DATA_KEY: &str = "snapshot_data_v1";
const CONSENSUS_SNAPSHOT_MAGIC: &[u8; 8] = b"ABCSNP01";
const CONSENSUS_SNAPSHOT_VERSION: u32 = 1;
const CONSENSUS_SNAPSHOT_HEADER_BYTES: usize = 8 + 4 + 8 + 8;
const MAX_COMMAND_SESSIONS: usize = 4_096;

#[derive(Debug, Clone, Eq, PartialEq)]
struct CommandSessionRecord {
    owner_epoch: SessionOwnerEpoch,
    owner_instance_id: Option<SessionOwnerInstanceId>,
    last_outcome: Option<CommandSessionOutcome>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CommandSessionOutcome {
    sequence: CommandSequence,
    command: ReplicatedBrokerCommandV1,
    response: ReplicatedBrokerResponseV1,
}

#[derive(Debug)]
pub(crate) struct AgentBrokerRaftStateMachine {
    persistence: RedbRaftPersistence,
    broker: BrokerStateMachine,
    command_sessions: BTreeMap<CommandSessionId, CommandSessionRecord>,
    observation: Arc<RaftBrokerObservation>,
    last_applied: Option<LogId<AgentBrokerRaftNodeId>>,
    last_membership: StoredMembership<AgentBrokerRaftNodeId, BasicNode>,
}

impl AgentBrokerRaftStateMachine {
    pub(crate) async fn load(
        persistence: RedbRaftPersistence,
        observation: Arc<RaftBrokerObservation>,
    ) -> Result<Self, StorageError<AgentBrokerRaftNodeId>> {
        let persisted = read_snapshot_pair(persistence.clone()).await?;
        if let Some((meta, data)) = persisted {
            let decoded = decode_consensus_snapshot(&data)
                .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;
            let broker = BrokerStateMachine::from_checkpoint(decoded.checkpoint)
                .map_err(|error| StorageIOError::read_state_machine(&error))?;
            observation.update(broker.state(), meta.last_log_id.map(|log_id| log_id.index));
            return Ok(Self {
                persistence,
                broker,
                command_sessions: decoded.command_sessions,
                observation,
                last_applied: meta.last_log_id,
                last_membership: meta.last_membership,
            });
        }

        let checkpoint = BrokerState::default().checkpoint();
        let broker = BrokerStateMachine::from_checkpoint(checkpoint)
            .map_err(|error| StorageIOError::read_state_machine(&error))?;
        observation.update(broker.state(), None);
        Ok(Self {
            persistence,
            broker,
            command_sessions: BTreeMap::new(),
            observation,
            last_applied: None,
            last_membership: StoredMembership::default(),
        })
    }

    fn apply_proposal(
        &mut self,
        proposal: ReplicatedBrokerProposalV1,
    ) -> io::Result<ReplicatedBrokerResponseV1> {
        let (identity, command) = proposal.into_parts().map_err(invalid_state_machine_data)?;
        match command {
            ReplicatedBrokerCommandV1::AcquireCommandSessionOwner {
                session_id,
                expected_owner_epoch,
                owner_instance_id,
            } => {
                if identity.is_some() {
                    return Err(invalid_state_machine_data(
                        "command-session owner acquisition must not carry identified mutation identity",
                    ));
                }
                self.acquire_command_session_owner(
                    session_id,
                    expected_owner_epoch,
                    owner_instance_id,
                )
            }
            command => match identity {
                Some(identity) => self.apply_identified(&identity, command),
                None => self.apply_replicated_command(command),
            },
        }
    }

    fn apply_identified(
        &mut self,
        identity: &CommandIdentity,
        command: ReplicatedBrokerCommandV1,
    ) -> io::Result<ReplicatedBrokerResponseV1> {
        if let Some(record) = self.command_sessions.get(identity.session_id()) {
            if identity.owner_epoch() != record.owner_epoch {
                return replicated_application_error(
                    BrokerErrorCode::StaleFence,
                    format!(
                        "command session {} owner epoch {} does not match broker-authoritative epoch {}",
                        identity.session_id(),
                        identity.owner_epoch().get(),
                        record.owner_epoch.get()
                    ),
                );
            }
            if identity.owner_instance_id() != record.owner_instance_id.as_ref() {
                return replicated_application_error(
                    BrokerErrorCode::StaleFence,
                    format!(
                        "command session {} owner instance does not match broker-authoritative owner instance",
                        identity.session_id()
                    ),
                );
            }
            if let Some(last_outcome) = &record.last_outcome {
                if identity.sequence() < last_outcome.sequence {
                    return replicated_application_error(
                        BrokerErrorCode::StaleFence,
                        format!(
                            "command_sequence {} is older than committed sequence {} for session {}",
                            identity.sequence().get(),
                            last_outcome.sequence.get(),
                            identity.session_id()
                        ),
                    );
                }
                if identity.sequence() == last_outcome.sequence {
                    if command.same_identified_request(&last_outcome.command) {
                        return Ok(last_outcome.response.clone());
                    }
                    return replicated_application_error(
                        BrokerErrorCode::Conflict,
                        format!(
                            "command_sequence {} for session {} is already committed with different command content",
                            identity.sequence().get(),
                            identity.session_id()
                        ),
                    );
                }
            } else if identity.sequence().get() != 1 {
                return replicated_application_error(
                    BrokerErrorCode::StaleFence,
                    format!(
                        "new owner epoch {} for session {} must restart command_sequence at 1, got {}",
                        identity.owner_epoch().get(),
                        identity.session_id(),
                        identity.sequence().get(),
                    ),
                );
            }
        } else {
            if identity.owner_epoch() != SessionOwnerEpoch::INITIAL {
                return replicated_application_error(
                    BrokerErrorCode::StaleFence,
                    format!(
                        "new command session {} must begin at broker-authoritative owner epoch 1, got {}",
                        identity.session_id(),
                        identity.owner_epoch().get()
                    ),
                );
            }
            if identity.owner_instance_id().is_some() {
                return replicated_application_error(
                    BrokerErrorCode::StaleFence,
                    format!(
                        "new command session {} cannot self-authorize an owner instance before broker acquisition",
                        identity.session_id()
                    ),
                );
            }
            if self.command_sessions.len() >= MAX_COMMAND_SESSIONS {
                return replicated_application_error(
                    BrokerErrorCode::CapacityExceeded,
                    format!(
                        "command session capacity reached: max {MAX_COMMAND_SESSIONS} active sessions"
                    ),
                );
            }
        }

        let response = self.apply_replicated_command(command.clone())?;
        self.command_sessions.insert(
            identity.session_id().clone(),
            CommandSessionRecord {
                owner_epoch: identity.owner_epoch(),
                owner_instance_id: identity.owner_instance_id().cloned(),
                last_outcome: Some(CommandSessionOutcome {
                    sequence: identity.sequence(),
                    command,
                    response: response.clone(),
                }),
            },
        );
        Ok(response)
    }

    fn acquire_command_session_owner(
        &mut self,
        session_id: String,
        expected_owner_epoch: u64,
        owner_instance_id: String,
    ) -> io::Result<ReplicatedBrokerResponseV1> {
        let session_id = CommandSessionId::new(session_id).map_err(invalid_state_machine_data)?;
        let expected_owner_epoch =
            SessionOwnerEpoch::new(expected_owner_epoch).map_err(invalid_state_machine_data)?;
        let owner_instance_id =
            SessionOwnerInstanceId::new(owner_instance_id).map_err(invalid_state_machine_data)?;
        let session_capacity_reached = self.command_sessions.len() >= MAX_COMMAND_SESSIONS;
        let record = match self.command_sessions.entry(session_id.clone()) {
            std::collections::btree_map::Entry::Vacant(vacant) => {
                if expected_owner_epoch != SessionOwnerEpoch::INITIAL {
                    return replicated_application_error(
                        BrokerErrorCode::StaleFence,
                        format!(
                            "new command session {} must begin at broker-authoritative owner epoch 1, got {}",
                            vacant.key(),
                            expected_owner_epoch.get()
                        ),
                    );
                }
                if session_capacity_reached {
                    return replicated_application_error(
                        BrokerErrorCode::CapacityExceeded,
                        format!(
                            "command session capacity reached: max {MAX_COMMAND_SESSIONS} active sessions"
                        ),
                    );
                }
                vacant.insert(CommandSessionRecord {
                    owner_epoch: SessionOwnerEpoch::INITIAL,
                    owner_instance_id: Some(owner_instance_id),
                    last_outcome: None,
                });
                return Ok(ReplicatedBrokerResponseV1::SessionOwnerAcquired {
                    owner_epoch: SessionOwnerEpoch::INITIAL.get(),
                });
            }
            std::collections::btree_map::Entry::Occupied(occupied) => occupied.into_mut(),
        };
        if record.owner_instance_id.as_ref() == Some(&owner_instance_id) {
            let current_epoch = record.owner_epoch.get();
            let retry_epoch = expected_owner_epoch.get().checked_add(1);
            if current_epoch == expected_owner_epoch.get() || retry_epoch == Some(current_epoch) {
                return Ok(ReplicatedBrokerResponseV1::SessionOwnerAcquired {
                    owner_epoch: current_epoch,
                });
            }
        }
        if record.owner_epoch != expected_owner_epoch {
            return replicated_application_error(
                BrokerErrorCode::StaleFence,
                format!(
                    "command session {session_id} expected owner epoch {} but broker-authoritative epoch is {}",
                    expected_owner_epoch.get(),
                    record.owner_epoch.get()
                ),
            );
        }
        let Some(next_owner_epoch) = record.owner_epoch.get().checked_add(1) else {
            return replicated_application_error(
                BrokerErrorCode::CapacityExceeded,
                format!("command session {session_id} owner epoch is exhausted"),
            );
        };
        let next_owner_epoch =
            SessionOwnerEpoch::new(next_owner_epoch).map_err(invalid_state_machine_data)?;
        record.owner_epoch = next_owner_epoch;
        record.owner_instance_id = Some(owner_instance_id);
        record.last_outcome = None;
        Ok(ReplicatedBrokerResponseV1::SessionOwnerAcquired {
            owner_epoch: next_owner_epoch.get(),
        })
    }

    fn apply_replicated_command(
        &mut self,
        command: ReplicatedBrokerCommandV1,
    ) -> io::Result<ReplicatedBrokerResponseV1> {
        let command = agent_broker_domain::commands::BrokerCommand::try_from(command)
            .map_err(invalid_state_machine_data)?;
        let application_result = self
            .broker
            .apply(command)
            .map(|applied| applied.result)
            .map_err(BrokerError::from);
        ReplicatedBrokerResponseV1::from_application_result(application_result)
            .map_err(invalid_state_machine_data)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AgentBrokerRaftSnapshotBuilder {
    persistence: RedbRaftPersistence,
    checkpoint: BrokerCheckpoint,
    command_sessions: BTreeMap<CommandSessionId, CommandSessionRecord>,
    last_applied: Option<LogId<AgentBrokerRaftNodeId>>,
    last_membership: StoredMembership<AgentBrokerRaftNodeId, BasicNode>,
}

impl RaftSnapshotBuilder<AgentBrokerRaftTypeConfig> for AgentBrokerRaftSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<AgentBrokerRaftTypeConfig>, StorageError<AgentBrokerRaftNodeId>> {
        let data = encode_consensus_snapshot(&self.checkpoint, &self.command_sessions)
            .map_err(|error| StorageIOError::read_state_machine(&error))?;
        let snapshot_id = snapshot_id(self.last_applied, self.checkpoint.revision.get());
        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };
        persist_snapshot(self.persistence.clone(), &meta, &data).await?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<AgentBrokerRaftTypeConfig> for AgentBrokerRaftStateMachine {
    type SnapshotBuilder = AgentBrokerRaftSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<AgentBrokerRaftNodeId>>,
            StoredMembership<AgentBrokerRaftNodeId, BasicNode>,
        ),
        StorageError<AgentBrokerRaftNodeId>,
    > {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ReplicatedBrokerResponseV1>, StorageError<AgentBrokerRaftNodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<AgentBrokerRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            let response = match entry.payload {
                EntryPayload::Blank => ReplicatedBrokerResponseV1::RaftControl,
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembership::new(Some(log_id), membership);
                    ReplicatedBrokerResponseV1::RaftControl
                }
                EntryPayload::Normal(data) => self
                    .apply_proposal(data)
                    .map_err(|error| StorageIOError::read_state_machine(&error))?,
            };
            self.last_applied = Some(log_id);
            self.observation.update(
                self.broker.state(),
                self.last_applied.map(|applied| applied.index),
            );
            responses.push(response);
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        AgentBrokerRaftSnapshotBuilder {
            persistence: self.persistence.clone(),
            checkpoint: self.broker.state().checkpoint(),
            command_sessions: self.command_sessions.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<AgentBrokerRaftNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<AgentBrokerRaftNodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<AgentBrokerRaftNodeId>> {
        let data = snapshot.into_inner();
        let decoded = decode_consensus_snapshot(&data)
            .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;
        let replacement = BrokerStateMachine::from_checkpoint(decoded.checkpoint)
            .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;

        persist_snapshot(self.persistence.clone(), meta, &data).await?;

        self.broker = replacement;
        self.command_sessions = decoded.command_sessions;
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.observation.update(
            self.broker.state(),
            self.last_applied.map(|applied| applied.index),
        );
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<AgentBrokerRaftTypeConfig>>, StorageError<AgentBrokerRaftNodeId>>
    {
        let Some((meta, data)) = read_snapshot_pair(self.persistence.clone()).await? else {
            return Ok(None);
        };
        // Read-back validates both snapshot bytes and Broker checkpoint invariants before exposing it
        // to OpenRaft. A corrupted durable snapshot is fatal rather than silently skipped.
        let _decoded = decode_consensus_snapshot(&data)
            .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

async fn persist_snapshot(
    persistence: RedbRaftPersistence,
    meta: &SnapshotMeta<AgentBrokerRaftNodeId, BasicNode>,
    data: &[u8],
) -> Result<(), StorageError<AgentBrokerRaftNodeId>> {
    let encoded_meta = serde_json::to_vec(meta)
        .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
    let snapshot_data = data.to_vec();
    let signature = meta.signature();
    run_persistence_io(move || {
        persistence.write_meta_pair(
            SNAPSHOT_META_KEY,
            &encoded_meta,
            SNAPSHOT_DATA_KEY,
            &snapshot_data,
        )
    })
    .await
    .map_err(|error| StorageIOError::write_snapshot(Some(signature), &error).into())
}

async fn read_snapshot_pair(
    persistence: RedbRaftPersistence,
) -> Result<
    Option<(SnapshotMeta<AgentBrokerRaftNodeId, BasicNode>, Vec<u8>)>,
    StorageError<AgentBrokerRaftNodeId>,
> {
    let (encoded_meta, data) = run_persistence_io(move || {
        persistence.read_meta_pair(SNAPSHOT_META_KEY, SNAPSHOT_DATA_KEY)
    })
    .await
    .map_err(|error| StorageIOError::read(&error))?;

    match (encoded_meta, data) {
        (None, None) => Ok(None),
        (Some(encoded_meta), Some(data)) => {
            let meta = serde_json::from_slice(&encoded_meta)
                .map_err(|error| StorageIOError::read(&error))?;
            Ok(Some((meta, data)))
        }
        _ => Err(StorageIOError::read(&io::Error::new(
            io::ErrorKind::InvalidData,
            "persisted Raft snapshot metadata/data pair is incomplete",
        ))
        .into()),
    }
}

#[derive(Debug)]
struct DecodedConsensusSnapshot {
    checkpoint: BrokerCheckpoint,
    command_sessions: BTreeMap<CommandSessionId, CommandSessionRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandSessionsSnapshotV1 {
    version: u32,
    records: Vec<CommandSessionSnapshotRecordV1>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandSessionSnapshotRecordV1 {
    session_id: String,
    #[serde(default = "initial_owner_epoch_u64")]
    owner_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner_instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    command: Option<ReplicatedBrokerCommandV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response: Option<ReplicatedBrokerResponseV1>,
}

fn encode_consensus_snapshot(
    checkpoint: &BrokerCheckpoint,
    command_sessions: &BTreeMap<CommandSessionId, CommandSessionRecord>,
) -> io::Result<Vec<u8>> {
    let broker_snapshot = encode_snapshot(checkpoint).map_err(invalid_snapshot_data)?;
    let sidecar = CommandSessionsSnapshotV1 {
        version: CONSENSUS_SNAPSHOT_VERSION,
        records: command_sessions
            .iter()
            .map(|(session_id, record)| {
                let (sequence, command, response) =
                    record
                        .last_outcome
                        .as_ref()
                        .map_or((None, None, None), |outcome| {
                            (
                                Some(outcome.sequence.get()),
                                Some(outcome.command.clone()),
                                Some(outcome.response.clone()),
                            )
                        });
                CommandSessionSnapshotRecordV1 {
                    session_id: session_id.as_str().to_owned(),
                    owner_epoch: record.owner_epoch.get(),
                    owner_instance_id: record
                        .owner_instance_id
                        .as_ref()
                        .map(|owner_instance_id| owner_instance_id.as_str().to_owned()),
                    sequence,
                    command,
                    response,
                }
            })
            .collect(),
    };
    let encoded_sidecar = serde_json::to_vec(&sidecar).map_err(invalid_snapshot_data)?;
    let broker_len = u64::try_from(broker_snapshot.len())
        .map_err(|_| invalid_snapshot_data("Broker snapshot length exceeds u64"))?;
    let sidecar_len = u64::try_from(encoded_sidecar.len())
        .map_err(|_| invalid_snapshot_data("command session snapshot length exceeds u64"))?;
    let capacity = CONSENSUS_SNAPSHOT_MAGIC
        .len()
        .saturating_add(4)
        .saturating_add(8)
        .saturating_add(8)
        .saturating_add(broker_snapshot.len())
        .saturating_add(encoded_sidecar.len());
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(CONSENSUS_SNAPSHOT_MAGIC);
    output.extend_from_slice(&CONSENSUS_SNAPSHOT_VERSION.to_be_bytes());
    output.extend_from_slice(&broker_len.to_be_bytes());
    output.extend_from_slice(&sidecar_len.to_be_bytes());
    output.extend_from_slice(&broker_snapshot);
    output.extend_from_slice(&encoded_sidecar);
    Ok(output)
}

fn decode_consensus_snapshot(data: &[u8]) -> io::Result<DecodedConsensusSnapshot> {
    if !data.starts_with(CONSENSUS_SNAPSHOT_MAGIC) {
        return Ok(DecodedConsensusSnapshot {
            checkpoint: decode_checkpoint(data)?,
            command_sessions: BTreeMap::new(),
        });
    }
    if data.len() < CONSENSUS_SNAPSHOT_HEADER_BYTES {
        return Err(invalid_snapshot_data(
            "consensus snapshot header is truncated",
        ));
    }
    let version = u32::from_be_bytes(
        data[8..12]
            .try_into()
            .map_err(|_| invalid_snapshot_data("invalid consensus snapshot version bytes"))?,
    );
    if version != CONSENSUS_SNAPSHOT_VERSION {
        return Err(invalid_snapshot_data(format!(
            "unsupported consensus snapshot version {version}"
        )));
    }
    let broker_len =
        usize::try_from(u64::from_be_bytes(data[12..20].try_into().map_err(
            |_| invalid_snapshot_data("invalid Broker snapshot length bytes"),
        )?))
        .map_err(|_| invalid_snapshot_data("Broker snapshot length exceeds platform usize"))?;
    let sidecar_len =
        usize::try_from(u64::from_be_bytes(data[20..28].try_into().map_err(
            |_| invalid_snapshot_data("invalid command session length bytes"),
        )?))
        .map_err(|_| invalid_snapshot_data("command session length exceeds platform usize"))?;
    let broker_end = CONSENSUS_SNAPSHOT_HEADER_BYTES
        .checked_add(broker_len)
        .ok_or_else(|| invalid_snapshot_data("consensus snapshot Broker length overflow"))?;
    let expected_end = broker_end
        .checked_add(sidecar_len)
        .ok_or_else(|| invalid_snapshot_data("consensus snapshot sidecar length overflow"))?;
    if expected_end != data.len() {
        return Err(invalid_snapshot_data(
            "consensus snapshot length fields do not match payload length",
        ));
    }
    let checkpoint = decode_checkpoint(&data[CONSENSUS_SNAPSHOT_HEADER_BYTES..broker_end])?;
    let sidecar: CommandSessionsSnapshotV1 =
        serde_json::from_slice(&data[broker_end..]).map_err(invalid_snapshot_data)?;
    if sidecar.version != CONSENSUS_SNAPSHOT_VERSION {
        return Err(invalid_snapshot_data(format!(
            "unsupported command session snapshot version {}",
            sidecar.version
        )));
    }
    if sidecar.records.len() > MAX_COMMAND_SESSIONS {
        return Err(invalid_snapshot_data(format!(
            "command session snapshot exceeds capacity {MAX_COMMAND_SESSIONS}"
        )));
    }
    let command_sessions = decode_command_session_records(sidecar.records)?;
    Ok(DecodedConsensusSnapshot {
        checkpoint,
        command_sessions,
    })
}

fn decode_command_session_records(
    records: Vec<CommandSessionSnapshotRecordV1>,
) -> io::Result<BTreeMap<CommandSessionId, CommandSessionRecord>> {
    let mut command_sessions = BTreeMap::new();
    for record in records {
        let CommandSessionSnapshotRecordV1 {
            session_id,
            owner_epoch,
            owner_instance_id,
            sequence,
            command,
            response,
        } = record;
        let session_id = CommandSessionId::new(session_id).map_err(invalid_snapshot_data)?;
        let owner_epoch = SessionOwnerEpoch::new(owner_epoch).map_err(invalid_snapshot_data)?;
        let owner_instance_id = owner_instance_id
            .map(SessionOwnerInstanceId::new)
            .transpose()
            .map_err(invalid_snapshot_data)?;
        let last_outcome = decode_command_session_outcome(sequence, command, response)?;
        if command_sessions
            .insert(
                session_id,
                CommandSessionRecord {
                    owner_epoch,
                    owner_instance_id,
                    last_outcome,
                },
            )
            .is_some()
        {
            return Err(invalid_snapshot_data(
                "command session snapshot contains duplicate session_id",
            ));
        }
    }
    Ok(command_sessions)
}

fn decode_command_session_outcome(
    sequence: Option<u64>,
    command: Option<ReplicatedBrokerCommandV1>,
    response: Option<ReplicatedBrokerResponseV1>,
) -> io::Result<Option<CommandSessionOutcome>> {
    match (sequence, command, response) {
        (None, None, None) => Ok(None),
        (Some(sequence), Some(command), Some(response)) => {
            let sequence = CommandSequence::new(sequence).map_err(invalid_snapshot_data)?;
            let _validated_command =
                agent_broker_domain::commands::BrokerCommand::try_from(command.clone())
                    .map_err(invalid_snapshot_data)?;
            let response = response.normalize_stored_session_outcome();
            let _validated_response = response
                .clone()
                .into_application_result()
                .map_err(invalid_snapshot_data)?;
            Ok(Some(CommandSessionOutcome {
                sequence,
                command,
                response,
            }))
        }
        _ => Err(invalid_snapshot_data(
            "command session snapshot outcome fields must be all present or all absent",
        )),
    }
}

const fn initial_owner_epoch_u64() -> u64 {
    1
}

fn decode_checkpoint(data: &[u8]) -> io::Result<BrokerCheckpoint> {
    let checkpoint = decode_snapshot(data).map_err(invalid_snapshot_data)?;
    BrokerStateMachine::from_checkpoint(checkpoint.clone()).map_err(invalid_snapshot_data)?;
    Ok(checkpoint)
}

fn replicated_application_error(
    code: BrokerErrorCode,
    message: impl Into<String>,
) -> io::Result<ReplicatedBrokerResponseV1> {
    let error = BrokerError::new(code, message).with_disposition(BrokerErrorDisposition::Rejected);
    ReplicatedBrokerResponseV1::from_pre_application_rejection(error)
        .map_err(invalid_state_machine_data)
}

fn invalid_snapshot_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn invalid_state_machine_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn snapshot_id(last_applied: Option<LogId<AgentBrokerRaftNodeId>>, revision: u64) -> String {
    match last_applied {
        Some(log_id) => format!(
            "agent-broker-{}-{}-{revision}",
            log_id.leader_id, log_id.index
        ),
        None => format!("agent-broker-empty-{revision}"),
    }
}

async fn run_persistence_io<T, F>(operation: F) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(io::Error::other)?
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io::Cursor;
    use std::sync::Arc;

    use agent_broker_application::{
        BrokerErrorCode, CommandIdentity, CommandSequence, CommandSessionId, SessionOwnerEpoch,
        SessionOwnerInstanceId,
    };
    use agent_broker_domain::commands::{BrokerCommand, EnsureNamespaceCommand};
    use agent_broker_domain::{BrokerState, NamespaceId};
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
    use openraft::{SnapshotMeta, StoredMembership};
    use tempfile::tempdir;

    use super::{AgentBrokerRaftStateMachine, read_snapshot_pair};
    use crate::raft_observation::RaftBrokerObservation;
    use crate::raft_persistence::RedbRaftPersistence;
    use crate::{ReplicatedBrokerCommandV1, ReplicatedBrokerProposalV1};

    #[tokio::test]
    async fn identified_command_retry_returns_exact_committed_response_without_reapply()
    -> Result<(), Box<dyn Error>> {
        let directory = tempdir()?;
        let persistence = RedbRaftPersistence::open(directory.path().join("raft.redb"))?;
        let observation = Arc::new(RaftBrokerObservation::new(&BrokerState::default(), None));
        let mut state_machine =
            AgentBrokerRaftStateMachine::load(persistence, Arc::clone(&observation)).await?;
        let identity =
            CommandIdentity::new(CommandSessionId::new("client-a")?, CommandSequence::new(1)?);
        let command = ReplicatedBrokerCommandV1::try_from(BrokerCommand::EnsureNamespace(
            EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("identified-a")?,
                max_namespaces: 64,
            },
        ))?;
        let proposal = ReplicatedBrokerProposalV1::identified(&identity, command.clone());

        let first = state_machine.apply_proposal(proposal.clone())?;
        let revision_after_first = state_machine.broker.state().revision();
        let retry = state_machine.apply_proposal(proposal)?;

        assert_eq!(retry, first);
        assert_eq!(
            state_machine.broker.state().revision(),
            revision_after_first
        );

        let conflicting_command = ReplicatedBrokerCommandV1::try_from(
            BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("identified-conflict")?,
                max_namespaces: 64,
            }),
        )?;
        let conflict_result = state_machine
            .apply_proposal(ReplicatedBrokerProposalV1::identified(
                &identity,
                conflicting_command,
            ))?
            .into_application_result()?;
        let conflict = match conflict_result {
            Ok(result) => {
                return Err(format!(
                    "same sequence with different command unexpectedly succeeded: {result:?}"
                )
                .into());
            }
            Err(error) => error,
        };
        assert_eq!(
            conflict.code(),
            agent_broker_application::BrokerErrorCode::Conflict
        );
        assert_eq!(
            state_machine.broker.state().revision(),
            revision_after_first
        );
        Ok(())
    }

    #[tokio::test]
    async fn new_session_owner_acquisition_bootstraps_epoch_one_without_business_mutation()
    -> Result<(), Box<dyn Error>> {
        let directory = tempdir()?;
        let persistence = RedbRaftPersistence::open(directory.path().join("raft.redb"))?;
        let observation = Arc::new(RaftBrokerObservation::new(&BrokerState::default(), None));
        let mut state_machine =
            AgentBrokerRaftStateMachine::load(persistence, Arc::clone(&observation)).await?;
        let session_id = CommandSessionId::new("owner-bootstrap-client")?;
        let owner_instance = SessionOwnerInstanceId::new("owner-bootstrap-process")?;
        let revision_before = state_machine.broker.state().revision();
        let acquisition = ReplicatedBrokerProposalV1::legacy(
            ReplicatedBrokerCommandV1::AcquireCommandSessionOwner {
                session_id: session_id.as_str().to_owned(),
                expected_owner_epoch: SessionOwnerEpoch::INITIAL.get(),
                owner_instance_id: owner_instance.as_str().to_owned(),
            },
        );

        let first = state_machine.apply_proposal(acquisition.clone())?;
        let retry = state_machine.apply_proposal(acquisition)?;
        assert_eq!(first, retry);
        let owner_epoch = match first {
            crate::ReplicatedBrokerResponseV1::SessionOwnerAcquired { owner_epoch } => {
                SessionOwnerEpoch::new(owner_epoch)?
            }
            other => return Err(format!("unexpected owner bootstrap response: {other:?}").into()),
        };
        assert_eq!(owner_epoch, SessionOwnerEpoch::INITIAL);
        assert_eq!(state_machine.broker.state().revision(), revision_before);

        let owned_identity = CommandIdentity::new_with_owner(
            session_id.clone(),
            owner_epoch,
            owner_instance,
            CommandSequence::new(1)?,
        );
        let owned_command = ReplicatedBrokerCommandV1::try_from(BrokerCommand::EnsureNamespace(
            EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("owner-bootstrap-namespace")?,
                max_namespaces: 64,
            },
        ))?;
        let committed = state_machine
            .apply_proposal(ReplicatedBrokerProposalV1::identified(
                &owned_identity,
                owned_command,
            ))?
            .into_application_result()?;
        if let Err(error) = committed {
            return Err(format!("new owner epoch-one mutation failed: {error}").into());
        }
        assert_eq!(
            state_machine.broker.state().revision().get(),
            revision_before.get() + 1
        );

        let ownerless_identity = CommandIdentity::new_with_owner_epoch(
            session_id,
            owner_epoch,
            CommandSequence::new(2)?,
        );
        let ownerless_command = ReplicatedBrokerCommandV1::try_from(
            BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("owner-bootstrap-ownerless")?,
                max_namespaces: 64,
            }),
        )?;
        let ownerless = state_machine
            .apply_proposal(ReplicatedBrokerProposalV1::identified(
                &ownerless_identity,
                ownerless_command,
            ))?
            .into_application_result()?;
        let ownerless_error = match ownerless {
            Ok(result) => {
                return Err(
                    format!("ownerless mutation unexpectedly succeeded: {result:?}").into(),
                );
            }
            Err(error) => error,
        };
        assert_eq!(ownerless_error.code(), BrokerErrorCode::StaleFence);
        Ok(())
    }

    #[tokio::test]
    async fn session_owner_acquisition_fences_old_owner_and_resets_sequence()
    -> Result<(), Box<dyn Error>> {
        let directory = tempdir()?;
        let persistence = RedbRaftPersistence::open(directory.path().join("raft.redb"))?;
        let observation = Arc::new(RaftBrokerObservation::new(&BrokerState::default(), None));
        let mut state_machine =
            AgentBrokerRaftStateMachine::load(persistence, Arc::clone(&observation)).await?;
        let session_id = CommandSessionId::new("owner-fencing-client")?;
        let epoch_one = SessionOwnerEpoch::INITIAL;
        let first_identity = CommandIdentity::new_with_owner_epoch(
            session_id.clone(),
            epoch_one,
            CommandSequence::new(1)?,
        );
        let first_command = ReplicatedBrokerCommandV1::try_from(BrokerCommand::EnsureNamespace(
            EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("owner-epoch-one")?,
                max_namespaces: 64,
            },
        ))?;
        let _first = state_machine.apply_proposal(ReplicatedBrokerProposalV1::identified(
            &first_identity,
            first_command,
        ))?;
        let revision_after_first = state_machine.broker.state().revision();
        let owner_instance = SessionOwnerInstanceId::new("owner-process-a")?;

        let owner_acquisition =
            state_machine.apply_proposal(ReplicatedBrokerProposalV1::legacy(
                ReplicatedBrokerCommandV1::AcquireCommandSessionOwner {
                    session_id: session_id.as_str().to_owned(),
                    expected_owner_epoch: epoch_one.get(),
                    owner_instance_id: owner_instance.as_str().to_owned(),
                },
            ))?;
        let epoch_two = match owner_acquisition {
            crate::ReplicatedBrokerResponseV1::SessionOwnerAcquired { owner_epoch } => {
                SessionOwnerEpoch::new(owner_epoch)?
            }
            other => return Err(format!("unexpected owner acquisition response: {other:?}").into()),
        };
        assert_eq!(epoch_two.get(), 2);
        assert_eq!(
            state_machine.broker.state().revision(),
            revision_after_first
        );

        let stale_identity = CommandIdentity::new_with_owner_epoch(
            session_id.clone(),
            epoch_one,
            CommandSequence::new(2)?,
        );
        let stale_command = ReplicatedBrokerCommandV1::try_from(BrokerCommand::EnsureNamespace(
            EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("stale-owner-must-not-apply")?,
                max_namespaces: 64,
            },
        ))?;
        let stale = state_machine
            .apply_proposal(ReplicatedBrokerProposalV1::identified(
                &stale_identity,
                stale_command,
            ))?
            .into_application_result()?;
        let stale_error = match stale {
            Ok(result) => {
                return Err(format!("stale owner unexpectedly succeeded: {result:?}").into());
            }
            Err(error) => error,
        };
        assert_eq!(stale_error.code(), BrokerErrorCode::StaleFence);
        assert_eq!(
            state_machine.broker.state().revision(),
            revision_after_first
        );

        let epoch_two_identity = CommandIdentity::new_with_owner(
            session_id,
            epoch_two,
            owner_instance,
            CommandSequence::new(1)?,
        );
        let epoch_two_command = ReplicatedBrokerCommandV1::try_from(
            BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("owner-epoch-two")?,
                max_namespaces: 64,
            }),
        )?;
        let epoch_two_proposal =
            ReplicatedBrokerProposalV1::identified(&epoch_two_identity, epoch_two_command);
        let committed = state_machine.apply_proposal(epoch_two_proposal.clone())?;
        let revision_after_epoch_two = state_machine.broker.state().revision();
        let retry = state_machine.apply_proposal(epoch_two_proposal)?;
        assert_eq!(retry, committed);
        assert_eq!(
            state_machine.broker.state().revision(),
            revision_after_epoch_two
        );
        Ok(())
    }

    #[tokio::test]
    async fn session_owner_acquisition_retry_is_idempotent_and_stale_contender_is_fenced()
    -> Result<(), Box<dyn Error>> {
        let directory = tempdir()?;
        let persistence = RedbRaftPersistence::open(directory.path().join("raft.redb"))?;
        let observation = Arc::new(RaftBrokerObservation::new(&BrokerState::default(), None));
        let mut state_machine =
            AgentBrokerRaftStateMachine::load(persistence, Arc::clone(&observation)).await?;
        let session_id = CommandSessionId::new("owner-acquisition-retry")?;
        let epoch_one = SessionOwnerEpoch::INITIAL;
        let initial_identity = CommandIdentity::new(session_id.clone(), CommandSequence::new(1)?);
        let initial_command = ReplicatedBrokerCommandV1::try_from(BrokerCommand::EnsureNamespace(
            EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("owner-acquisition-seed")?,
                max_namespaces: 64,
            },
        ))?;
        let _initial = state_machine.apply_proposal(ReplicatedBrokerProposalV1::identified(
            &initial_identity,
            initial_command,
        ))?;
        let revision_before = state_machine.broker.state().revision();
        let owner_a = SessionOwnerInstanceId::new("owner-instance-a")?;

        let acquire = || {
            ReplicatedBrokerProposalV1::legacy(
                ReplicatedBrokerCommandV1::AcquireCommandSessionOwner {
                    session_id: session_id.as_str().to_owned(),
                    expected_owner_epoch: epoch_one.get(),
                    owner_instance_id: owner_a.as_str().to_owned(),
                },
            )
        };
        let first = state_machine.apply_proposal(acquire())?;
        let retry = state_machine.apply_proposal(acquire())?;
        assert_eq!(first, retry);
        let epoch_two = match first {
            crate::ReplicatedBrokerResponseV1::SessionOwnerAcquired { owner_epoch } => {
                SessionOwnerEpoch::new(owner_epoch)?
            }
            other => return Err(format!("unexpected owner acquisition response: {other:?}").into()),
        };
        assert_eq!(epoch_two.get(), 2);
        assert_eq!(state_machine.broker.state().revision(), revision_before);

        let owner_b = SessionOwnerInstanceId::new("owner-instance-b")?;
        let forged_owner_identity = CommandIdentity::new_with_owner(
            session_id.clone(),
            epoch_two,
            owner_b.clone(),
            CommandSequence::new(1)?,
        );
        let forged_owner_command = ReplicatedBrokerCommandV1::try_from(
            BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("forged-same-epoch-owner")?,
                max_namespaces: 64,
            }),
        )?;
        assert_stale_identified(
            &mut state_machine,
            &forged_owner_identity,
            forged_owner_command,
        )?;
        let stale_contender = state_machine
            .apply_proposal(ReplicatedBrokerProposalV1::legacy(
                ReplicatedBrokerCommandV1::AcquireCommandSessionOwner {
                    session_id: session_id.as_str().to_owned(),
                    expected_owner_epoch: epoch_one.get(),
                    owner_instance_id: owner_b.as_str().to_owned(),
                },
            ))?
            .into_application_result()?;
        let stale_error = match stale_contender {
            Ok(result) => {
                return Err(
                    format!("stale competing owner unexpectedly succeeded: {result:?}").into(),
                );
            }
            Err(error) => error,
        };
        assert_eq!(stale_error.code(), BrokerErrorCode::StaleFence);
        assert_eq!(state_machine.broker.state().revision(), revision_before);

        let owner_b_acquired = state_machine.apply_proposal(ReplicatedBrokerProposalV1::legacy(
            ReplicatedBrokerCommandV1::AcquireCommandSessionOwner {
                session_id: session_id.as_str().to_owned(),
                expected_owner_epoch: epoch_two.get(),
                owner_instance_id: owner_b.as_str().to_owned(),
            },
        ))?;
        let epoch_three = match owner_b_acquired {
            crate::ReplicatedBrokerResponseV1::SessionOwnerAcquired { owner_epoch } => {
                SessionOwnerEpoch::new(owner_epoch)?
            }
            other => return Err(format!("unexpected second owner acquisition: {other:?}").into()),
        };
        assert_eq!(epoch_three.get(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn session_owner_epoch_survives_snapshot_and_keeps_old_owner_fenced()
    -> Result<(), Box<dyn Error>> {
        let directory = tempdir()?;
        let persistence = RedbRaftPersistence::open(directory.path().join("raft.redb"))?;
        let observation = Arc::new(RaftBrokerObservation::new(&BrokerState::default(), None));
        let mut state_machine =
            AgentBrokerRaftStateMachine::load(persistence.clone(), Arc::clone(&observation))
                .await?;
        let session_id = CommandSessionId::new("owner-snapshot-client")?;
        let epoch_one = SessionOwnerEpoch::INITIAL;
        let first_identity = CommandIdentity::new(session_id.clone(), CommandSequence::new(1)?);
        let first_command = ReplicatedBrokerCommandV1::try_from(BrokerCommand::EnsureNamespace(
            EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("owner-snapshot-one")?,
                max_namespaces: 64,
            },
        ))?;
        let _first = state_machine.apply_proposal(ReplicatedBrokerProposalV1::identified(
            &first_identity,
            first_command,
        ))?;
        let owner_instance = SessionOwnerInstanceId::new("snapshot-owner-process")?;
        let owner_acquisition =
            state_machine.apply_proposal(ReplicatedBrokerProposalV1::legacy(
                ReplicatedBrokerCommandV1::AcquireCommandSessionOwner {
                    session_id: session_id.as_str().to_owned(),
                    expected_owner_epoch: epoch_one.get(),
                    owner_instance_id: owner_instance.as_str().to_owned(),
                },
            ))?;
        let epoch_two = match owner_acquisition {
            crate::ReplicatedBrokerResponseV1::SessionOwnerAcquired { owner_epoch } => {
                SessionOwnerEpoch::new(owner_epoch)?
            }
            other => return Err(format!("unexpected owner acquisition response: {other:?}").into()),
        };
        let epoch_two_identity = CommandIdentity::new_with_owner(
            session_id.clone(),
            epoch_two,
            owner_instance,
            CommandSequence::new(1)?,
        );
        let epoch_two_command = ReplicatedBrokerCommandV1::try_from(
            BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("owner-snapshot-two")?,
                max_namespaces: 64,
            }),
        )?;
        let epoch_two_proposal =
            ReplicatedBrokerProposalV1::identified(&epoch_two_identity, epoch_two_command);
        let committed = state_machine.apply_proposal(epoch_two_proposal.clone())?;
        let mut builder = state_machine.get_snapshot_builder().await;
        let _snapshot = builder.build_snapshot().await?;
        drop(state_machine);

        let recovered_observation =
            Arc::new(RaftBrokerObservation::new(&BrokerState::default(), None));
        let mut recovered =
            AgentBrokerRaftStateMachine::load(persistence, Arc::clone(&recovered_observation))
                .await?;
        let stale_identity =
            CommandIdentity::new_with_owner_epoch(session_id, epoch_one, CommandSequence::new(2)?);
        let stale_command = ReplicatedBrokerCommandV1::try_from(BrokerCommand::EnsureNamespace(
            EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("owner-snapshot-stale")?,
                max_namespaces: 64,
            },
        ))?;
        let stale = recovered
            .apply_proposal(ReplicatedBrokerProposalV1::identified(
                &stale_identity,
                stale_command,
            ))?
            .into_application_result()?;
        let stale_error = match stale {
            Ok(result) => {
                return Err(
                    format!("recovered stale owner unexpectedly succeeded: {result:?}").into(),
                );
            }
            Err(error) => error,
        };
        assert_eq!(stale_error.code(), BrokerErrorCode::StaleFence);
        let retry = recovered.apply_proposal(epoch_two_proposal)?;
        assert_eq!(retry, committed);
        Ok(())
    }

    fn assert_stale_identified(
        state_machine: &mut AgentBrokerRaftStateMachine,
        identity: &CommandIdentity,
        command: ReplicatedBrokerCommandV1,
    ) -> Result<(), Box<dyn Error>> {
        let result = state_machine
            .apply_proposal(ReplicatedBrokerProposalV1::identified(identity, command))?
            .into_application_result()?;
        match result {
            Ok(result) => {
                Err(format!("stale identified owner unexpectedly succeeded: {result:?}").into())
            }
            Err(error) => {
                assert_eq!(error.code(), BrokerErrorCode::StaleFence);
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn command_session_snapshot_preserves_retry_identity_after_recovery()
    -> Result<(), Box<dyn Error>> {
        let directory = tempdir()?;
        let persistence = RedbRaftPersistence::open(directory.path().join("raft.redb"))?;
        let observation = Arc::new(RaftBrokerObservation::new(&BrokerState::default(), None));
        let mut state_machine =
            AgentBrokerRaftStateMachine::load(persistence.clone(), Arc::clone(&observation))
                .await?;
        let identity = CommandIdentity::new(
            CommandSessionId::new("snapshot-client")?,
            CommandSequence::new(9)?,
        );
        let command = ReplicatedBrokerCommandV1::try_from(BrokerCommand::EnsureNamespace(
            EnsureNamespaceCommand {
                namespace_id: NamespaceId::new("snapshot-identified")?,
                max_namespaces: 64,
            },
        ))?;
        let proposal = ReplicatedBrokerProposalV1::identified(&identity, command);
        let committed = state_machine.apply_proposal(proposal.clone())?;
        let committed_revision = state_machine.broker.state().revision();

        let mut builder = state_machine.get_snapshot_builder().await;
        let _snapshot = builder.build_snapshot().await?;
        drop(state_machine);

        let recovered_observation =
            Arc::new(RaftBrokerObservation::new(&BrokerState::default(), None));
        let mut recovered =
            AgentBrokerRaftStateMachine::load(persistence, Arc::clone(&recovered_observation))
                .await?;
        let retry = recovered.apply_proposal(proposal)?;

        assert_eq!(retry, committed);
        assert_eq!(recovered_observation.revision(), committed_revision);
        Ok(())
    }

    #[tokio::test]
    async fn truncated_incoming_snapshot_preserves_authoritative_state()
    -> Result<(), Box<dyn Error>> {
        let directory = tempdir()?;
        let persistence = RedbRaftPersistence::open(directory.path().join("raft.redb"))?;
        let observation = Arc::new(RaftBrokerObservation::new(&BrokerState::default(), None));
        let mut state_machine =
            AgentBrokerRaftStateMachine::load(persistence.clone(), Arc::clone(&observation))
                .await?;
        let revision_before = observation.revision();
        let applied_before = observation.applied_index();

        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "truncated-incoming-snapshot".to_owned(),
        };
        let install = state_machine
            .install_snapshot(
                &meta,
                Box::new(Cursor::new(b"{\"schema_version\":".to_vec())),
            )
            .await;
        assert!(install.is_err());
        assert_eq!(observation.revision(), revision_before);
        assert_eq!(observation.applied_index(), applied_before);
        assert!(read_snapshot_pair(persistence).await?.is_none());
        Ok(())
    }
}

use std::io;
use std::io::Cursor;
use std::sync::Arc;

use agent_broker_application::BrokerError;
use agent_broker_domain::{BrokerCheckpoint, BrokerState, BrokerStateMachine};
use agent_broker_storage::{decode_snapshot, encode_snapshot};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership,
};

use crate::ReplicatedBrokerResponseV1;
use crate::raft_observation::RaftBrokerObservation;
use crate::raft_persistence::RedbRaftPersistence;
use crate::raft_type_config::{AgentBrokerRaftNodeId, AgentBrokerRaftTypeConfig};

const SNAPSHOT_META_KEY: &str = "snapshot_meta_v1";
const SNAPSHOT_DATA_KEY: &str = "snapshot_data_v1";

#[derive(Debug)]
pub(crate) struct AgentBrokerRaftStateMachine {
    persistence: RedbRaftPersistence,
    broker: BrokerStateMachine,
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
        match persisted {
            Some((meta, data)) => {
                let checkpoint = decode_checkpoint(&meta, &data)?;
                let broker = BrokerStateMachine::from_checkpoint(checkpoint)
                    .map_err(|error| StorageIOError::read_state_machine(&error))?;
                observation.update(broker.state(), meta.last_log_id.map(|log_id| log_id.index));
                Ok(Self {
                    persistence,
                    broker,
                    observation,
                    last_applied: meta.last_log_id,
                    last_membership: meta.last_membership,
                })
            }
            None => {
                let checkpoint = BrokerState::default().checkpoint();
                let broker = BrokerStateMachine::from_checkpoint(checkpoint)
                    .map_err(|error| StorageIOError::read_state_machine(&error))?;
                observation.update(broker.state(), None);
                Ok(Self {
                    persistence,
                    broker,
                    observation,
                    last_applied: None,
                    last_membership: StoredMembership::default(),
                })
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AgentBrokerRaftSnapshotBuilder {
    persistence: RedbRaftPersistence,
    checkpoint: BrokerCheckpoint,
    last_applied: Option<LogId<AgentBrokerRaftNodeId>>,
    last_membership: StoredMembership<AgentBrokerRaftNodeId, BasicNode>,
}

impl RaftSnapshotBuilder<AgentBrokerRaftTypeConfig> for AgentBrokerRaftSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<AgentBrokerRaftTypeConfig>, StorageError<AgentBrokerRaftNodeId>> {
        let data = encode_snapshot(&self.checkpoint)
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
                EntryPayload::Normal(data) => {
                    let command = agent_broker_domain::commands::BrokerCommand::try_from(data)
                        .map_err(|error| StorageIOError::read_state_machine(&error))?;
                    let application_result = self
                        .broker
                        .apply(command)
                        .map(|applied| applied.result)
                        .map_err(BrokerError::from);
                    ReplicatedBrokerResponseV1::from_application_result(application_result)
                        .map_err(|error| StorageIOError::read_state_machine(&error))?
                }
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
        let checkpoint = decode_checkpoint(meta, &data)?;
        let replacement = BrokerStateMachine::from_checkpoint(checkpoint)
            .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;

        persist_snapshot(self.persistence.clone(), meta, &data).await?;

        self.broker = replacement;
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
        let _checkpoint = decode_checkpoint(&meta, &data)?;
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

fn decode_checkpoint(
    meta: &SnapshotMeta<AgentBrokerRaftNodeId, BasicNode>,
    data: &[u8],
) -> Result<BrokerCheckpoint, StorageError<AgentBrokerRaftNodeId>> {
    let checkpoint = decode_snapshot(data)
        .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;
    BrokerStateMachine::from_checkpoint(checkpoint.clone())
        .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;
    Ok(checkpoint)
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

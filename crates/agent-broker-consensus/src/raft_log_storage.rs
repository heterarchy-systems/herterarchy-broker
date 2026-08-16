use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    Entry, LogId, LogState, OptionalSend, RaftLogId, RaftLogReader, StorageError, StorageIOError,
    Vote,
};

use crate::raft_persistence::RedbRaftPersistence;
use crate::raft_type_config::{AgentBrokerRaftNodeId, AgentBrokerRaftTypeConfig};

const VOTE_KEY: &str = "vote_v1";
const COMMITTED_KEY: &str = "committed_v1";
const LAST_PURGED_KEY: &str = "last_purged_v1";

#[derive(Debug, Clone)]
pub(crate) struct AgentBrokerRaftLogStorage {
    persistence: RedbRaftPersistence,
    write_serial: Arc<tokio::sync::Mutex<()>>,
}

impl AgentBrokerRaftLogStorage {
    pub(crate) fn new(persistence: RedbRaftPersistence) -> Self {
        Self {
            persistence,
            write_serial: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    async fn read_meta<T>(
        &self,
        key: &'static str,
    ) -> Result<Option<T>, StorageError<AgentBrokerRaftNodeId>>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let persistence = self.persistence.clone();
        let bytes = spawn_blocking_io(move || persistence.read_meta(key))
            .await
            .map_err(read_storage_error)?;
        bytes
            .map(|bytes| serde_json::from_slice(&bytes).map_err(read_storage_error))
            .transpose()
    }

    async fn write_meta<T>(
        &self,
        key: &'static str,
        value: &T,
    ) -> Result<(), StorageError<AgentBrokerRaftNodeId>>
    where
        T: serde::Serialize + ?Sized,
    {
        let encoded = serde_json::to_vec(value).map_err(write_storage_error)?;
        let persistence = self.persistence.clone();
        spawn_blocking_io(move || persistence.write_meta(key, &encoded))
            .await
            .map_err(write_storage_error)
    }
}

impl RaftLogReader<AgentBrokerRaftTypeConfig> for AgentBrokerRaftLogStorage {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<AgentBrokerRaftTypeConfig>>, StorageError<AgentBrokerRaftNodeId>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let Some((start, end)) = normalize_range(&range).map_err(read_storage_error)? else {
            return Ok(Vec::new());
        };
        let persistence = self.persistence.clone();
        let encoded_entries = spawn_blocking_io(move || persistence.read_log_range(start, end))
            .await
            .map_err(|error| StorageIOError::read_logs(&error))?;

        encoded_entries
            .into_iter()
            .map(|(stored_index, encoded)| {
                let entry: Entry<AgentBrokerRaftTypeConfig> = serde_json::from_slice(&encoded)
                    .map_err(|error| StorageIOError::read_logs(&error))?;
                if entry.get_log_id().index != stored_index {
                    return Err(StorageIOError::read_logs(&io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Raft log key/index mismatch: key={stored_index}, entry={}",
                            entry.get_log_id().index
                        ),
                    ))
                    .into());
                }
                Ok(entry)
            })
            .collect()
    }
}

impl RaftLogStorage<AgentBrokerRaftTypeConfig> for AgentBrokerRaftLogStorage {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<AgentBrokerRaftTypeConfig>, StorageError<AgentBrokerRaftNodeId>> {
        let last_purged: Option<LogId<AgentBrokerRaftNodeId>> =
            self.read_meta(LAST_PURGED_KEY).await?;
        let persistence = self.persistence.clone();
        let last_present = spawn_blocking_io(move || persistence.read_last_log())
            .await
            .map_err(|error| StorageIOError::read_logs(&error))?;
        let last_present =
            last_present
                .map(
                    |(stored_index, encoded)| -> Result<
                        LogId<AgentBrokerRaftNodeId>,
                        StorageError<AgentBrokerRaftNodeId>,
                    > {
                        let entry: Entry<AgentBrokerRaftTypeConfig> =
                            serde_json::from_slice(&encoded).map_err(|error| {
                                StorageIOError::<AgentBrokerRaftNodeId>::read_logs(&error)
                            })?;
                        if entry.get_log_id().index != stored_index {
                            return Err(StorageIOError::<AgentBrokerRaftNodeId>::read_logs(
                                &io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "Raft log key/index mismatch in last entry",
                                ),
                            )
                            .into());
                        }
                        Ok(*entry.get_log_id())
                    },
                )
                .transpose()?;

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last_present.or(last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<AgentBrokerRaftNodeId>,
    ) -> Result<(), StorageError<AgentBrokerRaftNodeId>> {
        let _write_guard = self.write_serial.lock().await;
        let encoded =
            serde_json::to_vec(vote).map_err(|error| StorageIOError::write_vote(&error))?;
        let persistence = self.persistence.clone();
        spawn_blocking_io(move || persistence.write_meta(VOTE_KEY, &encoded))
            .await
            .map_err(|error| StorageIOError::write_vote(&error).into())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<AgentBrokerRaftNodeId>>, StorageError<AgentBrokerRaftNodeId>> {
        let persistence = self.persistence.clone();
        let encoded = spawn_blocking_io(move || persistence.read_meta(VOTE_KEY))
            .await
            .map_err(|error| StorageIOError::read_vote(&error))?;
        encoded
            .map(|encoded| {
                serde_json::from_slice(&encoded)
                    .map_err(|error| StorageIOError::read_vote(&error).into())
            })
            .transpose()
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<AgentBrokerRaftNodeId>>,
    ) -> Result<(), StorageError<AgentBrokerRaftNodeId>> {
        let _write_guard = self.write_serial.lock().await;
        self.write_meta(COMMITTED_KEY, &committed).await
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<AgentBrokerRaftNodeId>>, StorageError<AgentBrokerRaftNodeId>> {
        self.read_meta(COMMITTED_KEY).await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<AgentBrokerRaftTypeConfig>,
    ) -> Result<(), StorageError<AgentBrokerRaftNodeId>>
    where
        I: IntoIterator<Item = Entry<AgentBrokerRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let _write_guard = self.write_serial.lock().await;
        let entries = entries.into_iter().collect::<Vec<_>>();
        let mut encoded = Vec::with_capacity(entries.len());
        for entry in entries {
            let log_id = *entry.get_log_id();
            let payload = match serde_json::to_vec(&entry) {
                Ok(payload) => payload,
                Err(error) => {
                    let message = error.to_string();
                    callback.log_io_completed(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        message.clone(),
                    )));
                    return Err(StorageIOError::write_log_entry(
                        log_id,
                        &io::Error::new(io::ErrorKind::InvalidData, message),
                    )
                    .into());
                }
            };
            encoded.push((log_id.index, payload));
        }

        let persistence = self.persistence.clone();
        match spawn_blocking_io(move || persistence.append_log_batch(&encoded)).await {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                callback.log_io_completed(Err(io::Error::new(error.kind(), message.clone())));
                Err(StorageIOError::write_logs(&io::Error::other(message)).into())
            }
        }
    }

    async fn truncate(
        &mut self,
        log_id: LogId<AgentBrokerRaftNodeId>,
    ) -> Result<(), StorageError<AgentBrokerRaftNodeId>> {
        let _write_guard = self.write_serial.lock().await;
        let persistence = self.persistence.clone();
        spawn_blocking_io(move || persistence.truncate_from(log_id.index))
            .await
            .map_err(|error| StorageIOError::write_logs(&error).into())
    }

    async fn purge(
        &mut self,
        log_id: LogId<AgentBrokerRaftNodeId>,
    ) -> Result<(), StorageError<AgentBrokerRaftNodeId>> {
        let _write_guard = self.write_serial.lock().await;
        let encoded = serde_json::to_vec(&log_id).map_err(write_storage_error)?;
        let persistence = self.persistence.clone();
        spawn_blocking_io(move || {
            persistence.purge_through_and_write_meta(log_id.index, LAST_PURGED_KEY, &encoded)
        })
        .await
        .map_err(|error| StorageIOError::write_logs(&error).into())
    }
}

async fn spawn_blocking_io<T, F>(operation: F) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(io::Error::other)?
}

fn normalize_range<RB>(range: &RB) -> io::Result<Option<(u64, Option<u64>)>>
where
    RB: RangeBounds<u64>,
{
    let start = match range.start_bound() {
        Bound::Unbounded => 0,
        Bound::Included(value) => *value,
        Bound::Excluded(value) => value.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "excluded Raft range start overflows u64",
            )
        })?,
    };
    let end_exclusive = match range.end_bound() {
        Bound::Unbounded => None,
        Bound::Excluded(value) => Some(*value),
        Bound::Included(value) => value.checked_add(1),
    };
    if end_exclusive.is_some_and(|end| start >= end) {
        return Ok(None);
    }
    Ok(Some((start, end_exclusive)))
}

fn read_storage_error(
    error: impl std::error::Error + 'static,
) -> StorageError<AgentBrokerRaftNodeId> {
    StorageIOError::read(&error).into()
}

fn write_storage_error(
    error: impl std::error::Error + 'static,
) -> StorageError<AgentBrokerRaftNodeId> {
    StorageIOError::write(&error).into()
}

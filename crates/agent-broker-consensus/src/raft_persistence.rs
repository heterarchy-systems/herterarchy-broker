use std::io;
use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

const RAFT_LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log_v1");
const RAFT_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta_v1");

/// Crash-safe byte persistence shared by the one-node `OpenRaft` log and snapshot adapters.
///
/// `redb` owns transactional durability. `OpenRaft` serialization stays in the higher consensus layer so
/// this type does not become a second interpretation of Raft data structures.
#[derive(Debug, Clone)]
pub(crate) struct RedbRaftPersistence {
    database: Arc<Database>,
}

type OptionalMetaPair = (Option<Vec<u8>>, Option<Vec<u8>>);

impl RedbRaftPersistence {
    pub(crate) fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let database = Database::create(path).map_err(io_other)?;
        let persistence = Self {
            database: Arc::new(database),
        };
        persistence.ensure_tables()?;
        Ok(persistence)
    }

    fn ensure_tables(&self) -> io::Result<()> {
        let transaction = self.database.begin_write().map_err(io_other)?;
        {
            let _log_table = transaction.open_table(RAFT_LOG_TABLE).map_err(io_other)?;
            let _meta_table = transaction.open_table(RAFT_META_TABLE).map_err(io_other)?;
        }
        transaction.commit().map_err(io_other)
    }

    pub(crate) fn read_log_range(
        &self,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let transaction = self.database.begin_read().map_err(io_other)?;
        let table = transaction.open_table(RAFT_LOG_TABLE).map_err(io_other)?;
        let mut output = Vec::new();
        match end_exclusive {
            Some(end) => {
                for item in table.range(start_inclusive..end).map_err(io_other)? {
                    let (key, value) = item.map_err(io_other)?;
                    output.push((key.value(), value.value().to_vec()));
                }
            }
            None => {
                for item in table.range(start_inclusive..).map_err(io_other)? {
                    let (key, value) = item.map_err(io_other)?;
                    output.push((key.value(), value.value().to_vec()));
                }
            }
        }
        Ok(output)
    }

    pub(crate) fn read_last_log(&self) -> io::Result<Option<(u64, Vec<u8>)>> {
        let transaction = self.database.begin_read().map_err(io_other)?;
        let table = transaction.open_table(RAFT_LOG_TABLE).map_err(io_other)?;
        let Some((key, value)) = table.last().map_err(io_other)? else {
            return Ok(None);
        };
        Ok(Some((key.value(), value.value().to_vec())))
    }

    pub(crate) fn append_log_batch(&self, entries: &[(u64, Vec<u8>)]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        for adjacent in entries.windows(2) {
            if adjacent[1].0 != adjacent[0].0.saturating_add(1) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Raft append batch contains a log index hole",
                ));
            }
        }

        let transaction = self.database.begin_write().map_err(io_other)?;
        {
            let mut table = transaction.open_table(RAFT_LOG_TABLE).map_err(io_other)?;
            for (index, payload) in entries {
                table.insert(index, payload.as_slice()).map_err(io_other)?;
            }
        }
        transaction.commit().map_err(io_other)
    }

    pub(crate) fn truncate_from(&self, index: u64) -> io::Result<()> {
        let transaction = self.database.begin_write().map_err(io_other)?;
        {
            let mut table = transaction.open_table(RAFT_LOG_TABLE).map_err(io_other)?;
            table
                .retain(|stored_index, _| stored_index < index)
                .map_err(io_other)?;
        }
        transaction.commit().map_err(io_other)
    }

    pub(crate) fn purge_through_and_write_meta(
        &self,
        index: u64,
        meta_key: &'static str,
        meta_value: &[u8],
    ) -> io::Result<()> {
        let transaction = self.database.begin_write().map_err(io_other)?;
        {
            let mut log_table = transaction.open_table(RAFT_LOG_TABLE).map_err(io_other)?;
            log_table
                .retain(|stored_index, _| stored_index > index)
                .map_err(io_other)?;
            let mut meta_table = transaction.open_table(RAFT_META_TABLE).map_err(io_other)?;
            meta_table.insert(meta_key, meta_value).map_err(io_other)?;
        }
        transaction.commit().map_err(io_other)
    }

    pub(crate) fn read_meta(&self, key: &'static str) -> io::Result<Option<Vec<u8>>> {
        let transaction = self.database.begin_read().map_err(io_other)?;
        let table = transaction.open_table(RAFT_META_TABLE).map_err(io_other)?;
        let value = table.get(key).map_err(io_other)?;
        Ok(value.map(|value| value.value().to_vec()))
    }

    pub(crate) fn read_meta_pair(
        &self,
        first_key: &'static str,
        second_key: &'static str,
    ) -> io::Result<OptionalMetaPair> {
        let transaction = self.database.begin_read().map_err(io_other)?;
        let table = transaction.open_table(RAFT_META_TABLE).map_err(io_other)?;
        let first = table
            .get(first_key)
            .map_err(io_other)?
            .map(|value| value.value().to_vec());
        let second = table
            .get(second_key)
            .map_err(io_other)?
            .map(|value| value.value().to_vec());
        Ok((first, second))
    }

    pub(crate) fn write_meta(&self, key: &'static str, value: &[u8]) -> io::Result<()> {
        let transaction = self.database.begin_write().map_err(io_other)?;
        {
            let mut table = transaction.open_table(RAFT_META_TABLE).map_err(io_other)?;
            table.insert(key, value).map_err(io_other)?;
        }
        transaction.commit().map_err(io_other)
    }

    pub(crate) fn write_meta_pair(
        &self,
        first_key: &'static str,
        first_value: &[u8],
        second_key: &'static str,
        second_value: &[u8],
    ) -> io::Result<()> {
        let transaction = self.database.begin_write().map_err(io_other)?;
        {
            let mut table = transaction.open_table(RAFT_META_TABLE).map_err(io_other)?;
            table.insert(first_key, first_value).map_err(io_other)?;
            table.insert(second_key, second_value).map_err(io_other)?;
        }
        transaction.commit().map_err(io_other)
    }
}

fn io_other(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(error)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;

    use tempfile::tempdir;

    use super::RedbRaftPersistence;

    #[test]
    fn physically_corrupted_redb_file_fails_without_silent_reinitialization() -> io::Result<()> {
        let directory = tempdir()?;
        let path = directory.path().join("corrupted.redb");
        let corrupt_bytes = b"this-is-not-a-redb-database";
        fs::write(&path, corrupt_bytes)?;

        let open_result = RedbRaftPersistence::open(&path);
        if open_result.is_ok() {
            return Err(io::Error::other(
                "physically corrupted redb file unexpectedly opened",
            ));
        }
        let bytes_after_failed_open = fs::read(&path)?;
        if bytes_after_failed_open != corrupt_bytes {
            return Err(io::Error::other(
                "failed redb open modified or silently reinitialized corrupted durable bytes",
            ));
        }
        Ok(())
    }
}

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use agent_broker_domain::results::StateChangeSet;
use agent_broker_domain::{BrokerCheckpoint, BrokerState, Revision, Term};

use crate::filesystem_durability::{
    append_synced, fsync_compatible, truncate_synced, write_snapshot_atomic,
};
use crate::{
    RepositoryError, StorageError, apply_journal_mutation, decode_journal_mutation,
    decode_snapshot, encode_journal_mutation, encode_snapshot,
};

const DEFAULT_COMPACT_EVERY: u64 = 4_096;
const DEFAULT_MAX_JOURNAL_BYTES: u64 = 8 * 1024 * 1024;
const MIN_MAX_JOURNAL_BYTES: u64 = 4_096;
const MAX_JOURNAL_RECORD_BYTES: usize = 128 * 1024 * 1024;

/// Bounded compaction policy for the standalone durable journal.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct JournalCompactionPolicy {
    compact_every: NonZeroU64,
    max_journal_bytes: u64,
}

impl JournalCompactionPolicy {
    /// Construct a validated journal compaction policy.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryError::InvalidConfiguration`] for a zero commit interval or a byte
    /// threshold below the Python reference minimum of 4096 bytes.
    pub fn new(compact_every: u64, max_journal_bytes: u64) -> Result<Self, RepositoryError> {
        let compact_every = NonZeroU64::new(compact_every).ok_or(
            RepositoryError::InvalidConfiguration("compact_every must be positive"),
        )?;
        if max_journal_bytes < MIN_MAX_JOURNAL_BYTES {
            return Err(RepositoryError::InvalidConfiguration(
                "max_journal_bytes must be at least 4096",
            ));
        }
        Ok(Self {
            compact_every,
            max_journal_bytes,
        })
    }
}

impl Default for JournalCompactionPolicy {
    fn default() -> Self {
        Self {
            compact_every: NonZeroU64::new(DEFAULT_COMPACT_EVERY).unwrap_or(NonZeroU64::MIN),
            max_journal_bytes: DEFAULT_MAX_JOURNAL_BYTES,
        }
    }
}

/// Persistence port used by the single-node consensus implementation.
pub trait BrokerStateRepository {
    /// Load the latest durable logical checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryError`] when snapshot/journal I/O or strict recovery validation fails.
    fn load(&mut self) -> Result<BrokerCheckpoint, RepositoryError>;

    /// Durably persist one already-validated state-machine mutation.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryError`] when journal encoding, append/fsync, or required deferred
    /// compaction cannot satisfy the durability contract.
    fn commit(
        &mut self,
        state: &BrokerState,
        changes: &StateChangeSet,
    ) -> Result<(), RepositoryError>;
}

/// Incremental fsync journal with crash-safe periodic snapshot compaction.
#[derive(Debug)]
pub struct JournaledBrokerStateRepository {
    snapshot_path: PathBuf,
    journal_path: PathBuf,
    policy: JournalCompactionPolicy,
    commits_since_compaction: u64,
    journal_bytes: u64,
    compaction_deferred: bool,
}

impl JournaledBrokerStateRepository {
    /// Construct a repository using the Python-compatible default journal path when omitted.
    #[must_use]
    pub fn new(
        snapshot_path: impl Into<PathBuf>,
        journal_path: Option<PathBuf>,
        policy: JournalCompactionPolicy,
    ) -> Self {
        let snapshot_path = snapshot_path.into();
        let journal_path = journal_path.unwrap_or_else(|| default_journal_path(&snapshot_path));
        Self {
            snapshot_path,
            journal_path,
            policy,
            commits_since_compaction: 0,
            journal_bytes: 0,
            compaction_deferred: false,
        }
    }

    /// Borrow the configured snapshot path.
    #[must_use]
    pub fn snapshot_path(&self) -> &Path {
        &self.snapshot_path
    }

    /// Borrow the configured journal path.
    #[must_use]
    pub fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    /// Return whether the last post-commit compaction attempt failed after the WAL was durable.
    #[must_use]
    pub const fn compaction_deferred(&self) -> bool {
        self.compaction_deferred
    }

    fn should_compact(&self) -> bool {
        self.commits_since_compaction >= self.policy.compact_every.get()
            || self.journal_bytes >= self.policy.max_journal_bytes
    }

    fn compact(&mut self, state: &BrokerState) -> Result<(), RepositoryError> {
        let payload = encode_snapshot(&state.checkpoint())?;
        write_snapshot_atomic(&self.snapshot_path, &payload)?;
        truncate_synced(&self.journal_path)?;
        self.commits_since_compaction = 0;
        self.journal_bytes = 0;
        Ok(())
    }

    fn replay_journal(&self, checkpoint: &mut BrokerCheckpoint) -> Result<(), RepositoryError> {
        let mut journal = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.journal_path)
            .map_err(|error| RepositoryError::io("journal open for replay failed", error))?;
        let size = journal
            .metadata()
            .map_err(|error| RepositoryError::io("journal metadata read failed", error))?
            .len();
        let reader_file = journal
            .try_clone()
            .map_err(|error| RepositoryError::io("journal replay handle clone failed", error))?;
        let mut reader = BufReader::new(reader_file);
        let mut offset = 0_u64;
        let mut line = Vec::new();

        while offset < size {
            line.clear();
            let read = read_bounded_record(&mut reader, &mut line)?;
            if read == 0 {
                break;
            }
            let read = u64::try_from(read).map_err(|_| {
                RepositoryError::Storage(StorageError::InvalidFormat(
                    "journal record length does not fit u64".to_owned(),
                ))
            })?;
            let next_offset = offset.checked_add(read).ok_or_else(|| {
                RepositoryError::Storage(StorageError::InvalidFormat(
                    "journal replay offset overflowed".to_owned(),
                ))
            })?;
            let is_final = next_offset == size;
            if !line.ends_with(b"\n") {
                if is_final {
                    truncate_torn_tail(&mut journal, offset)?;
                    return Ok(());
                }
                return Err(StorageError::InvalidFormat(
                    "journal contains a torn record before its tail".to_owned(),
                )
                .into());
            }
            if serde_json::from_slice::<serde_json::Value>(&line).is_err() {
                if is_final {
                    truncate_torn_tail(&mut journal, offset)?;
                    return Ok(());
                }
                return Err(StorageError::InvalidFormat(
                    "journal contains invalid JSON before its tail".to_owned(),
                )
                .into());
            }
            let mutation = decode_journal_mutation(&line)?;
            apply_journal_mutation(checkpoint, mutation)?;
            offset = next_offset;
        }
        Ok(())
    }
}

impl BrokerStateRepository for JournaledBrokerStateRepository {
    fn load(&mut self) -> Result<BrokerCheckpoint, RepositoryError> {
        let mut checkpoint = load_snapshot(&self.snapshot_path)?;
        let snapshot_revision = checkpoint.revision;
        if self
            .journal_path
            .try_exists()
            .map_err(|error| RepositoryError::io("journal existence check failed", error))?
        {
            self.replay_journal(&mut checkpoint)?;
            self.journal_bytes = self
                .journal_path
                .metadata()
                .map_err(|error| RepositoryError::io("journal size read failed", error))?
                .len();
        }
        self.commits_since_compaction = checkpoint
            .revision
            .get()
            .saturating_sub(snapshot_revision.get());
        self.compaction_deferred = false;
        Ok(checkpoint)
    }

    fn commit(
        &mut self,
        state: &BrokerState,
        changes: &StateChangeSet,
    ) -> Result<(), RepositoryError> {
        if self.compaction_deferred {
            self.compact(state)?;
            self.compaction_deferred = false;
            return Ok(());
        }

        let payload = encode_journal_mutation(state, changes)?;
        append_synced(&self.journal_path, &payload)?;
        let payload_len = u64::try_from(payload.len()).map_err(|_| {
            RepositoryError::Storage(StorageError::InvalidFormat(
                "journal payload length does not fit u64".to_owned(),
            ))
        })?;
        self.journal_bytes = self.journal_bytes.checked_add(payload_len).ok_or_else(|| {
            RepositoryError::Storage(StorageError::InvalidFormat(
                "journal byte accounting overflowed".to_owned(),
            ))
        })?;
        self.commits_since_compaction =
            self.commits_since_compaction
                .checked_add(1)
                .ok_or_else(|| {
                    RepositoryError::Storage(StorageError::InvalidFormat(
                        "journal commit accounting overflowed".to_owned(),
                    ))
                })?;

        if self.should_compact() && self.compact(state).is_err() {
            self.compaction_deferred = true;
        }
        Ok(())
    }
}

fn load_snapshot(path: &Path) -> Result<BrokerCheckpoint, RepositoryError> {
    if !path
        .try_exists()
        .map_err(|error| RepositoryError::io("snapshot existence check failed", error))?
    {
        return Ok(empty_checkpoint());
    }
    let payload =
        fs::read(path).map_err(|error| RepositoryError::io("snapshot read failed", error))?;
    decode_snapshot(&payload).map_err(RepositoryError::from)
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

fn default_journal_path(snapshot_path: &Path) -> PathBuf {
    let mut extension = snapshot_path
        .extension()
        .map_or_else(OsString::new, std::ffi::OsStr::to_os_string);
    if extension.is_empty() {
        extension.push("journal");
    } else {
        extension.push(".journal");
    }
    snapshot_path.with_extension(extension)
}

fn truncate_torn_tail(journal: &mut File, offset: u64) -> Result<(), RepositoryError> {
    journal
        .set_len(offset)
        .map_err(|error| RepositoryError::io("torn journal tail truncation failed", error))?;
    fsync_compatible(journal)
        .map_err(|error| RepositoryError::io("torn journal tail fsync failed", error))
}

fn read_bounded_record(
    reader: &mut impl BufRead,
    line: &mut Vec<u8>,
) -> Result<usize, RepositoryError> {
    loop {
        let available = reader
            .fill_buf()
            .map_err(|error| RepositoryError::io("journal replay read failed", error))?;
        if available.is_empty() {
            return Ok(line.len());
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        if line.len().saturating_add(consumed) > MAX_JOURNAL_RECORD_BYTES {
            return Err(StorageError::InvalidFormat(format!(
                "journal record exceeds {MAX_JOURNAL_RECORD_BYTES} bytes"
            ))
            .into());
        }
        line.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(line.len());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::default_journal_path;

    #[test]
    fn default_journal_path_matches_python_suffix_behavior() {
        assert_eq!(
            default_journal_path(Path::new("broker-state.json")),
            Path::new("broker-state.json.journal")
        );
        assert_eq!(
            default_journal_path(Path::new("broker-state")),
            Path::new("broker-state.journal")
        );
    }
}

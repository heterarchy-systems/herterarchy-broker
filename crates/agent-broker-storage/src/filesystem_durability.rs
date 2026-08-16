use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use tempfile::Builder;

use crate::RepositoryError;

pub(crate) fn append_synced(path: &Path, payload: &[u8]) -> Result<(), RepositoryError> {
    let parent = ensure_parent_directory(path)?;
    let created = !path
        .try_exists()
        .map_err(|error| RepositoryError::io("journal existence check failed", error))?;
    let mut journal = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| RepositoryError::io("journal open for append failed", error))?;
    journal
        .write_all(payload)
        .map_err(|error| RepositoryError::io("journal append failed", error))?;
    journal
        .flush()
        .map_err(|error| RepositoryError::io("journal flush failed", error))?;
    fsync_compatible(&journal)
        .map_err(|error| RepositoryError::io("journal fsync failed", error))?;
    if created {
        sync_directory(parent)?;
    }
    Ok(())
}

pub(crate) fn write_snapshot_atomic(path: &Path, payload: &[u8]) -> Result<(), RepositoryError> {
    let parent = ensure_parent_directory(path)?;
    let mut temporary = Builder::new()
        .prefix(".agent-broker-snapshot.")
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|error| RepositoryError::io("snapshot tempfile creation failed", error))?;
    temporary
        .write_all(payload)
        .map_err(|error| RepositoryError::io("snapshot tempfile write failed", error))?;
    temporary
        .flush()
        .map_err(|error| RepositoryError::io("snapshot tempfile flush failed", error))?;
    fsync_compatible(temporary.as_file())
        .map_err(|error| RepositoryError::io("snapshot tempfile fsync failed", error))?;
    temporary
        .persist(path)
        .map_err(|error| RepositoryError::io("atomic snapshot replace failed", error.error))?;
    sync_directory(parent)
}

pub(crate) fn truncate_synced(path: &Path) -> Result<(), RepositoryError> {
    let parent = ensure_parent_directory(path)?;
    let mut journal = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|error| RepositoryError::io("journal open for truncation failed", error))?;
    journal
        .flush()
        .map_err(|error| RepositoryError::io("journal truncation flush failed", error))?;
    fsync_compatible(&journal)
        .map_err(|error| RepositoryError::io("journal truncation fsync failed", error))?;
    sync_directory(parent)
}

pub(crate) fn sync_directory(directory: &Path) -> Result<(), RepositoryError> {
    let handle = File::open(directory)
        .map_err(|error| RepositoryError::io("directory open for fsync failed", error))?;
    fsync_compatible(&handle).map_err(|error| RepositoryError::io("directory fsync failed", error))
}

pub(crate) fn fsync_compatible(file: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        // Rust std intentionally upgrades `File::sync_all` to `F_FULLFSYNC` on Apple platforms.
        // The Python reference contract uses POSIX `fsync(2)`, so use rustix's safe wrapper to
        // preserve the same durability boundary without introducing unsafe code.
        rustix::fs::fsync(file).map_err(io::Error::from)
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        file.sync_all()
    }
}

fn ensure_parent_directory(path: &Path) -> Result<&Path, RepositoryError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|error| RepositoryError::io("storage directory creation failed", error))?;
    Ok(parent)
}

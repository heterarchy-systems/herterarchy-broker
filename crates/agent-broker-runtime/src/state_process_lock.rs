use std::ffi::OsString;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::RuntimeError;

/// Exclusive process ownership guard for one standalone Broker state path.
#[derive(Debug)]
pub struct BrokerStateProcessLock {
    lock_path: PathBuf,
    file: File,
}

impl BrokerStateProcessLock {
    /// Acquire the Python-compatible `<state suffix>.lock` path non-blockingly.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::StateAlreadyOwned`] when another process/handle owns the state, or
    /// an I/O error when lock metadata cannot be created and fsynced.
    pub fn acquire(state_path: &Path) -> Result<Self, RuntimeError> {
        let lock_path = lock_path_for(state_path);
        let parent = lock_path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|error| {
            RuntimeError::io("Broker state lock directory creation failed", error)
        })?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&lock_path)
            .map_err(|error| RuntimeError::io("Broker state lock file open failed", error))?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(RuntimeError::StateAlreadyOwned),
            Err(TryLockError::Error(error)) => {
                return Err(RuntimeError::io(
                    "Broker state lock acquisition failed",
                    error,
                ));
            }
        }
        if let Err(error) = write_lock_metadata(&mut file) {
            let _ = file.unlock();
            return Err(error);
        }
        Ok(Self { lock_path, file })
    }

    /// Borrow the lock file path for diagnostics.
    #[must_use]
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }
}

impl Drop for BrokerStateProcessLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn write_lock_metadata(file: &mut File) -> Result<(), RuntimeError> {
    file.set_len(0)
        .map_err(|error| RuntimeError::io("Broker state lock truncate failed", error))?;
    let pid = std::process::id();
    writeln!(file, "{pid}")
        .map_err(|error| RuntimeError::io("Broker state lock metadata write failed", error))?;
    file.flush()
        .map_err(|error| RuntimeError::io("Broker state lock metadata flush failed", error))?;
    fsync_compatible(file)
        .map_err(|error| RuntimeError::io("Broker state lock metadata fsync failed", error))
}

fn fsync_compatible(file: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        rustix::fs::fsync(file).map_err(io::Error::from)
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        file.sync_all()
    }
}

fn lock_path_for(state_path: &Path) -> PathBuf {
    let mut extension = state_path
        .extension()
        .map_or_else(OsString::new, std::ffi::OsStr::to_os_string);
    if extension.is_empty() {
        extension.push("lock");
    } else {
        extension.push(".lock");
    }
    state_path.with_extension(extension)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::lock_path_for;

    #[test]
    fn lock_path_matches_python_suffix_contract() {
        assert_eq!(
            lock_path_for(Path::new("broker-state.json")),
            Path::new("broker-state.json.lock")
        );
        assert_eq!(
            lock_path_for(Path::new("broker-state")),
            Path::new("broker-state.lock")
        );
    }
}

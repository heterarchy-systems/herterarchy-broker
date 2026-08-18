use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use agent_broker_application::{
    CommandIdentity, CommandSessionId, SessionOwnerEpoch, SessionOwnerInstanceId,
};
use agent_broker_protocol::BrokerRequest;
use tokio::sync::Semaphore;

use crate::{
    ClientSessionStoreError, DurableClientSessionStore, DurableSessionOwner,
    PendingOwnerAcquisition, ReservedCommand,
};

/// Async facade over the crash-safe local command-session store.
///
/// The underlying store intentionally remains synchronous because it performs filesystem locking,
/// atomic replacement, and fsync. Every potentially blocking operation is isolated on Tokio's
/// blocking pool; Broker network I/O is never routed through this adapter. A one-permit admission
/// gate ensures concurrent callers do not occupy multiple blocking-pool threads while waiting on the
/// store's exclusive mutex.
///
/// Once a submitted blocking operation starts, cancellation of the awaiting async Task does not
/// abort that durability operation. The permit remains held until the blocking operation finishes,
/// so subsequent calls serialize behind the final durable state instead of racing it.
#[derive(Clone)]
pub struct AsyncDurableClientSessionStore {
    inner: Arc<Mutex<DurableClientSessionStore>>,
    admission: Arc<Semaphore>,
    session_id: CommandSessionId,
    state_path: PathBuf,
    lock_path: PathBuf,
}

impl AsyncDurableClientSessionStore {
    /// Open or create one durable session without blocking an async executor worker.
    ///
    /// # Errors
    /// Returns the same durable store errors as [`DurableClientSessionStore::open_or_create`].
    pub async fn open_or_create(
        path: impl AsRef<Path>,
        session_id: CommandSessionId,
    ) -> Result<Self, ClientSessionStoreError> {
        let state_path = path.as_ref().to_path_buf();
        let open_path = state_path.clone();
        let open_session_id = session_id.clone();
        let (store, lock_path) = tokio::task::spawn_blocking(move || {
            let store = DurableClientSessionStore::open_or_create(open_path, open_session_id)?;
            let lock_path = store.lock_path().to_path_buf();
            Ok::<_, ClientSessionStoreError>((store, lock_path))
        })
        .await
        .map_err(|error| join_error(&error))??;
        Ok(Self {
            inner: Arc::new(Mutex::new(store)),
            admission: Arc::new(Semaphore::new(1)),
            session_id,
            state_path,
            lock_path,
        })
    }

    #[must_use]
    pub const fn session_id(&self) -> &CommandSessionId {
        &self.session_id
    }

    #[must_use]
    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    #[must_use]
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }

    /// Read the next durable sequence on the blocking pool.
    ///
    /// # Errors
    /// Returns durable store errors or a blocking-task failure.
    pub async fn next_sequence(
        &self,
    ) -> Result<agent_broker_application::CommandSequence, ClientSessionStoreError> {
        self.with_store(|store| store.next_sequence()).await
    }

    /// Read the current confirmed owner.
    ///
    /// # Errors
    /// Returns durable store errors or a blocking-task failure.
    pub async fn owner(&self) -> Result<Option<DurableSessionOwner>, ClientSessionStoreError> {
        self.with_store(|store| store.owner()).await
    }

    /// Read any pending owner acquisition.
    ///
    /// # Errors
    /// Returns durable store errors or a blocking-task failure.
    pub async fn pending_owner_acquisition(
        &self,
    ) -> Result<Option<PendingOwnerAcquisition>, ClientSessionStoreError> {
        self.with_store(|store| store.pending_owner_acquisition())
            .await
    }

    /// Durably reserve an owner acquisition before network submission.
    ///
    /// # Errors
    /// Returns durable store errors or a blocking-task failure.
    pub async fn begin_owner_acquisition(
        &self,
        owner_instance_id: SessionOwnerInstanceId,
    ) -> Result<PendingOwnerAcquisition, ClientSessionStoreError> {
        self.with_store(move |store| store.begin_owner_acquisition(owner_instance_id))
            .await
    }

    /// Durably confirm a Broker-returned owner epoch.
    ///
    /// # Errors
    /// Returns durable store errors or a blocking-task failure.
    pub async fn confirm_owner_acquisition(
        &self,
        owner_epoch: SessionOwnerEpoch,
    ) -> Result<DurableSessionOwner, ClientSessionStoreError> {
        self.with_store(move |store| store.confirm_owner_acquisition(owner_epoch))
            .await
    }

    /// Durably reserve one exact owner-aware mutation.
    ///
    /// # Errors
    /// Returns durable store errors or a blocking-task failure.
    pub async fn reserve_command(
        &self,
        request: BrokerRequest,
    ) -> Result<ReservedCommand, ClientSessionStoreError> {
        self.with_store(move |store| store.reserve_command(request))
            .await
    }

    /// Read the exact in-flight mutation, if any.
    ///
    /// # Errors
    /// Returns durable store errors or a blocking-task failure.
    pub async fn in_flight(&self) -> Result<Option<ReservedCommand>, ClientSessionStoreError> {
        self.with_store(|store| store.in_flight()).await
    }

    /// Durably acknowledge a definitive outcome and advance the sequence.
    ///
    /// # Errors
    /// Returns durable store errors or a blocking-task failure.
    pub async fn acknowledge_in_flight_outcome(
        &self,
        identity: CommandIdentity,
    ) -> Result<(), ClientSessionStoreError> {
        self.with_store(move |store| store.acknowledge_in_flight_outcome(&identity))
            .await
    }

    /// Durably release an explicitly rejected in-flight command without advancing sequence.
    ///
    /// # Errors
    /// Returns durable store errors or a blocking-task failure.
    pub async fn release_rejected_in_flight(
        &self,
        identity: CommandIdentity,
    ) -> Result<(), ClientSessionStoreError> {
        self.with_store(move |store| store.release_rejected_in_flight(&identity))
            .await
    }

    async fn with_store<T, F>(&self, operation: F) -> Result<T, ClientSessionStoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut DurableClientSessionStore) -> Result<T, ClientSessionStoreError>
            + Send
            + 'static,
    {
        let permit = Arc::clone(&self.admission)
            .acquire_owned()
            .await
            .map_err(|_| {
                ClientSessionStoreError::InvalidState(
                    "async durable client session admission gate closed unexpectedly".to_owned(),
                )
            })?;
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut store = inner.lock().map_err(|_| {
                ClientSessionStoreError::InvalidState(
                    "async durable client session store lock was poisoned".to_owned(),
                )
            })?;
            operation(&mut store)
        })
        .await
        .map_err(|error| join_error(&error))?
    }
}

fn join_error(error: &tokio::task::JoinError) -> ClientSessionStoreError {
    ClientSessionStoreError::InvalidState(format!(
        "async durable client session blocking task failed: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use agent_broker_application::{CommandSessionId, SessionOwnerEpoch, SessionOwnerInstanceId};
    use agent_broker_domain::NamespaceId;
    use agent_broker_protocol::{BrokerRequest, EnsureNamespaceRequest, RequestId};
    use tempfile::tempdir;

    use super::AsyncDurableClientSessionStore;

    #[tokio::test]
    async fn async_durable_store_preserves_write_ahead_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("async-client-session.json");
        let store = AsyncDurableClientSessionStore::open_or_create(
            &path,
            CommandSessionId::new("async-durable-session")?,
        )
        .await?;
        let owner_instance = SessionOwnerInstanceId::new("async-durable-owner")?;
        let pending = store
            .begin_owner_acquisition(owner_instance.clone())
            .await?;
        assert_eq!(pending.expected_owner_epoch(), SessionOwnerEpoch::INITIAL);
        let owner = store
            .confirm_owner_acquisition(SessionOwnerEpoch::INITIAL)
            .await?;
        assert_eq!(owner.owner_instance_id(), &owner_instance);

        let reserved = store
            .reserve_command(BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
                request_id: RequestId::new("async-durable-request-1")?,
                namespace_id: NamespaceId::new("async-durable-namespace")?,
            }))
            .await?;
        assert_eq!(store.in_flight().await?, Some(reserved.clone()));
        store
            .acknowledge_in_flight_outcome(reserved.identity().clone())
            .await?;
        assert!(store.in_flight().await?.is_none());
        assert_eq!(store.next_sequence().await?.get(), 2);
        assert_eq!(store.state_path(), path.as_path());
        assert!(store.lock_path().is_absolute() || store.lock_path().parent().is_some());
        Ok(())
    }
}

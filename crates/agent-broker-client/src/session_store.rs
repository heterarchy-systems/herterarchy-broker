use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use agent_broker_application::{
    CommandIdentity, CommandSequence, CommandSessionId, SessionOwnerEpoch, SessionOwnerInstanceId,
};
use agent_broker_protocol::{
    BrokerRequest, Operation, ProtocolCodecError, decode_request, encode_request,
};
use serde::{Deserialize, Serialize};
use tempfile::Builder;

const SESSION_STORE_VERSION: u32 = 1;

/// Durable local command-session state failure.
#[derive(Debug)]
pub enum ClientSessionStoreError {
    Io {
        context: &'static str,
        source: io::Error,
    },
    Protocol(ProtocolCodecError),
    InvalidState(String),
    StateAlreadyOwned,
    OperationBlocked(&'static str),
    SequenceExhausted,
}

impl ClientSessionStoreError {
    fn io(context: &'static str, source: io::Error) -> Self {
        Self::Io { context, source }
    }
}

impl fmt::Display for ClientSessionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { context, source } => write!(formatter, "{context}: {source}"),
            Self::Protocol(error) => error.fmt(formatter),
            Self::InvalidState(message) => {
                write!(formatter, "invalid client session state: {message}")
            }
            Self::StateAlreadyOwned => {
                formatter.write_str("client session state is already owned by another process")
            }
            Self::OperationBlocked(message) => formatter.write_str(message),
            Self::SequenceExhausted => formatter.write_str("client command sequence is exhausted"),
        }
    }
}

impl Error for ClientSessionStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Protocol(error) => Some(error),
            Self::InvalidState(_)
            | Self::StateAlreadyOwned
            | Self::OperationBlocked(_)
            | Self::SequenceExhausted => None,
        }
    }
}

impl From<ProtocolCodecError> for ClientSessionStoreError {
    fn from(error: ProtocolCodecError) -> Self {
        Self::Protocol(error)
    }
}

/// Broker owner acquisition that was durably reserved before any network send.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PendingOwnerAcquisition {
    session_id: CommandSessionId,
    expected_owner_epoch: SessionOwnerEpoch,
    owner_instance_id: SessionOwnerInstanceId,
}

impl PendingOwnerAcquisition {
    #[must_use]
    pub const fn session_id(&self) -> &CommandSessionId {
        &self.session_id
    }

    #[must_use]
    pub const fn expected_owner_epoch(&self) -> SessionOwnerEpoch {
        self.expected_owner_epoch
    }

    #[must_use]
    pub const fn owner_instance_id(&self) -> &SessionOwnerInstanceId {
        &self.owner_instance_id
    }
}

/// Exact owner-aware mutation identity and request durably reserved before network submission.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReservedCommand {
    identity: CommandIdentity,
    request: BrokerRequest,
}

impl ReservedCommand {
    #[must_use]
    pub const fn identity(&self) -> &CommandIdentity {
        &self.identity
    }

    #[must_use]
    pub const fn request(&self) -> &BrokerRequest {
        &self.request
    }
}

/// Current durable owner identity for one logical command session.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DurableSessionOwner {
    owner_epoch: SessionOwnerEpoch,
    owner_instance_id: SessionOwnerInstanceId,
}

impl DurableSessionOwner {
    #[must_use]
    pub const fn owner_epoch(&self) -> SessionOwnerEpoch {
        self.owner_epoch
    }

    #[must_use]
    pub const fn owner_instance_id(&self) -> &SessionOwnerInstanceId {
        &self.owner_instance_id
    }
}

/// Exclusive, crash-recoverable local state for one protocol-v3 command session.
///
/// The store never sends network requests and never retries automatically. It only establishes the
/// local write-ahead ordering required for callers to safely retry the exact persisted identity.
#[derive(Debug)]
pub struct DurableClientSessionStore {
    path: PathBuf,
    lock_path: PathBuf,
    lock_file: File,
    session_id: CommandSessionId,
    state: PersistedSessionState,
}

impl DurableClientSessionStore {
    /// Open or create one durable logical command session and acquire exclusive local ownership.
    ///
    /// # Errors
    ///
    /// Returns an I/O error, [`ClientSessionStoreError::StateAlreadyOwned`] when another process has
    /// the state locked, or [`ClientSessionStoreError::InvalidState`] for corrupt/incompatible state.
    pub fn open_or_create(
        path: impl AsRef<Path>,
        session_id: CommandSessionId,
    ) -> Result<Self, ClientSessionStoreError> {
        let path = path.as_ref().to_path_buf();
        let (lock_path, lock_file) = acquire_state_lock(&path)?;
        let state = if path.try_exists().map_err(|error| {
            ClientSessionStoreError::io("client session existence check failed", error)
        })? {
            load_state(&path, &session_id)?
        } else {
            let state = PersistedSessionState::new(&session_id);
            persist_state(&path, &state)?;
            state
        };
        Ok(Self {
            path,
            lock_path,
            lock_file,
            session_id,
            state,
        })
    }

    #[must_use]
    pub fn session_id(&self) -> &CommandSessionId {
        &self.session_id
    }

    #[must_use]
    pub fn state_path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }

    /// Return the next durable command sequence that will be reserved for the confirmed owner.
    ///
    /// # Errors
    ///
    /// Returns invalid-state if the persisted sequence is not a valid non-zero command sequence.
    pub fn next_sequence(&self) -> Result<CommandSequence, ClientSessionStoreError> {
        CommandSequence::new(self.state.next_sequence)
            .map_err(|error| ClientSessionStoreError::InvalidState(error.to_string()))
    }

    /// Return the currently confirmed Broker owner, if acquisition has completed locally.
    ///
    /// # Errors
    ///
    /// Returns invalid-state if persisted owner fields fail validation.
    pub fn owner(&self) -> Result<Option<DurableSessionOwner>, ClientSessionStoreError> {
        self.state.owner.as_ref().map(decode_owner).transpose()
    }

    /// Return a durably pending owner acquisition that must be retried with the exact same fields.
    ///
    /// # Errors
    ///
    /// Returns invalid-state if persisted acquisition fields fail validation.
    pub fn pending_owner_acquisition(
        &self,
    ) -> Result<Option<PendingOwnerAcquisition>, ClientSessionStoreError> {
        self.state
            .pending_owner_acquisition
            .as_ref()
            .map(|pending| decode_pending(self.session_id(), pending))
            .transpose()
    }

    /// Durably reserve a Broker owner acquisition before any network send.
    ///
    /// A pending mutation must be recovered before takeover because Broker acquisition clears the
    /// previous owner's recoverable outcome.
    ///
    /// # Errors
    ///
    /// Returns operation-blocked while a mutation/acquisition is unresolved, or durability errors.
    pub fn begin_owner_acquisition(
        &mut self,
        owner_instance_id: SessionOwnerInstanceId,
    ) -> Result<PendingOwnerAcquisition, ClientSessionStoreError> {
        if self.state.in_flight.is_some() {
            return Err(ClientSessionStoreError::OperationBlocked(
                "cannot acquire a new session owner while an in-flight command is unresolved",
            ));
        }
        if self.state.pending_owner_acquisition.is_some() {
            return Err(ClientSessionStoreError::OperationBlocked(
                "session owner acquisition is already pending",
            ));
        }
        let current_owner = self.owner()?;
        if current_owner
            .as_ref()
            .is_some_and(|owner| owner.owner_instance_id == owner_instance_id)
        {
            return Err(ClientSessionStoreError::OperationBlocked(
                "confirmed owner instance must continue its current sequence domain instead of reacquiring ownership",
            ));
        }
        let expected_owner_epoch = current_owner
            .as_ref()
            .map_or(SessionOwnerEpoch::INITIAL, |owner| owner.owner_epoch);
        let pending = PendingOwnerAcquisition {
            session_id: self.session_id().clone(),
            expected_owner_epoch,
            owner_instance_id,
        };
        let mut next = self.state.clone();
        next.pending_owner_acquisition = Some(PersistedPendingOwnerAcquisition::from(&pending));
        self.commit_state(next)?;
        Ok(pending)
    }

    /// Commit a Broker-confirmed owner acquisition into local durable state.
    ///
    /// The Broker may return the expected epoch for a newly bootstrapped v3 session, or exactly
    /// `expected + 1` when fencing a pre-existing owner. Any other epoch is fail-closed.
    ///
    /// # Errors
    ///
    /// Returns invalid-state for a mismatched Broker response or when no acquisition is pending.
    pub fn confirm_owner_acquisition(
        &mut self,
        owner_epoch: SessionOwnerEpoch,
    ) -> Result<DurableSessionOwner, ClientSessionStoreError> {
        let pending = self.pending_owner_acquisition()?.ok_or_else(|| {
            ClientSessionStoreError::InvalidState(
                "owner acquisition response received without a durable pending acquisition"
                    .to_owned(),
            )
        })?;
        validate_acquired_epoch(
            pending.expected_owner_epoch,
            owner_epoch,
            self.state.owner.is_some(),
        )?;
        let owner = DurableSessionOwner {
            owner_epoch,
            owner_instance_id: pending.owner_instance_id,
        };
        let mut next = self.state.clone();
        next.owner = Some(PersistedOwner::from(&owner));
        next.pending_owner_acquisition = None;
        next.next_sequence = 1;
        next.in_flight = None;
        self.commit_state(next)?;
        Ok(owner)
    }

    /// Durably reserve one exact owner-aware mutation before it may be sent to the Broker.
    ///
    /// # Errors
    ///
    /// Returns operation-blocked until ownership is confirmed and all prior ambiguity is resolved,
    /// protocol errors while encoding the request, sequence exhaustion, or durability errors.
    pub fn reserve_command(
        &mut self,
        request: BrokerRequest,
    ) -> Result<ReservedCommand, ClientSessionStoreError> {
        if self.state.pending_owner_acquisition.is_some() {
            return Err(ClientSessionStoreError::OperationBlocked(
                "cannot reserve a command while owner acquisition is unresolved",
            ));
        }
        if self.state.in_flight.is_some() {
            return Err(ClientSessionStoreError::OperationBlocked(
                "cannot reserve a second command while an in-flight command is unresolved",
            ));
        }
        if request.operation() == Operation::Health {
            return Err(ClientSessionStoreError::OperationBlocked(
                "durable command sessions are mutation-only",
            ));
        }
        if self.state.next_sequence == u64::MAX {
            return Err(ClientSessionStoreError::SequenceExhausted);
        }
        let owner = self
            .owner()?
            .ok_or(ClientSessionStoreError::OperationBlocked(
                "cannot reserve a command before Broker owner acquisition",
            ))?;
        let sequence = CommandSequence::new(self.state.next_sequence)
            .map_err(|error| ClientSessionStoreError::InvalidState(error.to_string()))?;
        let identity = CommandIdentity::new_with_owner(
            self.session_id().clone(),
            owner.owner_epoch,
            owner.owner_instance_id,
            sequence,
        );
        let request_frame = String::from_utf8(encode_request(&request)?)
            .map_err(|error| ClientSessionStoreError::InvalidState(error.to_string()))?;
        let reserved = ReservedCommand { identity, request };
        let mut next = self.state.clone();
        next.in_flight = Some(PersistedInFlight::from_reserved(&reserved, request_frame));
        self.commit_state(next)?;
        Ok(reserved)
    }

    /// Recover the exact mutation identity/request that remains ambiguous after a crash or response
    /// loss. The caller may explicitly retry this value; the store never retries on its own.
    ///
    /// # Errors
    ///
    /// Returns invalid-state or protocol errors if the persisted in-flight record is corrupt.
    pub fn in_flight(&self) -> Result<Option<ReservedCommand>, ClientSessionStoreError> {
        self.state
            .in_flight
            .as_ref()
            .map(|in_flight| decode_in_flight(self.session_id(), in_flight))
            .transpose()
    }

    /// Acknowledge that the caller has observed a definitive Broker outcome for the exact persisted
    /// command. This durably advances the sequence and clears ambiguity before another command can be
    /// reserved.
    ///
    /// # Errors
    ///
    /// Returns invalid-state when `identity` is not the current persisted in-flight command, or on
    /// durability failure.
    pub fn acknowledge_in_flight_outcome(
        &mut self,
        identity: &CommandIdentity,
    ) -> Result<(), ClientSessionStoreError> {
        let in_flight = self.in_flight()?.ok_or_else(|| {
            ClientSessionStoreError::InvalidState(
                "cannot acknowledge an outcome without an in-flight command".to_owned(),
            )
        })?;
        if in_flight.identity() != identity {
            return Err(ClientSessionStoreError::InvalidState(
                "outcome identity does not match durable in-flight command".to_owned(),
            ));
        }
        let next_sequence = identity
            .sequence()
            .get()
            .checked_add(1)
            .ok_or(ClientSessionStoreError::SequenceExhausted)?;
        let mut next = self.state.clone();
        next.next_sequence = next_sequence;
        next.in_flight = None;
        self.commit_state(next)
    }

    /// Durably release an exact in-flight command after the Broker has explicitly classified it as
    /// rejected before authoritative command-outcome storage. The sequence is intentionally not
    /// advanced so a caller may correct the rejected condition without silently consuming a slot.
    ///
    /// This method must never be used for `UNKNOWN` outcomes.
    ///
    /// # Errors
    ///
    /// Returns invalid-state when `identity` does not match the current durable in-flight command,
    /// or on durability failure.
    pub fn release_rejected_in_flight(
        &mut self,
        identity: &CommandIdentity,
    ) -> Result<(), ClientSessionStoreError> {
        let in_flight = self.in_flight()?.ok_or_else(|| {
            ClientSessionStoreError::InvalidState(
                "cannot release a rejected command without an in-flight command".to_owned(),
            )
        })?;
        if in_flight.identity() != identity {
            return Err(ClientSessionStoreError::InvalidState(
                "rejected outcome identity does not match durable in-flight command".to_owned(),
            ));
        }
        let mut next = self.state.clone();
        next.in_flight = None;
        self.commit_state(next)
    }

    fn commit_state(&mut self, next: PersistedSessionState) -> Result<(), ClientSessionStoreError> {
        validate_state(&next, self.session_id())?;
        persist_state(&self.path, &next)?;
        self.state = next;
        Ok(())
    }
}

impl Drop for DurableClientSessionStore {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedSessionState {
    version: u32,
    session_id: String,
    owner: Option<PersistedOwner>,
    pending_owner_acquisition: Option<PersistedPendingOwnerAcquisition>,
    next_sequence: u64,
    in_flight: Option<PersistedInFlight>,
}

impl PersistedSessionState {
    fn new(session_id: &CommandSessionId) -> Self {
        Self {
            version: SESSION_STORE_VERSION,
            session_id: session_id.as_str().to_owned(),
            owner: None,
            pending_owner_acquisition: None,
            next_sequence: 1,
            in_flight: None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedOwner {
    owner_epoch: u64,
    owner_instance_id: String,
}

impl From<&DurableSessionOwner> for PersistedOwner {
    fn from(owner: &DurableSessionOwner) -> Self {
        Self {
            owner_epoch: owner.owner_epoch.get(),
            owner_instance_id: owner.owner_instance_id.as_str().to_owned(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPendingOwnerAcquisition {
    expected_owner_epoch: u64,
    owner_instance_id: String,
}

impl From<&PendingOwnerAcquisition> for PersistedPendingOwnerAcquisition {
    fn from(pending: &PendingOwnerAcquisition) -> Self {
        Self {
            expected_owner_epoch: pending.expected_owner_epoch.get(),
            owner_instance_id: pending.owner_instance_id.as_str().to_owned(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedInFlight {
    owner_epoch: u64,
    owner_instance_id: String,
    sequence: u64,
    request_frame: String,
}

impl PersistedInFlight {
    fn from_reserved(reserved: &ReservedCommand, request_frame: String) -> Self {
        Self {
            owner_epoch: reserved.identity.owner_epoch().get(),
            owner_instance_id: reserved
                .identity
                .owner_instance_id()
                .map_or_else(String::new, |owner| owner.as_str().to_owned()),
            sequence: reserved.identity.sequence().get(),
            request_frame,
        }
    }
}

fn load_state(
    path: &Path,
    expected_session_id: &CommandSessionId,
) -> Result<PersistedSessionState, ClientSessionStoreError> {
    let encoded = fs::read(path)
        .map_err(|error| ClientSessionStoreError::io("client session state read failed", error))?;
    let state: PersistedSessionState = serde_json::from_slice(&encoded).map_err(|error| {
        ClientSessionStoreError::InvalidState(format!("state JSON decode failed: {error}"))
    })?;
    validate_state(&state, expected_session_id)?;
    Ok(state)
}

fn validate_state(
    state: &PersistedSessionState,
    expected_session_id: &CommandSessionId,
) -> Result<(), ClientSessionStoreError> {
    if state.version != SESSION_STORE_VERSION {
        return Err(ClientSessionStoreError::InvalidState(format!(
            "unsupported version {}",
            state.version
        )));
    }
    let stored_session_id = CommandSessionId::new(state.session_id.clone())
        .map_err(|error| ClientSessionStoreError::InvalidState(error.to_string()))?;
    if &stored_session_id != expected_session_id {
        return Err(ClientSessionStoreError::InvalidState(format!(
            "session id mismatch: expected {expected_session_id}, found {}",
            state.session_id
        )));
    }
    CommandSequence::new(state.next_sequence)
        .map_err(|error| ClientSessionStoreError::InvalidState(error.to_string()))?;
    let owner = state.owner.as_ref().map(decode_owner).transpose()?;
    let pending = state
        .pending_owner_acquisition
        .as_ref()
        .map(|pending| decode_pending(expected_session_id, pending))
        .transpose()?;
    let in_flight = state
        .in_flight
        .as_ref()
        .map(|in_flight| decode_in_flight(expected_session_id, in_flight))
        .transpose()?;
    validate_state_relationships(state, owner.as_ref(), pending.as_ref(), in_flight.as_ref())
}

fn validate_state_relationships(
    state: &PersistedSessionState,
    owner: Option<&DurableSessionOwner>,
    pending: Option<&PendingOwnerAcquisition>,
    in_flight: Option<&ReservedCommand>,
) -> Result<(), ClientSessionStoreError> {
    if pending.is_some() && in_flight.is_some() {
        return Err(ClientSessionStoreError::InvalidState(
            "owner acquisition and command cannot both be in flight".to_owned(),
        ));
    }
    if owner.is_none() && state.next_sequence != 1 {
        return Err(ClientSessionStoreError::InvalidState(
            "unowned session must keep next_sequence at 1".to_owned(),
        ));
    }
    if let (Some(owner), Some(in_flight)) = (owner, in_flight) {
        if in_flight.identity().owner_epoch() != owner.owner_epoch
            || in_flight.identity().owner_instance_id() != Some(&owner.owner_instance_id)
            || in_flight.identity().sequence().get() != state.next_sequence
        {
            return Err(ClientSessionStoreError::InvalidState(
                "in-flight command identity does not match durable owner/next sequence".to_owned(),
            ));
        }
    } else if in_flight.is_some() {
        return Err(ClientSessionStoreError::InvalidState(
            "in-flight command requires a confirmed owner".to_owned(),
        ));
    }
    Ok(())
}

fn decode_owner(owner: &PersistedOwner) -> Result<DurableSessionOwner, ClientSessionStoreError> {
    Ok(DurableSessionOwner {
        owner_epoch: SessionOwnerEpoch::new(owner.owner_epoch)
            .map_err(|error| ClientSessionStoreError::InvalidState(error.to_string()))?,
        owner_instance_id: SessionOwnerInstanceId::new(owner.owner_instance_id.clone())
            .map_err(|error| ClientSessionStoreError::InvalidState(error.to_string()))?,
    })
}

fn decode_pending(
    session_id: &CommandSessionId,
    pending: &PersistedPendingOwnerAcquisition,
) -> Result<PendingOwnerAcquisition, ClientSessionStoreError> {
    Ok(PendingOwnerAcquisition {
        session_id: session_id.clone(),
        expected_owner_epoch: SessionOwnerEpoch::new(pending.expected_owner_epoch)
            .map_err(|error| ClientSessionStoreError::InvalidState(error.to_string()))?,
        owner_instance_id: SessionOwnerInstanceId::new(pending.owner_instance_id.clone())
            .map_err(|error| ClientSessionStoreError::InvalidState(error.to_string()))?,
    })
}

fn decode_in_flight(
    session_id: &CommandSessionId,
    in_flight: &PersistedInFlight,
) -> Result<ReservedCommand, ClientSessionStoreError> {
    let owner_epoch = SessionOwnerEpoch::new(in_flight.owner_epoch)
        .map_err(|error| ClientSessionStoreError::InvalidState(error.to_string()))?;
    let owner_instance_id = SessionOwnerInstanceId::new(in_flight.owner_instance_id.clone())
        .map_err(|error| ClientSessionStoreError::InvalidState(error.to_string()))?;
    let sequence = CommandSequence::new(in_flight.sequence)
        .map_err(|error| ClientSessionStoreError::InvalidState(error.to_string()))?;
    let request = decode_request(in_flight.request_frame.as_bytes())?;
    if request.operation() == Operation::Health {
        return Err(ClientSessionStoreError::InvalidState(
            "in-flight durable command cannot be health".to_owned(),
        ));
    }
    Ok(ReservedCommand {
        identity: CommandIdentity::new_with_owner(
            session_id.clone(),
            owner_epoch,
            owner_instance_id,
            sequence,
        ),
        request,
    })
}

fn validate_acquired_epoch(
    expected: SessionOwnerEpoch,
    actual: SessionOwnerEpoch,
    replacing_confirmed_owner: bool,
) -> Result<(), ClientSessionStoreError> {
    let advanced = expected.get().checked_add(1);
    if replacing_confirmed_owner && advanced == Some(actual.get()) {
        return Ok(());
    }
    if !replacing_confirmed_owner && (actual == expected || advanced == Some(actual.get())) {
        return Ok(());
    }
    Err(ClientSessionStoreError::InvalidState(format!(
        "Broker returned owner epoch {} for expected epoch {}",
        actual.get(),
        expected.get()
    )))
}

fn persist_state(
    path: &Path,
    state: &PersistedSessionState,
) -> Result<(), ClientSessionStoreError> {
    let parent = ensure_parent(path)?;
    let encoded = serde_json::to_vec(state).map_err(|error| {
        ClientSessionStoreError::InvalidState(format!("state JSON encode failed: {error}"))
    })?;
    let mut temporary = Builder::new()
        .prefix(".agent-broker-client-session.")
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|error| {
            ClientSessionStoreError::io("client session tempfile creation failed", error)
        })?;
    secure_file_mode(temporary.as_file())?;
    temporary.write_all(&encoded).map_err(|error| {
        ClientSessionStoreError::io("client session tempfile write failed", error)
    })?;
    temporary.flush().map_err(|error| {
        ClientSessionStoreError::io("client session tempfile flush failed", error)
    })?;
    fsync_compatible(temporary.as_file()).map_err(|error| {
        ClientSessionStoreError::io("client session tempfile fsync failed", error)
    })?;
    temporary.persist(path).map_err(|error| {
        ClientSessionStoreError::io("client session atomic replace failed", error.error)
    })?;
    sync_directory(parent)
}

fn acquire_state_lock(path: &Path) -> Result<(PathBuf, File), ClientSessionStoreError> {
    let parent = ensure_parent(path)?;
    let lock_path = lock_path_for(path);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(&lock_path)
        .map_err(|error| ClientSessionStoreError::io("client session lock open failed", error))?;
    match file.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Err(ClientSessionStoreError::StateAlreadyOwned),
        Err(TryLockError::Error(error)) => {
            return Err(ClientSessionStoreError::io(
                "client session lock acquisition failed",
                error,
            ));
        }
    }
    sync_directory(parent)?;
    Ok((lock_path, file))
}

fn lock_path_for(path: &Path) -> PathBuf {
    let mut extension = path
        .extension()
        .map_or_else(OsString::new, std::ffi::OsStr::to_os_string);
    if extension.is_empty() {
        extension.push("lock");
    } else {
        extension.push(".lock");
    }
    path.with_extension(extension)
}

fn ensure_parent(path: &Path) -> Result<&Path, ClientSessionStoreError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| {
        ClientSessionStoreError::io("client session directory creation failed", error)
    })?;
    Ok(parent)
}

fn sync_directory(directory: &Path) -> Result<(), ClientSessionStoreError> {
    let handle = File::open(directory).map_err(|error| {
        ClientSessionStoreError::io("client session directory open failed", error)
    })?;
    fsync_compatible(&handle).map_err(|error| {
        ClientSessionStoreError::io("client session directory fsync failed", error)
    })
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

fn secure_file_mode(file: &File) -> Result<(), ClientSessionStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|error| {
                ClientSessionStoreError::io("client session file mode setup failed", error)
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use agent_broker_application::{CommandSessionId, SessionOwnerEpoch, SessionOwnerInstanceId};
    use agent_broker_domain::NamespaceId;
    use agent_broker_protocol::{BrokerRequest, EnsureNamespaceRequest, RequestId};
    use tempfile::tempdir;

    use super::{ClientSessionStoreError, DurableClientSessionStore};

    #[test]
    fn reserved_command_survives_reopen_and_blocks_takeover_until_acknowledged()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("client-session.json");
        let session_id = CommandSessionId::new("durable-client-session")?;
        let owner_instance = SessionOwnerInstanceId::new("durable-client-process-a")?;
        let request = BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
            request_id: RequestId::new("durable-client-request-1")?,
            namespace_id: NamespaceId::new("durable-client-namespace")?,
        });

        let reserved = {
            let mut store = DurableClientSessionStore::open_or_create(&path, session_id.clone())?;
            let pending = store.begin_owner_acquisition(owner_instance.clone())?;
            assert_eq!(pending.expected_owner_epoch(), SessionOwnerEpoch::INITIAL);
            let owner = store.confirm_owner_acquisition(SessionOwnerEpoch::INITIAL)?;
            assert_eq!(owner.owner_instance_id(), &owner_instance);
            store.reserve_command(request.clone())?
        };

        let mut reopened = DurableClientSessionStore::open_or_create(&path, session_id)?;
        assert_eq!(reopened.in_flight()?, Some(reserved.clone()));
        let blocked = reopened
            .begin_owner_acquisition(SessionOwnerInstanceId::new("durable-client-process-b")?);
        assert!(matches!(
            blocked,
            Err(ClientSessionStoreError::OperationBlocked(_))
        ));
        reopened.acknowledge_in_flight_outcome(reserved.identity())?;
        assert!(reopened.in_flight()?.is_none());
        assert!(matches!(
            reopened.begin_owner_acquisition(owner_instance),
            Err(ClientSessionStoreError::OperationBlocked(_))
        ));
        let takeover = reopened
            .begin_owner_acquisition(SessionOwnerInstanceId::new("durable-client-process-b")?)?;
        assert_eq!(takeover.expected_owner_epoch(), SessionOwnerEpoch::INITIAL);
        assert!(matches!(
            reopened.confirm_owner_acquisition(SessionOwnerEpoch::INITIAL),
            Err(ClientSessionStoreError::InvalidState(_))
        ));
        let owner_two = reopened.confirm_owner_acquisition(SessionOwnerEpoch::new(2)?)?;
        assert_eq!(owner_two.owner_epoch().get(), 2);
        Ok(())
    }

    #[test]
    fn pending_owner_acquisition_survives_reopen_for_exact_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("pending-owner.json");
        let session_id = CommandSessionId::new("durable-client-pending-owner")?;
        let owner_instance = SessionOwnerInstanceId::new("durable-client-pending-process")?;
        let pending = {
            let mut store = DurableClientSessionStore::open_or_create(&path, session_id.clone())?;
            store.begin_owner_acquisition(owner_instance)?
        };

        let mut reopened = DurableClientSessionStore::open_or_create(&path, session_id)?;
        assert_eq!(reopened.pending_owner_acquisition()?, Some(pending));
        assert!(reopened.owner()?.is_none());
        let owner = reopened.confirm_owner_acquisition(SessionOwnerEpoch::INITIAL)?;
        assert_eq!(owner.owner_epoch(), SessionOwnerEpoch::INITIAL);
        assert!(reopened.pending_owner_acquisition()?.is_none());
        Ok(())
    }

    #[test]
    fn exclusive_state_lock_rejects_second_local_owner() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("client-session.json");
        let session_id = CommandSessionId::new("durable-client-lock")?;
        let first = DurableClientSessionStore::open_or_create(&path, session_id.clone())?;
        let second = DurableClientSessionStore::open_or_create(&path, session_id);
        assert!(matches!(
            second,
            Err(ClientSessionStoreError::StateAlreadyOwned)
        ));
        drop(first);
        Ok(())
    }

    #[test]
    fn corrupt_or_cross_session_state_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("client-session.json");
        let session_id = CommandSessionId::new("durable-client-corrupt")?;
        {
            let _store = DurableClientSessionStore::open_or_create(&path, session_id.clone())?;
        }
        std::fs::write(&path, b"{not-json")?;
        let corrupt = DurableClientSessionStore::open_or_create(&path, session_id);
        assert!(matches!(
            corrupt,
            Err(ClientSessionStoreError::InvalidState(_))
        ));

        let other_path = directory.path().join("other-session.json");
        let expected = CommandSessionId::new("durable-client-expected")?;
        {
            let _store = DurableClientSessionStore::open_or_create(
                &other_path,
                CommandSessionId::new("durable-client-actual")?,
            )?;
        }
        let mismatch = DurableClientSessionStore::open_or_create(&other_path, expected);
        assert!(matches!(
            mismatch,
            Err(ClientSessionStoreError::InvalidState(_))
        ));
        Ok(())
    }
}

use std::fmt;
use std::num::NonZeroU64;

const MAX_COMMAND_SESSION_ID_BYTES: usize = 128;
const MAX_OWNER_INSTANCE_ID_BYTES: usize = 128;

/// Stable provider-neutral client session identity used only for idempotent consensus mutation ordering.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CommandSessionId(String);

impl CommandSessionId {
    /// Construct a bounded ASCII command session identifier.
    ///
    /// # Errors
    ///
    /// Returns [`CommandIdentityError`] when the identifier is empty, too long, starts with an
    /// unsupported character, or contains characters outside the Broker identifier alphabet.
    pub fn new(value: impl Into<String>) -> Result<Self, CommandIdentityError> {
        let value = value.into();
        validate_session_id(&value)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CommandSessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Monotonic non-zero sequence within one command session.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CommandSequence(NonZeroU64);

impl CommandSequence {
    /// Construct a non-zero command sequence.
    ///
    /// # Errors
    ///
    /// Returns [`CommandIdentityError`] for sequence zero.
    pub fn new(value: u64) -> Result<Self, CommandIdentityError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(CommandIdentityError::ZeroSequence)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Broker-authoritative owner incarnation for one logical command session.
///
/// Epoch `1` is the initial owner. Higher epochs are reserved for an explicit committed ownership
/// transition; callers must not treat a larger number as self-authorizing takeover.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SessionOwnerEpoch(NonZeroU64);

impl SessionOwnerEpoch {
    pub const INITIAL: Self = Self(NonZeroU64::MIN);

    /// Construct a non-zero session-owner epoch.
    ///
    /// # Errors
    ///
    /// Returns [`CommandIdentityError`] for epoch zero.
    pub fn new(value: u64) -> Result<Self, CommandIdentityError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(CommandIdentityError::ZeroOwnerEpoch)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Stable identity of one concrete process/incarnation that owns a logical command session.
///
/// A new process taking over the same logical session must use a new instance ID. This value is
/// compared with broker-authoritative replicated ownership state; it is not a correlation ID.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SessionOwnerInstanceId(String);

impl SessionOwnerInstanceId {
    /// Construct a bounded ASCII owner-instance identifier.
    ///
    /// # Errors
    ///
    /// Returns [`CommandIdentityError`] when the identifier is empty, too long, starts with an
    /// unsupported character, or contains characters outside the Broker identifier alphabet.
    pub fn new(value: impl Into<String>) -> Result<Self, CommandIdentityError> {
        let value = value.into();
        validate_identifier(&value, MAX_OWNER_INSTANCE_ID_BYTES)
            .map_err(CommandIdentityError::InvalidOwnerInstanceId)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionOwnerInstanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Stable mutation identity. `request_id` remains correlation-only and must not be substituted for this value.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CommandIdentity {
    session_id: CommandSessionId,
    owner_epoch: SessionOwnerEpoch,
    owner_instance_id: Option<SessionOwnerInstanceId>,
    sequence: CommandSequence,
}

impl CommandIdentity {
    #[must_use]
    pub const fn new(session_id: CommandSessionId, sequence: CommandSequence) -> Self {
        Self {
            session_id,
            owner_epoch: SessionOwnerEpoch::INITIAL,
            owner_instance_id: None,
            sequence,
        }
    }

    #[must_use]
    pub const fn new_with_owner_epoch(
        session_id: CommandSessionId,
        owner_epoch: SessionOwnerEpoch,
        sequence: CommandSequence,
    ) -> Self {
        Self {
            session_id,
            owner_epoch,
            owner_instance_id: None,
            sequence,
        }
    }

    #[must_use]
    pub const fn new_with_owner(
        session_id: CommandSessionId,
        owner_epoch: SessionOwnerEpoch,
        owner_instance_id: SessionOwnerInstanceId,
        sequence: CommandSequence,
    ) -> Self {
        Self {
            session_id,
            owner_epoch,
            owner_instance_id: Some(owner_instance_id),
            sequence,
        }
    }

    #[must_use]
    pub const fn session_id(&self) -> &CommandSessionId {
        &self.session_id
    }

    #[must_use]
    pub const fn owner_epoch(&self) -> SessionOwnerEpoch {
        self.owner_epoch
    }

    #[must_use]
    pub const fn owner_instance_id(&self) -> Option<&SessionOwnerInstanceId> {
        self.owner_instance_id.as_ref()
    }

    #[must_use]
    pub const fn sequence(&self) -> CommandSequence {
        self.sequence
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CommandIdentityError {
    InvalidSessionId(&'static str),
    InvalidOwnerInstanceId(&'static str),
    ZeroOwnerEpoch,
    ZeroSequence,
}

impl fmt::Display for CommandIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSessionId(reason) => {
                write!(formatter, "invalid command_session_id: {reason}")
            }
            Self::InvalidOwnerInstanceId(reason) => {
                write!(
                    formatter,
                    "invalid command_session_owner_instance_id: {reason}"
                )
            }
            Self::ZeroOwnerEpoch => {
                formatter.write_str("command_session_owner_epoch must be greater than zero")
            }
            Self::ZeroSequence => formatter.write_str("command_sequence must be greater than zero"),
        }
    }
}

impl std::error::Error for CommandIdentityError {}

fn validate_session_id(value: &str) -> Result<(), CommandIdentityError> {
    validate_identifier(value, MAX_COMMAND_SESSION_ID_BYTES)
        .map_err(CommandIdentityError::InvalidSessionId)
}

fn validate_identifier(value: &str, max_bytes: usize) -> Result<(), &'static str> {
    if value.is_empty() {
        return Err("must not be empty");
    }
    if value.len() > max_bytes {
        return Err("must be at most 128 ASCII bytes");
    }
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return Err("must not be empty");
    };
    if !first.is_ascii_alphanumeric() {
        return Err("must start with an ASCII alphanumeric character");
    }
    if !bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-'))
    {
        return Err("contains unsupported characters");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CommandIdentity, CommandSequence, CommandSessionId, SessionOwnerEpoch,
        SessionOwnerInstanceId,
    };

    #[test]
    fn command_identity_requires_stable_bounded_session_and_nonzero_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let session = CommandSessionId::new("orchestrator-a:session-1")?;
        let sequence = CommandSequence::new(7)?;
        let identity = CommandIdentity::new(session.clone(), sequence);
        assert_eq!(identity.session_id(), &session);
        assert_eq!(identity.owner_epoch(), SessionOwnerEpoch::INITIAL);
        assert_eq!(identity.sequence().get(), 7);
        let next_owner = SessionOwnerEpoch::new(2)?;
        let next_identity = CommandIdentity::new_with_owner_epoch(session, next_owner, sequence);
        assert_eq!(next_identity.owner_epoch(), next_owner);
        assert_eq!(next_identity.owner_instance_id(), None);
        let owner_instance = SessionOwnerInstanceId::new("worker-process-2")?;
        let owned_identity = CommandIdentity::new_with_owner(
            next_identity.session_id().clone(),
            next_owner,
            owner_instance.clone(),
            sequence,
        );
        assert_eq!(owned_identity.owner_instance_id(), Some(&owner_instance));
        assert!(CommandSessionId::new("-bad").is_err());
        assert!(SessionOwnerInstanceId::new("-bad").is_err());
        assert!(SessionOwnerEpoch::new(0).is_err());
        assert!(CommandSequence::new(0).is_err());
        Ok(())
    }
}

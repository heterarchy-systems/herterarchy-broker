use std::error::Error;
use std::fmt;

use agent_broker_domain::{ConsumerGroupError, StateMachineError, TaskTransitionError};

/// Stable protocol-facing Broker error codes preserved from the Python reference implementation.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum BrokerErrorCode {
    InvalidRequest,
    NotFound,
    Conflict,
    CapacityExceeded,
    StaleFence,
    PersistenceError,
    TransportError,
    CommitOutcomeUnknown,
    InternalError,
}

/// Whether a client-visible mutation error is a committed command outcome, a definitive
/// pre-application rejection, or still ambiguous.
///
/// Protocol-v1/v2 intentionally ignore this metadata. Newer owner-aware protocol generations may
/// use it to decide whether a durable local command sequence can advance safely.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Default)]
pub enum BrokerErrorDisposition {
    Committed,
    Rejected,
    #[default]
    Unknown,
}

impl BrokerErrorDisposition {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "COMMITTED",
            Self::Rejected => "REJECTED",
            Self::Unknown => "UNKNOWN",
        }
    }
}

impl BrokerErrorCode {
    /// Return the stable wire-compatible error code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "INVALID_REQUEST",
            Self::NotFound => "NOT_FOUND",
            Self::Conflict => "CONFLICT",
            Self::CapacityExceeded => "CAPACITY_EXCEEDED",
            Self::StaleFence => "STALE_FENCE",
            Self::PersistenceError => "PERSISTENCE_ERROR",
            Self::TransportError => "TRANSPORT_ERROR",
            Self::CommitOutcomeUnknown => "COMMIT_OUTCOME_UNKNOWN",
            Self::InternalError => "INTERNAL_ERROR",
        }
    }
}

/// Stable application/protocol error independent of transport and consensus implementation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrokerError {
    code: BrokerErrorCode,
    message: String,
    disposition: BrokerErrorDisposition,
}

impl BrokerError {
    /// Construct a stable Broker error.
    #[must_use]
    pub fn new(code: BrokerErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            disposition: BrokerErrorDisposition::Unknown,
        }
    }

    /// Attach commit-aware disposition without changing the stable error code/message contract.
    #[must_use]
    pub fn with_disposition(mut self, disposition: BrokerErrorDisposition) -> Self {
        self.disposition = disposition;
        self
    }

    /// Return the typed stable error code.
    #[must_use]
    pub const fn code(&self) -> BrokerErrorCode {
        self.code
    }

    /// Borrow the human-readable error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Return whether this error is authoritative, definitively rejected before command outcome
    /// storage, or still ambiguous.
    #[must_use]
    pub const fn disposition(&self) -> BrokerErrorDisposition {
        self.disposition
    }
}

impl fmt::Display for BrokerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for BrokerError {}

impl From<StateMachineError> for BrokerError {
    fn from(error: StateMachineError) -> Self {
        let code = state_machine_error_code(&error);
        Self::new(code, error.to_string())
    }
}

fn state_machine_error_code(error: &StateMachineError) -> BrokerErrorCode {
    match error {
        StateMachineError::InvalidCapacity { .. } | StateMachineError::TimestampOverflow => {
            BrokerErrorCode::InvalidRequest
        }
        StateMachineError::CapacityExceeded { .. } => BrokerErrorCode::CapacityExceeded,
        StateMachineError::NamespaceNotFound(_)
        | StateMachineError::TaskNotFound(_)
        | StateMachineError::ConsumerGroupNotFound(_) => BrokerErrorCode::NotFound,
        StateMachineError::TaskConflict(_)
        | StateMachineError::LeaseIdConflict(_)
        | StateMachineError::ConsumerGroupConflict(_) => BrokerErrorCode::Conflict,
        StateMachineError::StaleTerm { .. } | StateMachineError::NewTermNotGreater { .. } => {
            BrokerErrorCode::StaleFence
        }
        StateMachineError::ConsumerGroupTransition(error) => consumer_group_error_code(error),
        StateMachineError::TaskTransition(error) => task_transition_error_code(error),
        StateMachineError::TaskCountUnderflow(_) | StateMachineError::FencingValue(_) => {
            BrokerErrorCode::InternalError
        }
    }
}

fn consumer_group_error_code(error: &ConsumerGroupError) -> BrokerErrorCode {
    match error {
        ConsumerGroupError::StaleGeneration { .. } => BrokerErrorCode::StaleFence,
        ConsumerGroupError::MemberNotFound(_) => BrokerErrorCode::NotFound,
        ConsumerGroupError::CapabilityConflict(_) => BrokerErrorCode::Conflict,
        ConsumerGroupError::MemberCapacityReached { .. } => BrokerErrorCode::CapacityExceeded,
        ConsumerGroupError::InvalidMemberCapacity => BrokerErrorCode::InvalidRequest,
        ConsumerGroupError::FencingValue(_) => BrokerErrorCode::InternalError,
    }
}

fn task_transition_error_code(error: &TaskTransitionError) -> BrokerErrorCode {
    match error {
        TaskTransitionError::NotQueued { .. } | TaskTransitionError::AlreadyCompleted => {
            BrokerErrorCode::Conflict
        }
        TaskTransitionError::NotLeased { .. }
        | TaskTransitionError::StaleGroup
        | TaskTransitionError::StaleOwner
        | TaskTransitionError::StaleGeneration
        | TaskTransitionError::StaleLeaseEpoch
        | TaskTransitionError::StaleLeaseId
        | TaskTransitionError::LeaseExpired => BrokerErrorCode::StaleFence,
        TaskTransitionError::FencingValue(_) => BrokerErrorCode::InternalError,
    }
}

#[cfg(test)]
mod tests {
    use agent_broker_domain::{
        ConsumerGroupError, ConsumerGroupId, Generation, LeaseId, MemberId, StateMachineError,
        TaskId, TaskTransitionError,
    };

    use super::{BrokerError, BrokerErrorCode, BrokerErrorDisposition};

    #[test]
    fn stable_codes_match_python_reference_strings() {
        assert_eq!(BrokerErrorCode::InvalidRequest.as_str(), "INVALID_REQUEST");
        assert_eq!(BrokerErrorCode::NotFound.as_str(), "NOT_FOUND");
        assert_eq!(BrokerErrorCode::Conflict.as_str(), "CONFLICT");
        assert_eq!(
            BrokerErrorCode::CapacityExceeded.as_str(),
            "CAPACITY_EXCEEDED"
        );
        assert_eq!(BrokerErrorCode::StaleFence.as_str(), "STALE_FENCE");
        assert_eq!(
            BrokerErrorCode::PersistenceError.as_str(),
            "PERSISTENCE_ERROR"
        );
        assert_eq!(BrokerErrorCode::TransportError.as_str(), "TRANSPORT_ERROR");
        assert_eq!(
            BrokerErrorCode::CommitOutcomeUnknown.as_str(),
            "COMMIT_OUTCOME_UNKNOWN"
        );
        assert_eq!(BrokerErrorCode::InternalError.as_str(), "INTERNAL_ERROR");
        assert_eq!(BrokerErrorDisposition::Committed.as_str(), "COMMITTED");
        assert_eq!(BrokerErrorDisposition::Rejected.as_str(), "REJECTED");
        assert_eq!(BrokerErrorDisposition::Unknown.as_str(), "UNKNOWN");
        assert_eq!(
            BrokerError::new(BrokerErrorCode::Conflict, "conflict").disposition(),
            BrokerErrorDisposition::Unknown
        );
    }

    #[test]
    fn state_machine_categories_map_to_stable_protocol_codes()
    -> Result<(), Box<dyn std::error::Error>> {
        let not_found = BrokerError::from(StateMachineError::TaskNotFound(TaskId::new("task-1")?));
        assert_eq!(not_found.code(), BrokerErrorCode::NotFound);

        let conflict =
            BrokerError::from(StateMachineError::LeaseIdConflict(LeaseId::new("lease-1")?));
        assert_eq!(conflict.code(), BrokerErrorCode::Conflict);

        let capacity = BrokerError::from(StateMachineError::CapacityExceeded {
            resource: "Broker Task",
            max: 1,
        });
        assert_eq!(capacity.code(), BrokerErrorCode::CapacityExceeded);

        let stale_group = BrokerError::from(StateMachineError::ConsumerGroupTransition(
            ConsumerGroupError::StaleGeneration {
                expected: Generation::new(1),
                actual: Generation::new(2),
            },
        ));
        assert_eq!(stale_group.code(), BrokerErrorCode::StaleFence);

        let missing_member = BrokerError::from(StateMachineError::ConsumerGroupTransition(
            ConsumerGroupError::MemberNotFound(MemberId::new("worker-a")?),
        ));
        assert_eq!(missing_member.code(), BrokerErrorCode::NotFound);

        let stale_lease = BrokerError::from(StateMachineError::TaskTransition(
            TaskTransitionError::LeaseExpired,
        ));
        assert_eq!(stale_lease.code(), BrokerErrorCode::StaleFence);

        let group_conflict = BrokerError::from(StateMachineError::ConsumerGroupConflict(
            ConsumerGroupId::new("engineering")?,
        ));
        assert_eq!(group_conflict.code(), BrokerErrorCode::Conflict);
        Ok(())
    }
}

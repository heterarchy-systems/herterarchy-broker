use std::error::Error;
use std::fmt;

use agent_broker_domain::{
    Capabilities, CapabilitiesError, Capability, ConsumerGroupId, ConsumerId, Generation,
    LeaseDurationMs, LeaseEpoch, LeaseId, NamespaceId, TaskId, TaskObjective, TaskResult, Term,
};

/// Stable protocol generation shared with the Python reference Broker.
pub const PROTOCOL_VERSION: u32 = 1;
const MAX_REQUEST_ID_BYTES: usize = 128;
const MAX_DECLARED_CAPABILITIES: usize = 64;

/// Correlation identifier carried by every Broker request envelope.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RequestId(String);

impl RequestId {
    /// Validate the protocol-v1 request identifier contract.
    ///
    /// # Errors
    ///
    /// Returns [`RequestIdError`] when the identifier is empty, too long, or contains characters
    /// outside the Python reference protocol contract.
    pub fn new(value: impl Into<String>) -> Result<Self, RequestIdError> {
        let value = value.into();
        let bytes = value.as_bytes();
        let Some(first) = bytes.first() else {
            return Err(RequestIdError);
        };
        if bytes.len() > MAX_REQUEST_ID_BYTES || !first.is_ascii_alphanumeric() {
            return Err(RequestIdError);
        }
        if bytes.iter().skip(1).any(|byte| !is_request_id_byte(*byte)) {
            return Err(RequestIdError);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for RequestId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

fn is_request_id_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-')
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RequestIdError;

impl fmt::Display for RequestIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("request_id contains unsupported characters or length")
    }
}

impl Error for RequestIdError {}

/// Ordered capability declarations preserved exactly at the wire boundary.
///
/// Python protocol-v1 preserves declaration order and duplicates while the application layer
/// normalizes the values into the domain [`Capabilities`] set. Keeping those responsibilities
/// separate is required for byte-level request compatibility during the migration.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeclaredCapabilities(Box<[String]>);

impl DeclaredCapabilities {
    /// Validate capability syntax while preserving the original order and duplicates.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilitiesError`] when more than 64 values are declared or any value violates
    /// the provider-neutral capability syntax contract.
    pub fn new<I, S>(values: I) -> Result<Self, CapabilitiesError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let values = values.into_iter().map(Into::into).collect::<Vec<_>>();
        if values.len() > MAX_DECLARED_CAPABILITIES {
            return Err(CapabilitiesError::TooMany {
                max: MAX_DECLARED_CAPABILITIES,
            });
        }
        for value in &values {
            Capability::new(value.clone())?;
        }
        Ok(Self(values.into_boxed_slice()))
    }

    /// Borrow declarations in their original wire order.
    #[must_use]
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }

    /// Normalize declarations at the application boundary.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilitiesError`] if the domain normalization contract rejects the values.
    pub fn into_normalized(self) -> Result<Capabilities, CapabilitiesError> {
        Capabilities::new(self.0.into_vec())
    }
}

/// Stable protocol-v1 operation names.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Operation {
    Health,
    EnsureNamespace,
    PublishTask,
    EnsureConsumerGroup,
    JoinConsumerGroup,
    Heartbeat,
    LeaveConsumerGroup,
    ClaimTask,
    RenewTaskLease,
    CompleteTask,
}

impl Operation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::EnsureNamespace => "namespace.ensure",
            Self::PublishTask => "task.publish",
            Self::EnsureConsumerGroup => "group.ensure",
            Self::JoinConsumerGroup => "group.join",
            Self::Heartbeat => "group.heartbeat",
            Self::LeaveConsumerGroup => "group.leave",
            Self::ClaimTask => "task.claim",
            Self::RenewTaskLease => "task.renew",
            Self::CompleteTask => "task.complete",
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for Operation {
    type Error = OperationParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "health" => Ok(Self::Health),
            "namespace.ensure" => Ok(Self::EnsureNamespace),
            "task.publish" => Ok(Self::PublishTask),
            "group.ensure" => Ok(Self::EnsureConsumerGroup),
            "group.join" => Ok(Self::JoinConsumerGroup),
            "group.heartbeat" => Ok(Self::Heartbeat),
            "group.leave" => Ok(Self::LeaveConsumerGroup),
            "task.claim" => Ok(Self::ClaimTask),
            "task.renew" => Ok(Self::RenewTaskLease),
            "task.complete" => Ok(Self::CompleteTask),
            _ => Err(OperationParseError(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OperationParseError(String);

impl fmt::Display for OperationParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unsupported Broker operation {:?}", self.0)
    }
}

impl Error for OperationParseError {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HealthRequest {
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureNamespaceRequest {
    pub request_id: RequestId,
    pub namespace_id: NamespaceId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublishTaskRequest {
    pub request_id: RequestId,
    pub namespace_id: NamespaceId,
    pub task_id: TaskId,
    pub objective: TaskObjective,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureConsumerGroupRequest {
    pub request_id: RequestId,
    pub namespace_id: NamespaceId,
    pub group_id: ConsumerGroupId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JoinConsumerGroupRequest {
    pub request_id: RequestId,
    pub group_id: ConsumerGroupId,
    pub member_id: ConsumerId,
    pub capabilities: DeclaredCapabilities,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HeartbeatRequest {
    pub request_id: RequestId,
    pub group_id: ConsumerGroupId,
    pub member_id: ConsumerId,
    pub expected_generation: Generation,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LeaveConsumerGroupRequest {
    pub request_id: RequestId,
    pub group_id: ConsumerGroupId,
    pub member_id: ConsumerId,
    pub expected_generation: Generation,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimTaskRequest {
    pub request_id: RequestId,
    pub group_id: ConsumerGroupId,
    pub member_id: ConsumerId,
    pub expected_term: Term,
    pub expected_generation: Generation,
    pub lease_id: LeaseId,
    pub lease_duration: LeaseDurationMs,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RenewTaskLeaseRequest {
    pub request_id: RequestId,
    pub task_id: TaskId,
    pub group_id: ConsumerGroupId,
    pub member_id: ConsumerId,
    pub expected_term: Term,
    pub expected_generation: Generation,
    pub expected_lease_epoch: LeaseEpoch,
    pub lease_id: LeaseId,
    pub lease_duration: LeaseDurationMs,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompleteTaskRequest {
    pub request_id: RequestId,
    pub task_id: TaskId,
    pub group_id: ConsumerGroupId,
    pub member_id: ConsumerId,
    pub expected_term: Term,
    pub expected_generation: Generation,
    pub expected_lease_epoch: LeaseEpoch,
    pub lease_id: LeaseId,
    pub result: TaskResult,
}

/// Fully validated protocol request ready to map into the application boundary.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BrokerRequest {
    Health(HealthRequest),
    EnsureNamespace(EnsureNamespaceRequest),
    PublishTask(PublishTaskRequest),
    EnsureConsumerGroup(EnsureConsumerGroupRequest),
    JoinConsumerGroup(JoinConsumerGroupRequest),
    Heartbeat(HeartbeatRequest),
    LeaveConsumerGroup(LeaveConsumerGroupRequest),
    ClaimTask(ClaimTaskRequest),
    RenewTaskLease(RenewTaskLeaseRequest),
    CompleteTask(CompleteTaskRequest),
}

impl BrokerRequest {
    #[must_use]
    pub const fn operation(&self) -> Operation {
        match self {
            Self::Health(_) => Operation::Health,
            Self::EnsureNamespace(_) => Operation::EnsureNamespace,
            Self::PublishTask(_) => Operation::PublishTask,
            Self::EnsureConsumerGroup(_) => Operation::EnsureConsumerGroup,
            Self::JoinConsumerGroup(_) => Operation::JoinConsumerGroup,
            Self::Heartbeat(_) => Operation::Heartbeat,
            Self::LeaveConsumerGroup(_) => Operation::LeaveConsumerGroup,
            Self::ClaimTask(_) => Operation::ClaimTask,
            Self::RenewTaskLease(_) => Operation::RenewTaskLease,
            Self::CompleteTask(_) => Operation::CompleteTask,
        }
    }

    #[must_use]
    pub fn request_id(&self) -> &RequestId {
        match self {
            Self::Health(request) => &request.request_id,
            Self::EnsureNamespace(request) => &request.request_id,
            Self::PublishTask(request) => &request.request_id,
            Self::EnsureConsumerGroup(request) => &request.request_id,
            Self::JoinConsumerGroup(request) => &request.request_id,
            Self::Heartbeat(request) => &request.request_id,
            Self::LeaveConsumerGroup(request) => &request.request_id,
            Self::ClaimTask(request) => &request.request_id,
            Self::RenewTaskLease(request) => &request.request_id,
            Self::CompleteTask(request) => &request.request_id,
        }
    }
}

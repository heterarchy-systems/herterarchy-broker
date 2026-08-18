use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use agent_broker_application::{BrokerError, BrokerErrorCode};
use agent_broker_domain::{
    ConsumerGroupId, ConsumerId, Generation, LeaseDurationMs, LeaseEpoch, LeaseId, NamespaceId,
    Revision, TaskId, TaskObjective, TaskResult, TaskStatus, Term, TimestampMs,
};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    BrokerRequest, BrokerResponse, ClaimTaskRequest, CompleteTaskRequest, DeclaredCapabilities,
    EnsureConsumerGroupRequest, EnsureNamespaceRequest, HealthRequest, HeartbeatRequest,
    JoinConsumerGroupRequest, LeaveConsumerGroupRequest, Operation, PROTOCOL_VERSION,
    PublishTaskRequest, RenewTaskLeaseRequest, RequestId, SuccessPayload,
};

/// Default protocol-v1 NDJSON frame limit shared with the Python standalone Broker.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 128 * 1024;
/// Maximum protocol error-message bytes emitted on the wire.
pub const MAX_ERROR_MESSAGE_BYTES: usize = 4 * 1024;

/// Strict protocol-v1 codec failure.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ProtocolCodecError {
    FrameTooLarge { actual: usize, max: usize },
    InvalidRequest(String),
    ErrorMessageTooLarge { actual: usize, max: usize },
    Serialization(String),
}

/// Failure while decoding a correlated Broker response for a known request operation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ResponseDecodeError {
    Protocol(ProtocolCodecError),
    Broker(BrokerError),
}

impl fmt::Display for ResponseDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => error.fmt(formatter),
            Self::Broker(error) => error.fmt(formatter),
        }
    }
}

impl Error for ResponseDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protocol(error) => Some(error),
            Self::Broker(error) => Some(error),
        }
    }
}

impl From<ProtocolCodecError> for ResponseDecodeError {
    fn from(error: ProtocolCodecError) -> Self {
        Self::Protocol(error)
    }
}

impl fmt::Display for ProtocolCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge { actual, max } => {
                write!(
                    formatter,
                    "protocol frame is {actual} bytes; maximum is {max}"
                )
            }
            Self::InvalidRequest(message) | Self::Serialization(message) => {
                formatter.write_str(message)
            }
            Self::ErrorMessageTooLarge { actual, max } => write!(
                formatter,
                "protocol error message is {actual} bytes; maximum is {max}"
            ),
        }
    }
}

impl Error for ProtocolCodecError {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestEnvelopeWire {
    version: u32,
    request_id: String,
    operation: String,
    payload: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyPayload {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NamespacePayload {
    namespace_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishTaskPayload {
    namespace_id: String,
    task_id: String,
    objective: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnsureGroupPayload {
    namespace_id: String,
    group_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JoinGroupPayload {
    group_id: String,
    member_id: String,
    capabilities: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MembershipPayload {
    group_id: String,
    member_id: String,
    expected_generation: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimTaskPayload {
    group_id: String,
    member_id: String,
    expected_term: u64,
    expected_generation: u64,
    lease_id: String,
    lease_duration_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RenewTaskPayload {
    task_id: String,
    group_id: String,
    member_id: String,
    expected_term: u64,
    expected_generation: u64,
    expected_lease_epoch: u64,
    lease_id: String,
    lease_duration_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompleteTaskPayload {
    task_id: String,
    group_id: String,
    member_id: String,
    expected_term: u64,
    expected_generation: u64,
    expected_lease_epoch: u64,
    lease_id: String,
    result: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseEnvelopeWire {
    version: u32,
    request_id: String,
    ok: bool,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<ResponseErrorWire>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseErrorWire {
    code: String,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthResultWire {
    protocol_version: u32,
    term: u64,
    revision: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NamespaceResultWire {
    term: u64,
    revision: u64,
    namespace_id: String,
    namespace_revision: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskPublishedResultWire {
    term: u64,
    revision: u64,
    task_id: String,
    task_revision: u64,
    status: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsumerGroupResultWire {
    term: u64,
    revision: u64,
    group_id: String,
    generation: u64,
    group_revision: u64,
    member_count: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeartbeatResultWire {
    term: u64,
    revision: u64,
    group_id: String,
    member_id: String,
    generation: u64,
    member_revision: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskClaimResultWire {
    term: u64,
    revision: u64,
    task_id: Option<String>,
    objective: Option<String>,
    task_revision: Option<u64>,
    lease_id: Option<String>,
    lease_epoch: Option<u64>,
    lease_expires_at_ms: Option<u64>,
    generation: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskLeaseRenewedResultWire {
    term: u64,
    revision: u64,
    task_id: String,
    task_revision: u64,
    lease_id: String,
    lease_epoch: u64,
    lease_expires_at_ms: u64,
    generation: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskCompletedResultWire {
    term: u64,
    revision: u64,
    task_id: String,
    task_revision: u64,
    status: String,
}

/// Decode one bounded protocol-v1 JSON/NDJSON request frame.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] for oversize frames, invalid JSON, unknown fields/operations,
/// version mismatch, wrong JSON types, or values that violate validated Broker input contracts.
pub fn decode_request(frame: &[u8]) -> Result<BrokerRequest, ProtocolCodecError> {
    decode_request_with_limit(frame, DEFAULT_MAX_FRAME_BYTES)
}

/// Decode one protocol-v1 request using an explicit frame limit.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] under the same conditions as [`decode_request`].
pub fn decode_request_with_limit(
    frame: &[u8],
    max_frame_bytes: usize,
) -> Result<BrokerRequest, ProtocolCodecError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: RequestEnvelopeWire = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Request frame must be valid protocol-v1 JSON: {error}"
        ))
    })?;
    if envelope.version != PROTOCOL_VERSION {
        return Err(invalid_request(format!(
            "Unsupported protocol version {}.",
            envelope.version
        )));
    }
    let request_id =
        RequestId::new(envelope.request_id).map_err(|error| invalid_field("request_id", &error))?;
    let operation = Operation::try_from(envelope.operation.as_str())
        .map_err(|error| invalid_request(error.to_string()))?;
    decode_operation(operation, request_id, envelope.payload)
}

/// Decode one correlated response for the operation that produced it.
///
/// # Errors
///
/// Returns [`ResponseDecodeError::Protocol`] for malformed/mismatched response frames and
/// [`ResponseDecodeError::Broker`] when the Broker returned a stable application error.
pub fn decode_response_for_operation(
    frame: &[u8],
    expected_request_id: &RequestId,
    operation: Operation,
) -> Result<SuccessPayload, ResponseDecodeError> {
    decode_response_for_operation_with_limit(
        frame,
        expected_request_id,
        operation,
        DEFAULT_MAX_FRAME_BYTES,
    )
}

/// Decode a correlated response using an explicit frame limit.
///
/// # Errors
///
/// Returns the same error categories as [`decode_response_for_operation`].
pub fn decode_response_for_operation_with_limit(
    frame: &[u8],
    expected_request_id: &RequestId,
    operation: Operation,
    max_frame_bytes: usize,
) -> Result<SuccessPayload, ResponseDecodeError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: ResponseEnvelopeWire = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Response frame must be valid protocol-v1 JSON: {error}"
        ))
    })?;
    if envelope.version != PROTOCOL_VERSION {
        return Err(invalid_request(format!(
            "Unsupported response protocol version {}.",
            envelope.version
        ))
        .into());
    }
    if envelope.request_id != expected_request_id.as_str() {
        return Err(invalid_request("Response request_id does not match the request.").into());
    }
    if !envelope.ok {
        if envelope.result.is_some() {
            return Err(invalid_request("Error response must not contain result.").into());
        }
        let error = envelope
            .error
            .ok_or_else(|| invalid_request("Error response must contain error."))?;
        ensure_error_message_bound(&error.message)?;
        let code = parse_error_code(&error.code)?;
        return Err(ResponseDecodeError::Broker(BrokerError::new(
            code,
            error.message,
        )));
    }
    if envelope.error.is_some() {
        return Err(invalid_request("Success response must not contain error.").into());
    }
    let result = envelope
        .result
        .ok_or_else(|| invalid_request("Success response must contain result."))?;
    decode_success_payload(operation, result).map_err(ResponseDecodeError::from)
}

fn decode_success_payload(
    operation: Operation,
    value: Value,
) -> Result<SuccessPayload, ProtocolCodecError> {
    match operation {
        Operation::Health => decode_health_result(value),
        Operation::EnsureNamespace => decode_namespace_result(value),
        Operation::PublishTask => decode_task_published_result(value),
        Operation::EnsureConsumerGroup
        | Operation::JoinConsumerGroup
        | Operation::LeaveConsumerGroup => decode_consumer_group_result(value),
        Operation::Heartbeat => decode_heartbeat_result(value),
        Operation::ClaimTask => decode_task_claim_result(value),
        Operation::RenewTaskLease => decode_task_lease_renewed_result(value),
        Operation::CompleteTask => decode_task_completed_result(value),
    }
}

fn decode_health_result(value: Value) -> Result<SuccessPayload, ProtocolCodecError> {
    require_object_keys(
        &value,
        &["protocol_version", "revision", "term"],
        "health result",
    )?;
    let wire: HealthResultWire = decode_payload(value)?;
    if wire.protocol_version != PROTOCOL_VERSION {
        return Err(invalid_request(
            "Health protocol_version does not match protocol-v1.",
        ));
    }
    Ok(SuccessPayload::Health {
        protocol_version: wire.protocol_version,
        term: term(wire.term, "term")?,
        revision: Revision::new(wire.revision),
    })
}

fn decode_namespace_result(value: Value) -> Result<SuccessPayload, ProtocolCodecError> {
    require_object_keys(
        &value,
        &["namespace_id", "namespace_revision", "revision", "term"],
        "namespace result",
    )?;
    let wire: NamespaceResultWire = decode_payload(value)?;
    Ok(SuccessPayload::Namespace {
        term: term(wire.term, "term")?,
        revision: Revision::new(wire.revision),
        namespace_id: NamespaceId::new(wire.namespace_id)
            .map_err(|error| invalid_field("namespace_id", &error))?,
        namespace_revision: positive_revision(wire.namespace_revision, "namespace_revision")?,
    })
}

fn decode_task_published_result(value: Value) -> Result<SuccessPayload, ProtocolCodecError> {
    require_object_keys(
        &value,
        &["revision", "status", "task_id", "task_revision", "term"],
        "task publish result",
    )?;
    let wire: TaskPublishedResultWire = decode_payload(value)?;
    Ok(SuccessPayload::TaskPublished {
        term: term(wire.term, "term")?,
        revision: Revision::new(wire.revision),
        task_id: TaskId::new(wire.task_id).map_err(|error| invalid_field("task_id", &error))?,
        task_revision: positive_revision(wire.task_revision, "task_revision")?,
        status: task_status(&wire.status)?,
    })
}

fn decode_consumer_group_result(value: Value) -> Result<SuccessPayload, ProtocolCodecError> {
    require_object_keys(
        &value,
        &[
            "generation",
            "group_id",
            "group_revision",
            "member_count",
            "revision",
            "term",
        ],
        "Consumer Group result",
    )?;
    let wire: ConsumerGroupResultWire = decode_payload(value)?;
    Ok(SuccessPayload::ConsumerGroup {
        term: term(wire.term, "term")?,
        revision: Revision::new(wire.revision),
        group_id: ConsumerGroupId::new(wire.group_id)
            .map_err(|error| invalid_field("group_id", &error))?,
        generation: Generation::new(wire.generation),
        group_revision: positive_revision(wire.group_revision, "group_revision")?,
        member_count: wire.member_count,
    })
}

fn decode_heartbeat_result(value: Value) -> Result<SuccessPayload, ProtocolCodecError> {
    require_object_keys(
        &value,
        &[
            "generation",
            "group_id",
            "member_id",
            "member_revision",
            "revision",
            "term",
        ],
        "heartbeat result",
    )?;
    let wire: HeartbeatResultWire = decode_payload(value)?;
    Ok(SuccessPayload::Heartbeat {
        term: term(wire.term, "term")?,
        revision: Revision::new(wire.revision),
        group_id: ConsumerGroupId::new(wire.group_id)
            .map_err(|error| invalid_field("group_id", &error))?,
        member_id: ConsumerId::new(wire.member_id)
            .map_err(|error| invalid_field("member_id", &error))?,
        generation: Generation::new(wire.generation),
        member_revision: positive_revision(wire.member_revision, "member_revision")?,
    })
}

fn decode_task_claim_result(value: Value) -> Result<SuccessPayload, ProtocolCodecError> {
    require_object_keys(
        &value,
        &[
            "generation",
            "lease_epoch",
            "lease_expires_at_ms",
            "lease_id",
            "objective",
            "revision",
            "task_id",
            "task_revision",
            "term",
        ],
        "task claim result",
    )?;
    let wire: TaskClaimResultWire = decode_payload(value)?;
    let task_id = wire
        .task_id
        .map(TaskId::new)
        .transpose()
        .map_err(|error| invalid_field("task_id", &error))?;
    let objective = wire
        .objective
        .map(TaskObjective::new)
        .transpose()
        .map_err(|error| invalid_field("objective", &error))?;
    let lease_id = wire
        .lease_id
        .map(LeaseId::new)
        .transpose()
        .map_err(|error| invalid_field("lease_id", &error))?;
    let populated = task_id.is_some();
    if [
        objective.is_some(),
        wire.task_revision.is_some(),
        lease_id.is_some(),
        wire.lease_epoch.is_some(),
        wire.lease_expires_at_ms.is_some(),
    ]
    .iter()
    .any(|present| *present != populated)
    {
        return Err(invalid_request(
            "Task claim result must contain either a complete lease payload or all null fields.",
        ));
    }
    Ok(SuccessPayload::TaskClaimed {
        term: term(wire.term, "term")?,
        revision: Revision::new(wire.revision),
        task_id,
        objective,
        task_revision: wire.task_revision.map(Revision::new),
        lease_id,
        lease_epoch: wire.lease_epoch.map(LeaseEpoch::new),
        lease_expires_at_ms: wire.lease_expires_at_ms.map(TimestampMs::new),
        generation: Generation::new(wire.generation),
    })
}

fn decode_task_lease_renewed_result(value: Value) -> Result<SuccessPayload, ProtocolCodecError> {
    require_object_keys(
        &value,
        &[
            "generation",
            "lease_epoch",
            "lease_expires_at_ms",
            "lease_id",
            "revision",
            "task_id",
            "task_revision",
            "term",
        ],
        "task renew result",
    )?;
    let wire: TaskLeaseRenewedResultWire = decode_payload(value)?;
    Ok(SuccessPayload::TaskLeaseRenewed {
        term: term(wire.term, "term")?,
        revision: Revision::new(wire.revision),
        task_id: TaskId::new(wire.task_id).map_err(|error| invalid_field("task_id", &error))?,
        task_revision: positive_revision(wire.task_revision, "task_revision")?,
        lease_id: LeaseId::new(wire.lease_id).map_err(|error| invalid_field("lease_id", &error))?,
        lease_epoch: LeaseEpoch::new(wire.lease_epoch),
        lease_expires_at_ms: TimestampMs::new(wire.lease_expires_at_ms),
        generation: Generation::new(wire.generation),
    })
}

fn decode_task_completed_result(value: Value) -> Result<SuccessPayload, ProtocolCodecError> {
    require_object_keys(
        &value,
        &["revision", "status", "task_id", "task_revision", "term"],
        "task complete result",
    )?;
    let wire: TaskCompletedResultWire = decode_payload(value)?;
    Ok(SuccessPayload::TaskCompleted {
        term: term(wire.term, "term")?,
        revision: Revision::new(wire.revision),
        task_id: TaskId::new(wire.task_id).map_err(|error| invalid_field("task_id", &error))?,
        task_revision: positive_revision(wire.task_revision, "task_revision")?,
        status: task_status(&wire.status)?,
    })
}

fn require_object_keys(
    value: &Value,
    expected: &[&str],
    label: &str,
) -> Result<(), ProtocolCodecError> {
    let Value::Object(object) = value else {
        return Err(invalid_request(format!("{label} must be a JSON object.")));
    };
    if object.len() != expected.len() || expected.iter().any(|key| !object.contains_key(*key)) {
        return Err(invalid_request(format!(
            "{label} fields do not match the protocol-v1 schema."
        )));
    }
    Ok(())
}

fn term(value: u64, field: &str) -> Result<Term, ProtocolCodecError> {
    Term::new(value).map_err(|error| invalid_field(field, &error))
}

fn positive_revision(value: u64, field: &str) -> Result<Revision, ProtocolCodecError> {
    if value == 0 {
        return Err(invalid_request(format!("{field} must be positive.")));
    }
    Ok(Revision::new(value))
}

fn task_status(value: &str) -> Result<TaskStatus, ProtocolCodecError> {
    match value {
        "queued" => Ok(TaskStatus::Queued),
        "leased" => Ok(TaskStatus::Leased),
        "completed" => Ok(TaskStatus::Completed),
        _ => Err(invalid_request(format!("Unknown task status {value:?}."))),
    }
}

fn parse_error_code(value: &str) -> Result<BrokerErrorCode, ProtocolCodecError> {
    match value {
        "INVALID_REQUEST" => Ok(BrokerErrorCode::InvalidRequest),
        "NOT_FOUND" => Ok(BrokerErrorCode::NotFound),
        "CONFLICT" => Ok(BrokerErrorCode::Conflict),
        "CAPACITY_EXCEEDED" => Ok(BrokerErrorCode::CapacityExceeded),
        "STALE_FENCE" => Ok(BrokerErrorCode::StaleFence),
        "PERSISTENCE_ERROR" => Ok(BrokerErrorCode::PersistenceError),
        "TRANSPORT_ERROR" => Ok(BrokerErrorCode::TransportError),
        "COMMIT_OUTCOME_UNKNOWN" => Ok(BrokerErrorCode::CommitOutcomeUnknown),
        "INTERNAL_ERROR" => Ok(BrokerErrorCode::InternalError),
        _ => Err(invalid_request(format!(
            "Unknown Broker error code {value:?}."
        ))),
    }
}

fn decode_operation(
    operation: Operation,
    request_id: RequestId,
    payload: Value,
) -> Result<BrokerRequest, ProtocolCodecError> {
    match operation {
        Operation::Health => decode_health(request_id, payload),
        Operation::EnsureNamespace => decode_namespace(request_id, payload),
        Operation::PublishTask => decode_publish(request_id, payload),
        Operation::EnsureConsumerGroup => decode_ensure_group(request_id, payload),
        Operation::JoinConsumerGroup => decode_join_group(request_id, payload),
        Operation::Heartbeat => decode_heartbeat(request_id, payload),
        Operation::LeaveConsumerGroup => decode_leave_group(request_id, payload),
        Operation::ClaimTask => decode_claim(request_id, payload),
        Operation::RenewTaskLease => decode_renew(request_id, payload),
        Operation::CompleteTask => decode_complete(request_id, payload),
    }
}

fn decode_health(
    request_id: RequestId,
    payload: Value,
) -> Result<BrokerRequest, ProtocolCodecError> {
    let _payload: EmptyPayload = decode_payload(payload)?;
    Ok(BrokerRequest::Health(HealthRequest { request_id }))
}

fn decode_namespace(
    request_id: RequestId,
    payload: Value,
) -> Result<BrokerRequest, ProtocolCodecError> {
    let payload: NamespacePayload = decode_payload(payload)?;
    Ok(BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id,
        namespace_id: NamespaceId::new(payload.namespace_id)
            .map_err(|error| invalid_field("namespace_id", &error))?,
    }))
}

fn decode_publish(
    request_id: RequestId,
    payload: Value,
) -> Result<BrokerRequest, ProtocolCodecError> {
    let payload: PublishTaskPayload = decode_payload(payload)?;
    Ok(BrokerRequest::PublishTask(PublishTaskRequest {
        request_id,
        namespace_id: NamespaceId::new(payload.namespace_id)
            .map_err(|error| invalid_field("namespace_id", &error))?,
        task_id: TaskId::new(payload.task_id).map_err(|error| invalid_field("task_id", &error))?,
        objective: TaskObjective::new(payload.objective)
            .map_err(|error| invalid_field("objective", &error))?,
    }))
}

fn decode_ensure_group(
    request_id: RequestId,
    payload: Value,
) -> Result<BrokerRequest, ProtocolCodecError> {
    let payload: EnsureGroupPayload = decode_payload(payload)?;
    Ok(BrokerRequest::EnsureConsumerGroup(
        EnsureConsumerGroupRequest {
            request_id,
            namespace_id: NamespaceId::new(payload.namespace_id)
                .map_err(|error| invalid_field("namespace_id", &error))?,
            group_id: ConsumerGroupId::new(payload.group_id)
                .map_err(|error| invalid_field("group_id", &error))?,
        },
    ))
}

fn decode_join_group(
    request_id: RequestId,
    payload: Value,
) -> Result<BrokerRequest, ProtocolCodecError> {
    let payload: JoinGroupPayload = decode_payload(payload)?;
    Ok(BrokerRequest::JoinConsumerGroup(JoinConsumerGroupRequest {
        request_id,
        group_id: ConsumerGroupId::new(payload.group_id)
            .map_err(|error| invalid_field("group_id", &error))?,
        member_id: ConsumerId::new(payload.member_id)
            .map_err(|error| invalid_field("member_id", &error))?,
        capabilities: DeclaredCapabilities::new(payload.capabilities)
            .map_err(|error| invalid_field("capabilities", &error))?,
    }))
}

fn decode_heartbeat(
    request_id: RequestId,
    payload: Value,
) -> Result<BrokerRequest, ProtocolCodecError> {
    let payload: MembershipPayload = decode_payload(payload)?;
    Ok(BrokerRequest::Heartbeat(HeartbeatRequest {
        request_id,
        group_id: ConsumerGroupId::new(payload.group_id)
            .map_err(|error| invalid_field("group_id", &error))?,
        member_id: ConsumerId::new(payload.member_id)
            .map_err(|error| invalid_field("member_id", &error))?,
        expected_generation: Generation::new(payload.expected_generation),
    }))
}

fn decode_leave_group(
    request_id: RequestId,
    payload: Value,
) -> Result<BrokerRequest, ProtocolCodecError> {
    let payload: MembershipPayload = decode_payload(payload)?;
    Ok(BrokerRequest::LeaveConsumerGroup(
        LeaveConsumerGroupRequest {
            request_id,
            group_id: ConsumerGroupId::new(payload.group_id)
                .map_err(|error| invalid_field("group_id", &error))?,
            member_id: ConsumerId::new(payload.member_id)
                .map_err(|error| invalid_field("member_id", &error))?,
            expected_generation: Generation::new(payload.expected_generation),
        },
    ))
}

fn decode_claim(
    request_id: RequestId,
    payload: Value,
) -> Result<BrokerRequest, ProtocolCodecError> {
    let payload: ClaimTaskPayload = decode_payload(payload)?;
    Ok(BrokerRequest::ClaimTask(ClaimTaskRequest {
        request_id,
        group_id: ConsumerGroupId::new(payload.group_id)
            .map_err(|error| invalid_field("group_id", &error))?,
        member_id: ConsumerId::new(payload.member_id)
            .map_err(|error| invalid_field("member_id", &error))?,
        expected_term: Term::new(payload.expected_term)
            .map_err(|error| invalid_field("expected_term", &error))?,
        expected_generation: Generation::new(payload.expected_generation),
        lease_id: LeaseId::new(payload.lease_id)
            .map_err(|error| invalid_field("lease_id", &error))?,
        lease_duration: LeaseDurationMs::new(payload.lease_duration_ms)
            .map_err(|error| invalid_field("lease_duration_ms", &error))?,
    }))
}

fn decode_renew(
    request_id: RequestId,
    payload: Value,
) -> Result<BrokerRequest, ProtocolCodecError> {
    let payload: RenewTaskPayload = decode_payload(payload)?;
    Ok(BrokerRequest::RenewTaskLease(RenewTaskLeaseRequest {
        request_id,
        task_id: TaskId::new(payload.task_id).map_err(|error| invalid_field("task_id", &error))?,
        group_id: ConsumerGroupId::new(payload.group_id)
            .map_err(|error| invalid_field("group_id", &error))?,
        member_id: ConsumerId::new(payload.member_id)
            .map_err(|error| invalid_field("member_id", &error))?,
        expected_term: Term::new(payload.expected_term)
            .map_err(|error| invalid_field("expected_term", &error))?,
        expected_generation: Generation::new(payload.expected_generation),
        expected_lease_epoch: LeaseEpoch::new(payload.expected_lease_epoch),
        lease_id: LeaseId::new(payload.lease_id)
            .map_err(|error| invalid_field("lease_id", &error))?,
        lease_duration: LeaseDurationMs::new(payload.lease_duration_ms)
            .map_err(|error| invalid_field("lease_duration_ms", &error))?,
    }))
}

fn decode_complete(
    request_id: RequestId,
    payload: Value,
) -> Result<BrokerRequest, ProtocolCodecError> {
    let payload: CompleteTaskPayload = decode_payload(payload)?;
    Ok(BrokerRequest::CompleteTask(CompleteTaskRequest {
        request_id,
        task_id: TaskId::new(payload.task_id).map_err(|error| invalid_field("task_id", &error))?,
        group_id: ConsumerGroupId::new(payload.group_id)
            .map_err(|error| invalid_field("group_id", &error))?,
        member_id: ConsumerId::new(payload.member_id)
            .map_err(|error| invalid_field("member_id", &error))?,
        expected_term: Term::new(payload.expected_term)
            .map_err(|error| invalid_field("expected_term", &error))?,
        expected_generation: Generation::new(payload.expected_generation),
        expected_lease_epoch: LeaseEpoch::new(payload.expected_lease_epoch),
        lease_id: LeaseId::new(payload.lease_id)
            .map_err(|error| invalid_field("lease_id", &error))?,
        result: TaskResult::new(payload.result).map_err(|error| invalid_field("result", &error))?,
    }))
}

fn decode_payload<T>(payload: Value) -> Result<T, ProtocolCodecError>
where
    T: DeserializeOwned,
{
    serde_json::from_value(payload)
        .map_err(|error| invalid_request(format!("Request payload does not match schema: {error}")))
}

/// Encode one validated request with Python protocol-v1 key ordering and NDJSON framing.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] if serialization fails or the encoded frame exceeds the default
/// bound.
pub fn encode_request(request: &BrokerRequest) -> Result<Vec<u8>, ProtocolCodecError> {
    encode_request_with_limit(request, DEFAULT_MAX_FRAME_BYTES)
}

/// Encode one request using an explicit frame limit.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] if serialization fails or the encoded frame exceeds the bound.
pub fn encode_request_with_limit(
    request: &BrokerRequest,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    let root = object([
        (
            "operation",
            Value::String(request.operation().as_str().to_owned()),
        ),
        ("payload", request_payload(request)),
        (
            "request_id",
            Value::String(request.request_id().as_str().to_owned()),
        ),
        ("version", Value::from(PROTOCOL_VERSION)),
    ]);
    encode_bounded(&root, max_frame_bytes)
}

fn request_payload(request: &BrokerRequest) -> Value {
    match request {
        BrokerRequest::Health(_) => object([]),
        BrokerRequest::EnsureNamespace(request) => object([(
            "namespace_id",
            Value::String(request.namespace_id.as_str().to_owned()),
        )]),
        BrokerRequest::PublishTask(request) => object([
            (
                "namespace_id",
                Value::String(request.namespace_id.as_str().to_owned()),
            ),
            (
                "objective",
                Value::String(request.objective.as_str().to_owned()),
            ),
            (
                "task_id",
                Value::String(request.task_id.as_str().to_owned()),
            ),
        ]),
        BrokerRequest::EnsureConsumerGroup(request) => object([
            (
                "group_id",
                Value::String(request.group_id.as_str().to_owned()),
            ),
            (
                "namespace_id",
                Value::String(request.namespace_id.as_str().to_owned()),
            ),
        ]),
        BrokerRequest::JoinConsumerGroup(request) => object([
            (
                "capabilities",
                Value::Array(
                    request
                        .capabilities
                        .as_slice()
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            ),
            (
                "group_id",
                Value::String(request.group_id.as_str().to_owned()),
            ),
            (
                "member_id",
                Value::String(request.member_id.as_str().to_owned()),
            ),
        ]),
        BrokerRequest::Heartbeat(request) => membership_payload(
            &request.group_id,
            &request.member_id,
            request.expected_generation.get(),
        ),
        BrokerRequest::LeaveConsumerGroup(request) => membership_payload(
            &request.group_id,
            &request.member_id,
            request.expected_generation.get(),
        ),
        BrokerRequest::ClaimTask(request) => claim_payload(request),
        BrokerRequest::RenewTaskLease(request) => renew_payload(request),
        BrokerRequest::CompleteTask(request) => complete_payload(request),
    }
}

fn membership_payload(
    group_id: &ConsumerGroupId,
    member_id: &ConsumerId,
    expected_generation: u64,
) -> Value {
    object([
        ("expected_generation", Value::from(expected_generation)),
        ("group_id", Value::String(group_id.as_str().to_owned())),
        ("member_id", Value::String(member_id.as_str().to_owned())),
    ])
}

fn claim_payload(request: &ClaimTaskRequest) -> Value {
    object([
        (
            "expected_generation",
            Value::from(request.expected_generation.get()),
        ),
        ("expected_term", Value::from(request.expected_term.get())),
        (
            "group_id",
            Value::String(request.group_id.as_str().to_owned()),
        ),
        (
            "lease_duration_ms",
            Value::from(request.lease_duration.get()),
        ),
        (
            "lease_id",
            Value::String(request.lease_id.as_str().to_owned()),
        ),
        (
            "member_id",
            Value::String(request.member_id.as_str().to_owned()),
        ),
    ])
}

fn renew_payload(request: &RenewTaskLeaseRequest) -> Value {
    object([
        (
            "expected_generation",
            Value::from(request.expected_generation.get()),
        ),
        (
            "expected_lease_epoch",
            Value::from(request.expected_lease_epoch.get()),
        ),
        ("expected_term", Value::from(request.expected_term.get())),
        (
            "group_id",
            Value::String(request.group_id.as_str().to_owned()),
        ),
        (
            "lease_duration_ms",
            Value::from(request.lease_duration.get()),
        ),
        (
            "lease_id",
            Value::String(request.lease_id.as_str().to_owned()),
        ),
        (
            "member_id",
            Value::String(request.member_id.as_str().to_owned()),
        ),
        (
            "task_id",
            Value::String(request.task_id.as_str().to_owned()),
        ),
    ])
}

fn complete_payload(request: &CompleteTaskRequest) -> Value {
    object([
        (
            "expected_generation",
            Value::from(request.expected_generation.get()),
        ),
        (
            "expected_lease_epoch",
            Value::from(request.expected_lease_epoch.get()),
        ),
        ("expected_term", Value::from(request.expected_term.get())),
        (
            "group_id",
            Value::String(request.group_id.as_str().to_owned()),
        ),
        (
            "lease_id",
            Value::String(request.lease_id.as_str().to_owned()),
        ),
        (
            "member_id",
            Value::String(request.member_id.as_str().to_owned()),
        ),
        ("result", Value::String(request.result.as_str().to_owned())),
        (
            "task_id",
            Value::String(request.task_id.as_str().to_owned()),
        ),
    ])
}

/// Encode one typed Broker response with Python protocol-v1 key ordering and NDJSON framing.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] if an error message is unbounded, serialization fails, or the
/// encoded response exceeds the default frame bound.
pub fn encode_response(response: &BrokerResponse) -> Result<Vec<u8>, ProtocolCodecError> {
    encode_response_with_limit(response, DEFAULT_MAX_FRAME_BYTES)
}

/// Encode one typed Broker response using an explicit frame limit.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] under the same conditions as [`encode_response`].
pub fn encode_response_with_limit(
    response: &BrokerResponse,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    let root = match response {
        BrokerResponse::Success { request_id, result } => object([
            ("ok", Value::Bool(true)),
            ("request_id", Value::String(request_id.as_str().to_owned())),
            ("result", success_payload(result)?),
            ("version", Value::from(PROTOCOL_VERSION)),
        ]),
        BrokerResponse::Error { request_id, error } => {
            ensure_error_message_bound(&error.message)?;
            object([
                (
                    "error",
                    object([
                        ("code", Value::String(error.code.as_str().to_owned())),
                        ("message", Value::String(error.message.clone())),
                    ]),
                ),
                ("ok", Value::Bool(false)),
                ("request_id", Value::String(request_id.as_str().to_owned())),
                ("version", Value::from(PROTOCOL_VERSION)),
            ])
        }
    };
    encode_bounded(&root, max_frame_bytes)
}

fn success_payload(payload: &SuccessPayload) -> Result<Value, ProtocolCodecError> {
    match payload {
        SuccessPayload::Health {
            protocol_version,
            term,
            revision,
        } => Ok(health_success_payload(*protocol_version, *term, *revision)),
        SuccessPayload::Namespace {
            term,
            revision,
            namespace_id,
            namespace_revision,
        } => Ok(namespace_success_payload(
            *term,
            *revision,
            namespace_id,
            *namespace_revision,
        )),
        SuccessPayload::TaskPublished {
            term,
            revision,
            task_id,
            task_revision,
            status,
        }
        | SuccessPayload::TaskCompleted {
            term,
            revision,
            task_id,
            task_revision,
            status,
        } => Ok(task_status_payload(
            *term,
            *revision,
            task_id,
            *task_revision,
            *status,
        )),
        SuccessPayload::ConsumerGroup {
            term,
            revision,
            group_id,
            generation,
            group_revision,
            member_count,
        } => consumer_group_success_payload(
            *term,
            *revision,
            group_id,
            *generation,
            *group_revision,
            *member_count,
        ),
        SuccessPayload::Heartbeat {
            term,
            revision,
            group_id,
            member_id,
            generation,
            member_revision,
        } => Ok(heartbeat_success_payload(
            *term,
            *revision,
            group_id,
            member_id,
            *generation,
            *member_revision,
        )),
        claimed @ SuccessPayload::TaskClaimed { .. } => claimed_success_from_payload(claimed),
        renewed @ SuccessPayload::TaskLeaseRenewed { .. } => renewed_success_from_payload(renewed),
        SuccessPayload::SessionOwnerAcquired { .. } => Err(ProtocolCodecError::InvalidRequest(
            "command-session owner acquisition is not a protocol-v1 response".to_owned(),
        )),
    }
}

fn health_success_payload(protocol_version: u32, term: Term, revision: Revision) -> Value {
    object([
        ("protocol_version", Value::from(protocol_version)),
        ("revision", Value::from(revision.get())),
        ("term", Value::from(term.get())),
    ])
}

fn namespace_success_payload(
    term: Term,
    revision: Revision,
    namespace_id: &NamespaceId,
    namespace_revision: Revision,
) -> Value {
    object([
        (
            "namespace_id",
            Value::String(namespace_id.as_str().to_owned()),
        ),
        ("namespace_revision", Value::from(namespace_revision.get())),
        ("revision", Value::from(revision.get())),
        ("term", Value::from(term.get())),
    ])
}

fn consumer_group_success_payload(
    term: Term,
    revision: Revision,
    group_id: &ConsumerGroupId,
    generation: Generation,
    group_revision: Revision,
    member_count: usize,
) -> Result<Value, ProtocolCodecError> {
    Ok(object([
        ("generation", Value::from(generation.get())),
        ("group_id", Value::String(group_id.as_str().to_owned())),
        ("group_revision", Value::from(group_revision.get())),
        ("member_count", Value::from(member_count_u64(member_count)?)),
        ("revision", Value::from(revision.get())),
        ("term", Value::from(term.get())),
    ]))
}

fn heartbeat_success_payload(
    term: Term,
    revision: Revision,
    group_id: &ConsumerGroupId,
    member_id: &ConsumerId,
    generation: Generation,
    member_revision: Revision,
) -> Value {
    object([
        ("generation", Value::from(generation.get())),
        ("group_id", Value::String(group_id.as_str().to_owned())),
        ("member_id", Value::String(member_id.as_str().to_owned())),
        ("member_revision", Value::from(member_revision.get())),
        ("revision", Value::from(revision.get())),
        ("term", Value::from(term.get())),
    ])
}

fn task_status_payload(
    term: Term,
    revision: Revision,
    task_id: &TaskId,
    task_revision: Revision,
    status: TaskStatus,
) -> Value {
    object([
        ("revision", Value::from(revision.get())),
        ("status", Value::String(task_status_str(status).to_owned())),
        ("task_id", Value::String(task_id.as_str().to_owned())),
        ("task_revision", Value::from(task_revision.get())),
        ("term", Value::from(term.get())),
    ])
}

struct ClaimSuccessView<'a> {
    term: Term,
    revision: Revision,
    task_id: Option<&'a TaskId>,
    objective: Option<&'a TaskObjective>,
    task_revision: Option<Revision>,
    lease_id: Option<&'a LeaseId>,
    lease_epoch: Option<LeaseEpoch>,
    lease_expires_at_ms: Option<TimestampMs>,
    generation: Generation,
}

fn claimed_success_from_payload(payload: &SuccessPayload) -> Result<Value, ProtocolCodecError> {
    let SuccessPayload::TaskClaimed {
        term,
        revision,
        task_id,
        objective,
        task_revision,
        lease_id,
        lease_epoch,
        lease_expires_at_ms,
        generation,
    } = payload
    else {
        return Err(ProtocolCodecError::Serialization(
            "internal TaskClaimed response mapping mismatch".to_owned(),
        ));
    };
    Ok(claim_success_payload(&ClaimSuccessView {
        term: *term,
        revision: *revision,
        task_id: task_id.as_ref(),
        objective: objective.as_ref(),
        task_revision: *task_revision,
        lease_id: lease_id.as_ref(),
        lease_epoch: *lease_epoch,
        lease_expires_at_ms: *lease_expires_at_ms,
        generation: *generation,
    }))
}

fn claim_success_payload(view: &ClaimSuccessView<'_>) -> Value {
    object([
        ("generation", Value::from(view.generation.get())),
        (
            "lease_epoch",
            option_u64(view.lease_epoch.map(LeaseEpoch::get)),
        ),
        (
            "lease_expires_at_ms",
            option_u64(view.lease_expires_at_ms.map(TimestampMs::get)),
        ),
        (
            "lease_id",
            option_string(view.lease_id.map(LeaseId::as_str)),
        ),
        (
            "objective",
            option_string(view.objective.map(TaskObjective::as_str)),
        ),
        ("revision", Value::from(view.revision.get())),
        ("task_id", option_string(view.task_id.map(TaskId::as_str))),
        (
            "task_revision",
            option_u64(view.task_revision.map(Revision::get)),
        ),
        ("term", Value::from(view.term.get())),
    ])
}

struct RenewedSuccessView<'a> {
    term: Term,
    revision: Revision,
    task_id: &'a TaskId,
    task_revision: Revision,
    lease_id: &'a LeaseId,
    lease_epoch: LeaseEpoch,
    lease_expires_at_ms: TimestampMs,
    generation: Generation,
}

fn renewed_success_from_payload(payload: &SuccessPayload) -> Result<Value, ProtocolCodecError> {
    let SuccessPayload::TaskLeaseRenewed {
        term,
        revision,
        task_id,
        task_revision,
        lease_id,
        lease_epoch,
        lease_expires_at_ms,
        generation,
    } = payload
    else {
        return Err(ProtocolCodecError::Serialization(
            "internal TaskLeaseRenewed response mapping mismatch".to_owned(),
        ));
    };
    Ok(renewed_success_payload(&RenewedSuccessView {
        term: *term,
        revision: *revision,
        task_id,
        task_revision: *task_revision,
        lease_id,
        lease_epoch: *lease_epoch,
        lease_expires_at_ms: *lease_expires_at_ms,
        generation: *generation,
    }))
}

fn renewed_success_payload(view: &RenewedSuccessView<'_>) -> Value {
    object([
        ("generation", Value::from(view.generation.get())),
        ("lease_epoch", Value::from(view.lease_epoch.get())),
        (
            "lease_expires_at_ms",
            Value::from(view.lease_expires_at_ms.get()),
        ),
        ("lease_id", Value::String(view.lease_id.as_str().to_owned())),
        ("revision", Value::from(view.revision.get())),
        ("task_id", Value::String(view.task_id.as_str().to_owned())),
        ("task_revision", Value::from(view.task_revision.get())),
        ("term", Value::from(view.term.get())),
    ])
}

fn task_status_str(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Leased => "leased",
        TaskStatus::Completed => "completed",
    }
}

fn member_count_u64(member_count: usize) -> Result<u64, ProtocolCodecError> {
    u64::try_from(member_count).map_err(|error| {
        ProtocolCodecError::Serialization(format!("member_count cannot be serialized: {error}"))
    })
}

fn option_u64(value: Option<u64>) -> Value {
    value.map_or(Value::Null, Value::from)
}

fn option_string(value: Option<&str>) -> Value {
    value.map_or(Value::Null, |value| Value::String(value.to_owned()))
}

fn object<const N: usize>(entries: [(&str, Value); N]) -> Value {
    let sorted = entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect::<BTreeMap<_, _>>();
    let map = sorted.into_iter().collect::<serde_json::Map<_, _>>();
    Value::Object(map)
}

fn encode_bounded(value: &Value, max_frame_bytes: usize) -> Result<Vec<u8>, ProtocolCodecError> {
    let mut encoded = serde_json::to_vec(value)
        .map_err(|error| ProtocolCodecError::Serialization(error.to_string()))?;
    encoded.push(b'\n');
    ensure_frame_limit(encoded.len(), max_frame_bytes)?;
    Ok(encoded)
}

fn ensure_frame_limit(actual: usize, max: usize) -> Result<(), ProtocolCodecError> {
    if actual <= max {
        return Ok(());
    }
    Err(ProtocolCodecError::FrameTooLarge { actual, max })
}

fn ensure_error_message_bound(message: &str) -> Result<(), ProtocolCodecError> {
    if message.len() <= MAX_ERROR_MESSAGE_BYTES {
        return Ok(());
    }
    Err(ProtocolCodecError::ErrorMessageTooLarge {
        actual: message.len(),
        max: MAX_ERROR_MESSAGE_BYTES,
    })
}

fn invalid_request(message: impl Into<String>) -> ProtocolCodecError {
    ProtocolCodecError::InvalidRequest(message.into())
}

fn invalid_field(field: &str, error: &dyn fmt::Display) -> ProtocolCodecError {
    invalid_request(format!("{field} is invalid: {error}"))
}

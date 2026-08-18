use std::collections::BTreeMap;

use agent_broker_application::{
    BrokerErrorDisposition, CommandIdentity, CommandSequence, CommandSessionId, SessionOwnerEpoch,
    SessionOwnerInstanceId,
};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::{
    BrokerRequest, BrokerResponse, Operation, PROTOCOL_VERSION, ProtocolCodecError, RequestId,
    ResponseDecodeError, SuccessPayload, decode_request_with_limit,
    decode_response_for_operation_with_limit, encode_request_with_limit,
    encode_response_with_limit,
};

pub const PROTOCOL_VERSION_V3: u32 = 3;
pub const ACQUIRE_COMMAND_SESSION_OWNER_OPERATION: &str = "acquire_command_session_owner";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerAcquisitionRequestV3 {
    request_id: RequestId,
    session_id: CommandSessionId,
    expected_owner_epoch: SessionOwnerEpoch,
    owner_instance_id: SessionOwnerInstanceId,
}

impl OwnerAcquisitionRequestV3 {
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        session_id: CommandSessionId,
        expected_owner_epoch: SessionOwnerEpoch,
        owner_instance_id: SessionOwnerInstanceId,
    ) -> Self {
        Self {
            request_id,
            session_id,
            expected_owner_epoch,
            owner_instance_id,
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerIdentifiedBrokerRequestV3 {
    identity: CommandIdentity,
    request: BrokerRequest,
}

impl OwnerIdentifiedBrokerRequestV3 {
    /// Construct an owner-aware mutation-only request.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolCodecError`] when owner instance metadata is absent or `request` is health.
    pub fn new(
        identity: CommandIdentity,
        request: BrokerRequest,
    ) -> Result<Self, ProtocolCodecError> {
        if identity.owner_instance_id().is_none() {
            return Err(invalid_request(
                "protocol-v3 owner-aware mutation requires command_session_owner_instance_id",
            ));
        }
        if request.operation() == Operation::Health {
            return Err(invalid_request(
                "protocol-v3 owner-aware requests are mutation-only; health remains protocol-v1/read-only",
            ));
        }
        Ok(Self { identity, request })
    }

    #[must_use]
    pub const fn identity(&self) -> &CommandIdentity {
        &self.identity
    }

    #[must_use]
    pub const fn request(&self) -> &BrokerRequest {
        &self.request
    }

    #[must_use]
    pub fn request_id(&self) -> &RequestId {
        self.request.request_id()
    }

    #[must_use]
    pub const fn operation(&self) -> Operation {
        self.request.operation()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BrokerRequestV3 {
    AcquireOwner(OwnerAcquisitionRequestV3),
    Mutation(OwnerIdentifiedBrokerRequestV3),
}

impl BrokerRequestV3 {
    #[must_use]
    pub fn request_id(&self) -> &RequestId {
        match self {
            Self::AcquireOwner(request) => request.request_id(),
            Self::Mutation(request) => request.request_id(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestEnvelopeV3 {
    version: u32,
    request_id: String,
    operation: String,
    command_session_id: String,
    #[serde(default)]
    expected_owner_epoch: Option<u64>,
    #[serde(default)]
    owner_epoch: Option<u64>,
    owner_instance_id: String,
    #[serde(default)]
    command_sequence: Option<u64>,
    payload: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseEnvelopeV3 {
    version: u32,
    request_id: String,
    ok: bool,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<ResponseErrorV3>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseErrorV3 {
    code: String,
    message: String,
    disposition: String,
}

/// Decode one strict protocol-v3 owner acquisition or owner-aware mutation.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] for malformed JSON, frame overflow, unsupported versions,
/// invalid owner/session identity, invalid field combinations, or invalid Broker mutation payloads.
pub fn decode_request_v3_with_limit(
    frame: &[u8],
    max_frame_bytes: usize,
) -> Result<BrokerRequestV3, ProtocolCodecError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: RequestEnvelopeV3 = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Request frame must be valid protocol-v3 JSON: {error}"
        ))
    })?;
    if envelope.version != PROTOCOL_VERSION_V3 {
        return Err(invalid_request(format!(
            "Unsupported protocol-v3 request version {}.",
            envelope.version
        )));
    }
    if envelope.operation == ACQUIRE_COMMAND_SESSION_OWNER_OPERATION {
        return decode_owner_acquisition(envelope);
    }
    decode_owner_mutation(envelope, max_frame_bytes)
}

fn decode_owner_acquisition(
    envelope: RequestEnvelopeV3,
) -> Result<BrokerRequestV3, ProtocolCodecError> {
    if envelope.owner_epoch.is_some() || envelope.command_sequence.is_some() {
        return Err(invalid_request(
            "protocol-v3 owner acquisition must not contain owner_epoch or command_sequence",
        ));
    }
    let expected_owner_epoch = envelope.expected_owner_epoch.ok_or_else(|| {
        invalid_request("protocol-v3 owner acquisition requires expected_owner_epoch")
    })?;
    if !envelope.payload.as_object().is_some_and(Map::is_empty) {
        return Err(invalid_request(
            "protocol-v3 owner acquisition payload must be an empty object",
        ));
    }
    let request_id =
        RequestId::new(envelope.request_id).map_err(|error| invalid_request(error.to_string()))?;
    let session_id = CommandSessionId::new(envelope.command_session_id)
        .map_err(|error| invalid_request(error.to_string()))?;
    let expected_owner_epoch = SessionOwnerEpoch::new(expected_owner_epoch)
        .map_err(|error| invalid_request(error.to_string()))?;
    let owner_instance_id = SessionOwnerInstanceId::new(envelope.owner_instance_id)
        .map_err(|error| invalid_request(error.to_string()))?;
    Ok(BrokerRequestV3::AcquireOwner(
        OwnerAcquisitionRequestV3::new(
            request_id,
            session_id,
            expected_owner_epoch,
            owner_instance_id,
        ),
    ))
}

fn decode_owner_mutation(
    envelope: RequestEnvelopeV3,
    max_frame_bytes: usize,
) -> Result<BrokerRequestV3, ProtocolCodecError> {
    if envelope.expected_owner_epoch.is_some() {
        return Err(invalid_request(
            "protocol-v3 owner-aware mutation must not contain expected_owner_epoch",
        ));
    }
    let owner_epoch = envelope
        .owner_epoch
        .ok_or_else(|| invalid_request("protocol-v3 owner-aware mutation requires owner_epoch"))?;
    let command_sequence = envelope.command_sequence.ok_or_else(|| {
        invalid_request("protocol-v3 owner-aware mutation requires command_sequence")
    })?;
    let session_id = CommandSessionId::new(envelope.command_session_id)
        .map_err(|error| invalid_request(error.to_string()))?;
    let owner_epoch =
        SessionOwnerEpoch::new(owner_epoch).map_err(|error| invalid_request(error.to_string()))?;
    let owner_instance_id = SessionOwnerInstanceId::new(envelope.owner_instance_id)
        .map_err(|error| invalid_request(error.to_string()))?;
    let sequence = CommandSequence::new(command_sequence)
        .map_err(|error| invalid_request(error.to_string()))?;
    let identity =
        CommandIdentity::new_with_owner(session_id, owner_epoch, owner_instance_id, sequence);
    let v1 = object([
        ("operation", Value::String(envelope.operation)),
        ("payload", envelope.payload),
        ("request_id", Value::String(envelope.request_id)),
        ("version", Value::from(PROTOCOL_VERSION)),
    ]);
    let v1_frame = serialize_bounded(&v1, max_frame_bytes)?;
    let request = decode_request_with_limit(&v1_frame, max_frame_bytes)?;
    OwnerIdentifiedBrokerRequestV3::new(identity, request).map(BrokerRequestV3::Mutation)
}

/// Encode one strict protocol-v3 owner acquisition request.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] when serialization fails or the encoded frame exceeds the bound.
pub fn encode_owner_acquisition_request_with_limit(
    request: &OwnerAcquisitionRequestV3,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    let root = object([
        (
            "command_session_id",
            Value::String(request.session_id().as_str().to_owned()),
        ),
        (
            "expected_owner_epoch",
            Value::from(request.expected_owner_epoch().get()),
        ),
        (
            "operation",
            Value::String(ACQUIRE_COMMAND_SESSION_OWNER_OPERATION.to_owned()),
        ),
        (
            "owner_instance_id",
            Value::String(request.owner_instance_id().as_str().to_owned()),
        ),
        ("payload", Value::Object(Map::new())),
        (
            "request_id",
            Value::String(request.request_id().as_str().to_owned()),
        ),
        ("version", Value::from(PROTOCOL_VERSION_V3)),
    ]);
    serialize_bounded(&root, max_frame_bytes)
}

/// Encode one strict protocol-v3 owner-aware mutation request.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] when the owner identity is incomplete, serialization fails, or
/// the encoded frame exceeds the bound.
pub fn encode_owner_mutation_request_with_limit(
    request: &OwnerIdentifiedBrokerRequestV3,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    let v1_frame = encode_request_with_limit(request.request(), max_frame_bytes)?;
    let mut v1: Value = serde_json::from_slice(&v1_frame)
        .map_err(|error| ProtocolCodecError::Serialization(error.to_string()))?;
    let payload = v1
        .get_mut("payload")
        .map(Value::take)
        .ok_or_else(|| invalid_request("internal protocol-v1 request encoding omitted payload"))?;
    let owner_instance_id = request.identity().owner_instance_id().ok_or_else(|| {
        invalid_request("protocol-v3 owner-aware mutation omitted owner instance identity")
    })?;
    let root = object([
        (
            "command_sequence",
            Value::from(request.identity().sequence().get()),
        ),
        (
            "command_session_id",
            Value::String(request.identity().session_id().as_str().to_owned()),
        ),
        (
            "operation",
            Value::String(request.operation().as_str().to_owned()),
        ),
        (
            "owner_epoch",
            Value::from(request.identity().owner_epoch().get()),
        ),
        (
            "owner_instance_id",
            Value::String(owner_instance_id.as_str().to_owned()),
        ),
        ("payload", payload),
        (
            "request_id",
            Value::String(request.request_id().as_str().to_owned()),
        ),
        ("version", Value::from(PROTOCOL_VERSION_V3)),
    ]);
    serialize_bounded(&root, max_frame_bytes)
}

/// Encode one Broker response using the protocol-v3 envelope.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] when the response cannot be encoded or exceeds the frame bound.
pub fn encode_response_v3_with_limit(
    response: &BrokerResponse,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    if let BrokerResponse::Success {
        request_id,
        result: SuccessPayload::SessionOwnerAcquired { owner_epoch },
    } = response
    {
        return serialize_bounded(
            &object([
                ("ok", Value::Bool(true)),
                ("request_id", Value::String(request_id.as_str().to_owned())),
                (
                    "result",
                    object([("owner_epoch", Value::from(owner_epoch.get()))]),
                ),
                ("version", Value::from(PROTOCOL_VERSION_V3)),
            ]),
            max_frame_bytes,
        );
    }
    let v1_frame = encode_response_with_limit(response, max_frame_bytes)?;
    let mut root: Value = serde_json::from_slice(&v1_frame)
        .map_err(|error| ProtocolCodecError::Serialization(error.to_string()))?;
    let object = root.as_object_mut().ok_or_else(|| {
        invalid_request("internal protocol-v1 response encoding was not an object")
    })?;
    if let BrokerResponse::Error { error, .. } = response {
        let error_object = object
            .get_mut("error")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| invalid_request("internal protocol-v1 error encoding omitted error"))?;
        error_object.insert(
            "disposition".to_owned(),
            Value::String(error.disposition.as_str().to_owned()),
        );
    }
    object.insert("version".to_owned(), Value::from(PROTOCOL_VERSION_V3));
    serialize_bounded(&root, max_frame_bytes)
}

/// Decode one correlated protocol-v3 owner-aware mutation response.
///
/// # Errors
///
/// Returns [`ResponseDecodeError::Protocol`] for malformed/mismatched frames and
/// [`ResponseDecodeError::Broker`] for stable Broker application errors.
pub fn decode_owner_mutation_response_with_limit(
    frame: &[u8],
    expected_request_id: &RequestId,
    operation: Operation,
    max_frame_bytes: usize,
) -> Result<SuccessPayload, ResponseDecodeError> {
    let disposition = response_error_disposition(frame, max_frame_bytes)?;
    let root = v3_response_to_v1(frame, max_frame_bytes)?;
    let v1_frame = serialize_bounded(&root, max_frame_bytes)?;
    match decode_response_for_operation_with_limit(
        &v1_frame,
        expected_request_id,
        operation,
        max_frame_bytes,
    ) {
        Err(ResponseDecodeError::Broker(error)) => Err(ResponseDecodeError::Broker(
            error.with_disposition(disposition.ok_or_else(|| {
                invalid_request("protocol-v3 error response omitted disposition")
            })?),
        )),
        other => other,
    }
}

/// Decode one correlated protocol-v3 owner acquisition response.
///
/// # Errors
///
/// Returns [`ResponseDecodeError::Protocol`] for malformed/mismatched frames and
/// [`ResponseDecodeError::Broker`] for stable Broker acquisition errors.
pub fn decode_owner_acquisition_response_with_limit(
    frame: &[u8],
    expected_request_id: &RequestId,
    max_frame_bytes: usize,
) -> Result<SessionOwnerEpoch, ResponseDecodeError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: ResponseEnvelopeV3 = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Response frame must be valid protocol-v3 JSON: {error}"
        ))
    })?;
    if envelope.version != PROTOCOL_VERSION_V3 {
        return Err(invalid_request(format!(
            "Unsupported response protocol-v3 version {}.",
            envelope.version
        ))
        .into());
    }
    let request_id = RequestId::new(envelope.request_id.clone())
        .map_err(|error| invalid_request(error.to_string()))?;
    if &request_id != expected_request_id {
        return Err(invalid_request("Response request_id does not match request.").into());
    }
    match (envelope.ok, envelope.result, envelope.error) {
        (true, Some(result), None) => {
            let object = result.as_object().ok_or_else(|| {
                invalid_request("protocol-v3 owner acquisition result must be an object")
            })?;
            if object.len() != 1 {
                return Err(invalid_request(
                    "protocol-v3 owner acquisition result contains unknown fields",
                )
                .into());
            }
            let owner_epoch = object
                .get("owner_epoch")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    invalid_request("protocol-v3 owner acquisition result requires owner_epoch")
                })?;
            SessionOwnerEpoch::new(owner_epoch)
                .map_err(|error| invalid_request(error.to_string()).into())
        }
        (false, None, Some(error)) => {
            let disposition = parse_error_disposition(&error.disposition)?;
            let root = object([
                (
                    "error",
                    object([
                        ("code", Value::String(error.code)),
                        ("message", Value::String(error.message)),
                    ]),
                ),
                ("ok", Value::Bool(false)),
                ("request_id", Value::String(request_id.as_str().to_owned())),
                ("version", Value::from(PROTOCOL_VERSION)),
            ]);
            let v1_frame = serialize_bounded(&root, max_frame_bytes)?;
            match decode_response_for_operation_with_limit(
                &v1_frame,
                expected_request_id,
                Operation::EnsureNamespace,
                max_frame_bytes,
            ) {
                Err(ResponseDecodeError::Broker(error)) => Err(ResponseDecodeError::Broker(
                    error.with_disposition(disposition),
                )),
                Err(error) => Err(error),
                Ok(_) => Err(invalid_request(
                    "protocol-v3 owner acquisition error decoded as success",
                )
                .into()),
            }
        }
        (true, _, _) => Err(invalid_request(
            "protocol-v3 success response must contain result and no error",
        )
        .into()),
        (false, _, _) => Err(invalid_request(
            "protocol-v3 error response must contain error and no result",
        )
        .into()),
    }
}

fn response_error_disposition(
    frame: &[u8],
    max_frame_bytes: usize,
) -> Result<Option<BrokerErrorDisposition>, ProtocolCodecError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: ResponseEnvelopeV3 = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Response frame must be valid protocol-v3 JSON: {error}"
        ))
    })?;
    envelope
        .error
        .as_ref()
        .map(|error| parse_error_disposition(&error.disposition))
        .transpose()
}

fn parse_error_disposition(value: &str) -> Result<BrokerErrorDisposition, ProtocolCodecError> {
    match value {
        "COMMITTED" => Ok(BrokerErrorDisposition::Committed),
        "REJECTED" => Ok(BrokerErrorDisposition::Rejected),
        "UNKNOWN" => Ok(BrokerErrorDisposition::Unknown),
        other => Err(invalid_request(format!(
            "Unsupported protocol-v3 error disposition {other}."
        ))),
    }
}

fn v3_response_to_v1(frame: &[u8], max_frame_bytes: usize) -> Result<Value, ProtocolCodecError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: ResponseEnvelopeV3 = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Response frame must be valid protocol-v3 JSON: {error}"
        ))
    })?;
    if envelope.version != PROTOCOL_VERSION_V3 {
        return Err(invalid_request(format!(
            "Unsupported response protocol-v3 version {}.",
            envelope.version
        )));
    }
    match (envelope.ok, envelope.result, envelope.error) {
        (true, Some(result), None) => Ok(object([
            ("ok", Value::Bool(true)),
            ("request_id", Value::String(envelope.request_id)),
            ("result", result),
            ("version", Value::from(PROTOCOL_VERSION)),
        ])),
        (false, None, Some(error)) => Ok(object([
            (
                "error",
                object([
                    ("code", Value::String(error.code)),
                    ("message", Value::String(error.message)),
                ]),
            ),
            ("ok", Value::Bool(false)),
            ("request_id", Value::String(envelope.request_id)),
            ("version", Value::from(PROTOCOL_VERSION)),
        ])),
        (true, _, _) => Err(invalid_request(
            "protocol-v3 success response must contain result and no error",
        )),
        (false, _, _) => Err(invalid_request(
            "protocol-v3 error response must contain error and no result",
        )),
    }
}

fn ensure_frame_limit(actual: usize, max: usize) -> Result<(), ProtocolCodecError> {
    if actual > max {
        return Err(ProtocolCodecError::FrameTooLarge { actual, max });
    }
    Ok(())
}

fn serialize_bounded(value: &Value, max: usize) -> Result<Vec<u8>, ProtocolCodecError> {
    let mut encoded = serde_json::to_vec(value)
        .map_err(|error| ProtocolCodecError::Serialization(error.to_string()))?;
    encoded.push(b'\n');
    ensure_frame_limit(encoded.len(), max)?;
    Ok(encoded)
}

fn object<const N: usize>(entries: [(&'static str, Value); N]) -> Value {
    let ordered: BTreeMap<String, Value> = entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect();
    Value::Object(ordered.into_iter().collect())
}

fn invalid_request(message: impl Into<String>) -> ProtocolCodecError {
    ProtocolCodecError::InvalidRequest(message.into())
}

#[cfg(test)]
mod tests {
    use agent_broker_application::{
        BrokerError, BrokerErrorCode, BrokerErrorDisposition, CommandIdentity, CommandSequence,
        CommandSessionId, SessionOwnerEpoch, SessionOwnerInstanceId,
    };
    use agent_broker_domain::NamespaceId;

    use super::*;
    use crate::{BrokerRequest, EnsureNamespaceRequest};

    #[test]
    fn owner_acquisition_round_trips_strict_v3() -> Result<(), Box<dyn std::error::Error>> {
        let request = OwnerAcquisitionRequestV3::new(
            RequestId::new("v3-acquire-1")?,
            CommandSessionId::new("session-a")?,
            SessionOwnerEpoch::INITIAL,
            SessionOwnerInstanceId::new("process-a")?,
        );
        let frame = encode_owner_acquisition_request_with_limit(&request, 16 * 1024)?;
        let decoded = decode_request_v3_with_limit(&frame, 16 * 1024)?;
        assert_eq!(decoded, BrokerRequestV3::AcquireOwner(request));
        Ok(())
    }

    #[test]
    fn owner_mutation_round_trips_strict_v3() -> Result<(), Box<dyn std::error::Error>> {
        let identity = CommandIdentity::new_with_owner(
            CommandSessionId::new("session-a")?,
            SessionOwnerEpoch::new(2)?,
            SessionOwnerInstanceId::new("process-a")?,
            CommandSequence::new(1)?,
        );
        let request = OwnerIdentifiedBrokerRequestV3::new(
            identity,
            BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
                request_id: RequestId::new("v3-mutation-1")?,
                namespace_id: NamespaceId::new("v3-owned")?,
            }),
        )?;
        let frame = encode_owner_mutation_request_with_limit(&request, 16 * 1024)?;
        let decoded = decode_request_v3_with_limit(&frame, 16 * 1024)?;
        assert_eq!(decoded, BrokerRequestV3::Mutation(request));
        Ok(())
    }

    #[test]
    fn acquisition_rejects_mutation_only_fields() {
        let frame = br#"{"version":3,"request_id":"v3-bad","operation":"acquire_command_session_owner","command_session_id":"session-a","expected_owner_epoch":1,"owner_epoch":2,"owner_instance_id":"process-a","payload":{}}
"#;
        assert!(decode_request_v3_with_limit(frame, 16 * 1024).is_err());
    }

    #[test]
    fn v3_error_disposition_round_trips_without_widening_v1_v2()
    -> Result<(), Box<dyn std::error::Error>> {
        let request_id = RequestId::new("v3-disposition")?;
        for disposition in [
            BrokerErrorDisposition::Committed,
            BrokerErrorDisposition::Rejected,
            BrokerErrorDisposition::Unknown,
        ] {
            let response = BrokerResponse::error(
                request_id.clone(),
                BrokerError::new(BrokerErrorCode::Conflict, "disposition-test")
                    .with_disposition(disposition),
            );
            let frame = encode_response_v3_with_limit(&response, 16 * 1024)?;
            let value: serde_json::Value = serde_json::from_slice(&frame)?;
            assert_eq!(
                value["error"]["disposition"].as_str(),
                Some(disposition.as_str())
            );
            let decoded = decode_owner_mutation_response_with_limit(
                &frame,
                &request_id,
                Operation::EnsureNamespace,
                16 * 1024,
            );
            let error = match decoded {
                Err(ResponseDecodeError::Broker(error)) => error,
                other => return Err(format!("expected v3 Broker error, got {other:?}").into()),
            };
            assert_eq!(error.code(), BrokerErrorCode::Conflict);
            assert_eq!(error.disposition(), disposition);
        }
        Ok(())
    }

    #[test]
    fn v3_error_disposition_is_required_and_strict() -> Result<(), Box<dyn std::error::Error>> {
        let request_id = RequestId::new("v3-disposition-bad")?;
        let missing = br#"{"version":3,"request_id":"v3-disposition-bad","ok":false,"error":{"code":"CONFLICT","message":"missing"}}
"#;
        assert!(matches!(
            decode_owner_mutation_response_with_limit(
                missing,
                &request_id,
                Operation::EnsureNamespace,
                16 * 1024
            ),
            Err(ResponseDecodeError::Protocol(_))
        ));

        let unknown = br#"{"version":3,"request_id":"v3-disposition-bad","ok":false,"error":{"code":"CONFLICT","message":"unknown","disposition":"MAYBE"}}
"#;
        assert!(matches!(
            decode_owner_mutation_response_with_limit(
                unknown,
                &request_id,
                Operation::EnsureNamespace,
                16 * 1024
            ),
            Err(ResponseDecodeError::Protocol(_))
        ));
        Ok(())
    }

    #[test]
    fn v3_error_encoding_preserves_v1_message_bound() -> Result<(), Box<dyn std::error::Error>> {
        let response = BrokerResponse::error(
            RequestId::new("v3-error-bound")?,
            BrokerError::new(
                BrokerErrorCode::InternalError,
                "x".repeat(crate::MAX_ERROR_MESSAGE_BYTES + 1),
            )
            .with_disposition(BrokerErrorDisposition::Committed),
        );
        assert!(encode_response_v3_with_limit(&response, 256 * 1024).is_err());
        Ok(())
    }

    #[test]
    fn frozen_v1_v2_do_not_accept_v3_owner_surface() {
        let v1_acquisition = br#"{"version":1,"request_id":"v1-reject-v3","operation":"acquire_command_session_owner","payload":{}}
"#;
        assert!(crate::decode_wire_request_with_limit(v1_acquisition, 16 * 1024).is_err());

        let v2_owner_field = br#"{"version":2,"request_id":"v2-reject-v3","operation":"ensure_namespace","command_session_id":"session-a","command_sequence":1,"owner_instance_id":"process-a","payload":{"namespace_id":"v2-must-reject-owner-field"}}
"#;
        assert!(crate::decode_wire_request_with_limit(v2_owner_field, 16 * 1024).is_err());
    }
}

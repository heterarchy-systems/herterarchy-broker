use std::collections::BTreeMap;

use agent_broker_application::{CommandIdentity, CommandSequence, CommandSessionId};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    BrokerRequest, BrokerRequestV3, BrokerResponse, Operation, PROTOCOL_VERSION,
    PROTOCOL_VERSION_V3, ProtocolCodecError, RequestId, ResponseDecodeError, SuccessPayload,
    decode_request_v3_with_limit, decode_request_with_limit,
    decode_response_for_operation_with_limit, encode_request_with_limit,
    encode_response_with_limit,
};

pub const PROTOCOL_VERSION_V2: u32 = 2;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IdentifiedBrokerRequest {
    identity: CommandIdentity,
    request: BrokerRequest,
}

impl IdentifiedBrokerRequest {
    /// Construct a mutation-only protocol-v2 request.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolCodecError`] when `request` is the read-only health operation.
    pub fn new(
        identity: CommandIdentity,
        request: BrokerRequest,
    ) -> Result<Self, ProtocolCodecError> {
        if request.operation() == Operation::Health {
            return Err(invalid_request(
                "protocol-v2 identified requests are mutation-only; health remains protocol-v1/read-only",
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

    #[must_use]
    pub fn into_parts(self) -> (CommandIdentity, BrokerRequest) {
        (self.identity, self.request)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BrokerWireRequest {
    V1(BrokerRequest),
    V2(IdentifiedBrokerRequest),
    V3(BrokerRequestV3),
}

#[must_use]
pub fn request_version_hint(frame: &[u8]) -> Option<u32> {
    serde_json::from_slice::<VersionProbe>(frame)
        .ok()
        .map(|probe| probe.version)
}

impl BrokerWireRequest {
    #[must_use]
    pub fn request_id(&self) -> &RequestId {
        match self {
            Self::V1(request) => request.request_id(),
            Self::V2(request) => request.request_id(),
            Self::V3(request) => request.request_id(),
        }
    }

    #[must_use]
    pub const fn protocol_version(&self) -> u32 {
        match self {
            Self::V1(_) => PROTOCOL_VERSION,
            Self::V2(_) => PROTOCOL_VERSION_V2,
            Self::V3(_) => PROTOCOL_VERSION_V3,
        }
    }
}

#[derive(Debug, Deserialize)]
struct VersionProbe {
    version: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestEnvelopeV2 {
    version: u32,
    request_id: String,
    operation: String,
    command_session_id: String,
    command_sequence: u64,
    payload: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseEnvelopeV2 {
    version: u32,
    request_id: String,
    ok: bool,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<ResponseErrorV2>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseErrorV2 {
    code: String,
    message: String,
}

/// Decode either a strict legacy protocol-v1 request or a strict identified protocol-v2 mutation.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] for malformed JSON, unsupported versions, schema drift, invalid
/// identity, invalid Broker payloads, or frame-size violations.
pub fn decode_wire_request(frame: &[u8]) -> Result<BrokerWireRequest, ProtocolCodecError> {
    decode_wire_request_with_limit(frame, crate::DEFAULT_MAX_FRAME_BYTES)
}

/// Decode one request using an explicit shared frame bound.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] under the same conditions as [`decode_wire_request`].
pub fn decode_wire_request_with_limit(
    frame: &[u8],
    max_frame_bytes: usize,
) -> Result<BrokerWireRequest, ProtocolCodecError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let probe: VersionProbe = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!("Request frame must be valid Broker JSON: {error}"))
    })?;
    match probe.version {
        PROTOCOL_VERSION => {
            decode_request_with_limit(frame, max_frame_bytes).map(BrokerWireRequest::V1)
        }
        PROTOCOL_VERSION_V2 => {
            decode_identified_request_with_limit(frame, max_frame_bytes).map(BrokerWireRequest::V2)
        }
        PROTOCOL_VERSION_V3 => {
            decode_request_v3_with_limit(frame, max_frame_bytes).map(BrokerWireRequest::V3)
        }
        version => Err(invalid_request(format!(
            "Unsupported protocol version {version}."
        ))),
    }
}

fn decode_identified_request_with_limit(
    frame: &[u8],
    max_frame_bytes: usize,
) -> Result<IdentifiedBrokerRequest, ProtocolCodecError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: RequestEnvelopeV2 = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Request frame must be valid protocol-v2 JSON: {error}"
        ))
    })?;
    if envelope.version != PROTOCOL_VERSION_V2 {
        return Err(invalid_request(format!(
            "Unsupported protocol-v2 request version {}.",
            envelope.version
        )));
    }
    let session_id = CommandSessionId::new(envelope.command_session_id)
        .map_err(|error| invalid_request(error.to_string()))?;
    let sequence = CommandSequence::new(envelope.command_sequence)
        .map_err(|error| invalid_request(error.to_string()))?;
    let identity = CommandIdentity::new(session_id, sequence);
    let v1 = object([
        ("operation", Value::String(envelope.operation)),
        ("payload", envelope.payload),
        ("request_id", Value::String(envelope.request_id)),
        ("version", Value::from(PROTOCOL_VERSION)),
    ]);
    let v1_frame = serialize_bounded(&v1, max_frame_bytes)?;
    let request = decode_request_with_limit(&v1_frame, max_frame_bytes)?;
    IdentifiedBrokerRequest::new(identity, request)
}

/// Encode one strict protocol-v2 identified mutation request.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] when serialization fails or the encoded frame exceeds the
/// default bound.
pub fn encode_identified_request(
    request: &IdentifiedBrokerRequest,
) -> Result<Vec<u8>, ProtocolCodecError> {
    encode_identified_request_with_limit(request, crate::DEFAULT_MAX_FRAME_BYTES)
}

/// Encode one protocol-v2 identified request using an explicit frame bound.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] when serialization fails or the encoded frame exceeds the bound.
pub fn encode_identified_request_with_limit(
    request: &IdentifiedBrokerRequest,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    let v1_frame = encode_request_with_limit(request.request(), max_frame_bytes)?;
    let mut v1: Value = serde_json::from_slice(&v1_frame)
        .map_err(|error| ProtocolCodecError::Serialization(error.to_string()))?;
    let payload = v1
        .get_mut("payload")
        .map(Value::take)
        .ok_or_else(|| invalid_request("internal protocol-v1 request encoding omitted payload"))?;
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
        ("payload", payload),
        (
            "request_id",
            Value::String(request.request_id().as_str().to_owned()),
        ),
        ("version", Value::from(PROTOCOL_VERSION_V2)),
    ]);
    serialize_bounded(&root, max_frame_bytes)
}

/// Encode one Broker response as a protocol-v2 response envelope.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] when the underlying response is invalid, serialization fails, or
/// the encoded frame exceeds the default bound.
pub fn encode_response_v2(response: &BrokerResponse) -> Result<Vec<u8>, ProtocolCodecError> {
    encode_response_v2_with_limit(response, crate::DEFAULT_MAX_FRAME_BYTES)
}

/// Encode one protocol-v2 response using an explicit frame bound.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] under the same conditions as [`encode_response_v2`].
pub fn encode_response_v2_with_limit(
    response: &BrokerResponse,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    let v1_frame = encode_response_with_limit(response, max_frame_bytes)?;
    let mut root: Value = serde_json::from_slice(&v1_frame)
        .map_err(|error| ProtocolCodecError::Serialization(error.to_string()))?;
    let object = root.as_object_mut().ok_or_else(|| {
        invalid_request("internal protocol-v1 response encoding was not an object")
    })?;
    object.insert("version".to_owned(), Value::from(PROTOCOL_VERSION_V2));
    serialize_bounded(&root, max_frame_bytes)
}

/// Decode one correlated protocol-v2 response for the operation that produced it.
///
/// # Errors
///
/// Returns [`ResponseDecodeError::Protocol`] for malformed/mismatched response frames and
/// [`ResponseDecodeError::Broker`] for stable Broker application errors.
pub fn decode_response_v2_for_operation(
    frame: &[u8],
    expected_request_id: &RequestId,
    operation: Operation,
) -> Result<SuccessPayload, ResponseDecodeError> {
    decode_response_v2_for_operation_with_limit(
        frame,
        expected_request_id,
        operation,
        crate::DEFAULT_MAX_FRAME_BYTES,
    )
}

/// Decode one correlated protocol-v2 response using an explicit frame bound.
///
/// # Errors
///
/// Returns the same error categories as [`decode_response_v2_for_operation`].
pub fn decode_response_v2_for_operation_with_limit(
    frame: &[u8],
    expected_request_id: &RequestId,
    operation: Operation,
    max_frame_bytes: usize,
) -> Result<SuccessPayload, ResponseDecodeError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: ResponseEnvelopeV2 = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Response frame must be valid protocol-v2 JSON: {error}"
        ))
    })?;
    if envelope.version != PROTOCOL_VERSION_V2 {
        return Err(invalid_request(format!(
            "Unsupported response protocol-v2 version {}.",
            envelope.version
        ))
        .into());
    }
    let root = match (envelope.ok, envelope.result, envelope.error) {
        (true, Some(result), None) => object([
            ("ok", Value::Bool(true)),
            ("request_id", Value::String(envelope.request_id)),
            ("result", result),
            ("version", Value::from(PROTOCOL_VERSION)),
        ]),
        (false, None, Some(error)) => object([
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
        ]),
        (true, _, _) => {
            return Err(invalid_request(
                "protocol-v2 success response must contain result and no error",
            )
            .into());
        }
        (false, _, _) => {
            return Err(invalid_request(
                "protocol-v2 error response must contain error and no result",
            )
            .into());
        }
    };
    let v1_frame = serialize_bounded(&root, max_frame_bytes)?;
    decode_response_for_operation_with_limit(
        &v1_frame,
        expected_request_id,
        operation,
        max_frame_bytes,
    )
}

fn serialize_bounded(value: &Value, max_frame_bytes: usize) -> Result<Vec<u8>, ProtocolCodecError> {
    let mut encoded = serde_json::to_vec(value)
        .map_err(|error| ProtocolCodecError::Serialization(error.to_string()))?;
    encoded.push(b'\n');
    ensure_frame_limit(encoded.len(), max_frame_bytes)?;
    Ok(encoded)
}

fn ensure_frame_limit(actual: usize, max: usize) -> Result<(), ProtocolCodecError> {
    if actual > max {
        return Err(ProtocolCodecError::FrameTooLarge { actual, max });
    }
    Ok(())
}

fn invalid_request(message: impl Into<String>) -> ProtocolCodecError {
    ProtocolCodecError::InvalidRequest(message.into())
}

fn object<const N: usize>(entries: [(&'static str, Value); N]) -> Value {
    let map = entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect::<BTreeMap<_, _>>();
    Value::Object(map.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use agent_broker_application::{
        BrokerError, BrokerErrorCode, CommandIdentity, CommandSequence, CommandSessionId,
    };
    use agent_broker_domain::NamespaceId;

    use super::*;
    use crate::{EnsureNamespaceRequest, HealthRequest};

    #[test]
    fn v2_identified_request_round_trips_without_changing_v1_request_shape()
    -> Result<(), Box<dyn std::error::Error>> {
        let request_id = RequestId::new("v2-req-1")?;
        let identified = IdentifiedBrokerRequest::new(
            CommandIdentity::new(CommandSessionId::new("client-a")?, CommandSequence::new(7)?),
            BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
                request_id: request_id.clone(),
                namespace_id: NamespaceId::new("v2-ns")?,
            }),
        )?;
        let frame = encode_identified_request(&identified)?;
        let decoded = decode_wire_request(&frame)?;
        assert_eq!(decoded, BrokerWireRequest::V2(identified));
        assert_eq!(decoded.request_id(), &request_id);
        assert_eq!(decoded.protocol_version(), PROTOCOL_VERSION_V2);
        Ok(())
    }

    #[test]
    fn v2_rejects_health_and_invalid_sequence() -> Result<(), Box<dyn std::error::Error>> {
        let health = IdentifiedBrokerRequest::new(
            CommandIdentity::new(CommandSessionId::new("client-a")?, CommandSequence::new(1)?),
            BrokerRequest::Health(HealthRequest {
                request_id: RequestId::new("health-v2")?,
            }),
        );
        assert!(health.is_err());

        let invalid = br#"{"version":2,"request_id":"req","operation":"namespace.ensure","command_session_id":"client-a","command_sequence":0,"payload":{"namespace_id":"x"}}\n"#;
        assert!(decode_wire_request(invalid).is_err());
        Ok(())
    }

    #[test]
    fn v2_response_decodes_commit_outcome_unknown() -> Result<(), Box<dyn std::error::Error>> {
        let request_id = RequestId::new("v2-error")?;
        let response = BrokerResponse::error(
            request_id.clone(),
            BrokerError::new(
                BrokerErrorCode::CommitOutcomeUnknown,
                "submitted but unresolved",
            ),
        );
        let frame = encode_response_v2(&response)?;
        let decoded =
            decode_response_v2_for_operation(&frame, &request_id, Operation::EnsureNamespace);
        let error = match decoded {
            Err(ResponseDecodeError::Broker(error)) => error,
            other => return Err(format!("unexpected v2 response decode result: {other:?}").into()),
        };
        assert_eq!(error.code(), BrokerErrorCode::CommitOutcomeUnknown);
        Ok(())
    }
}

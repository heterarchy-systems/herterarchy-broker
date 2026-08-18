from __future__ import annotations

import json
import math
import re
from enum import Enum
from typing import TypeAlias, TypeVar

from .errors import BrokerError, BrokerErrorCode, ErrorDisposition, ProtocolError
from .models import (
    BrokerResult,
    ConsumerGroupResult,
    HealthResult,
    HeartbeatResult,
    NamespaceResult,
    OwnerAcquisitionResult,
    TaskClaimResult,
    TaskCompletedResult,
    TaskLeaseRenewedResult,
    TaskPublishedResult,
)

PROTOCOL_V1 = 1
PROTOCOL_V3 = 3
OWNER_ACQUIRE_OPERATION = "acquire_command_session_owner"
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")

JsonValue: TypeAlias = (
    str
    | int
    | float
    | bool
    | None
    | list["JsonValue"]
    | tuple["JsonValue", ...]
    | dict[str, "JsonValue"]
)
JsonObject: TypeAlias = dict[str, JsonValue]
_ResultT = TypeVar("_ResultT", bound=BrokerResult)


class Operation(str, Enum):
    """Protocol operation tokens supported by Broker request/response encoding.

    Args:
            None

    Returns:
        None: No value.
    """

    HEALTH = "health"
    ENSURE_NAMESPACE = "namespace.ensure"
    PUBLISH_TASK = "task.publish"
    ENSURE_GROUP = "group.ensure"
    JOIN_GROUP = "group.join"
    HEARTBEAT = "group.heartbeat"
    LEAVE_GROUP = "group.leave"
    CLAIM_TASK = "task.claim"
    RENEW_TASK = "task.renew"
    COMPLETE_TASK = "task.complete"


def validate_request_id(request_id: str) -> str:
    """Validate a broker request identifier against protocol constraints.

    Args:
            request_id: str

    Returns:
        str: Result of the operation.
    """

    if not isinstance(request_id, str) or not REQUEST_ID_RE.fullmatch(request_id):
        raise ValueError(
            "request_id must be 1..=128 ASCII bytes, start alphanumeric, and then use only alphanumeric/_/./:/-"
        )
    return request_id


def _serialize(envelope: JsonObject) -> bytes:
    """Serialize a protocol envelope into a newline-delimited UTF-8 frame.

    Args:
            envelope: JsonObject

    Returns:
        bytes: Result of the operation.
    """

    try:
        encoded = (
            json.dumps(
                envelope,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            ).encode("utf-8")
            + b"\n"
        )
    except (TypeError, ValueError) as error:
        raise ProtocolError(f"request is not JSON serializable: {error}") from error
    return encoded


def encode_health(request_id: str) -> bytes:
    """Encode a health-check request payload frame.

    Args:
            request_id: str

    Returns:
        bytes: Result of the operation.
    """

    validate_request_id(request_id)
    return _serialize(
        {
            "version": PROTOCOL_V1,
            "request_id": request_id,
            "operation": Operation.HEALTH.value,
            "payload": {},
        }
    )


def encode_mutation_v1(
    request_id: str,
    operation: Operation,
    payload: JsonObject,
) -> bytes:
    """Encode a protocol-v1 mutation request frame.

    Args:
            request_id: str
            operation: Operation
            payload: JsonObject

    Returns:
        bytes: Result of the operation.
    """

    if operation is Operation.HEALTH:
        raise ValueError("health is read-only and must use encode_health")
    validate_request_id(request_id)
    return _serialize(
        {
            "version": PROTOCOL_V1,
            "request_id": request_id,
            "operation": operation.value,
            "payload": payload,
        }
    )


def encode_owner_acquire(
    request_id: str,
    session_id: str,
    expected_owner_epoch: int,
    owner_instance_id: str,
) -> bytes:
    """Encode an owner acquisition request frame.

    Args:
            request_id: str
            session_id: str
            expected_owner_epoch: int
            owner_instance_id: str

    Returns:
        bytes: Result of the operation.
    """

    validate_request_id(request_id)
    if (
        isinstance(expected_owner_epoch, bool)
        or not isinstance(expected_owner_epoch, int)
        or expected_owner_epoch <= 0
    ):
        raise ValueError("expected_owner_epoch must be a positive integer")
    return _serialize(
        {
            "version": PROTOCOL_V3,
            "request_id": request_id,
            "operation": OWNER_ACQUIRE_OPERATION,
            "command_session_id": session_id,
            "expected_owner_epoch": expected_owner_epoch,
            "owner_instance_id": owner_instance_id,
            "payload": {},
        }
    )


def encode_owner_mutation(
    request_id: str,
    operation: Operation,
    payload: JsonObject,
    *,
    session_id: str,
    owner_epoch: int,
    owner_instance_id: str,
    sequence: int,
) -> bytes:
    """Encode an owner-scoped mutation request frame.

    Args:
            request_id: str
            operation: Operation
            payload: JsonObject
            session_id: str
            owner_epoch: int
            owner_instance_id: str
            sequence: int

    Returns:
        bytes: Result of the operation.
    """

    if operation is Operation.HEALTH:
        raise ValueError(
            "health is protocol-v1 read-only and cannot use owner-aware mutation"
        )
    validate_request_id(request_id)
    return _serialize(
        {
            "version": PROTOCOL_V3,
            "request_id": request_id,
            "operation": operation.value,
            "command_session_id": session_id,
            "owner_epoch": owner_epoch,
            "owner_instance_id": owner_instance_id,
            "command_sequence": sequence,
            "payload": payload,
        }
    )


def decode_health_response(frame: bytes, request_id: str) -> HealthResult:
    """Decode and validate a health response frame.

    Args:
            frame: bytes
            request_id: str

    Returns:
        HealthResult: Result of the operation.
    """

    envelope = _decode_envelope(
        frame, request_id, PROTOCOL_V1, disposition_required=False
    )
    result = _success_result(envelope)
    _require_keys(result, {"protocol_version", "term", "revision"}, "health result")
    protocol_version = _integer(result["protocol_version"], "protocol_version")
    if protocol_version != PROTOCOL_V1:
        raise ProtocolError("health protocol_version does not match protocol-v1")
    return HealthResult(
        protocol_version,
        _integer(result["term"], "term"),
        _integer(result["revision"], "revision"),
    )


def decode_owner_acquire_response(
    frame: bytes, request_id: str
) -> OwnerAcquisitionResult:
    """Decode and validate an owner acquisition response frame.

    Args:
            frame: bytes
            request_id: str

    Returns:
        OwnerAcquisitionResult: Result of the operation.
    """

    envelope = _decode_envelope(
        frame, request_id, PROTOCOL_V3, disposition_required=True
    )
    result = _success_result(envelope)
    _require_keys(result, {"owner_epoch"}, "owner acquisition result")
    epoch = _integer(result["owner_epoch"], "owner_epoch")
    if epoch <= 0:
        raise ProtocolError("owner_epoch must be positive")
    return OwnerAcquisitionResult(epoch)


def decode_mutation_response(
    frame: bytes, request_id: str, operation: Operation
) -> BrokerResult:
    """Decode and validate a mutation response frame.

    Args:
            frame: bytes
            request_id: str
            operation: Operation

    Returns:
        BrokerResult: Result of the operation.
    """

    envelope = _decode_envelope(
        frame, request_id, PROTOCOL_V3, disposition_required=True
    )
    return _decode_mutation_result(_success_result(envelope), operation)


def decode_mutation_response_v1(
    frame: bytes, request_id: str, operation: Operation
) -> BrokerResult:
    """Decode and validate a protocol-v1 mutation response frame.

    Args:
            frame: bytes
            request_id: str
            operation: Operation

    Returns:
        BrokerResult: Result of the operation.
    """

    envelope = _decode_envelope(
        frame, request_id, PROTOCOL_V1, disposition_required=False
    )
    return _decode_mutation_result(_success_result(envelope), operation)


def expect_mutation_result_type(
    decoded_result: BrokerResult,
    expected_result_type: type[_ResultT],
    operation: Operation,
) -> _ResultT:
    """Fail closed when a mutation decoder returns the wrong result variant."""

    if not isinstance(decoded_result, expected_result_type):
        raise ProtocolError(
            f"{operation.value} returned unexpected result type "
            f"{type(decoded_result).__name__}"
        )
    return decoded_result


def _decode_mutation_result(result: JsonObject, operation: Operation) -> BrokerResult:
    """Decode the operation-specific payload from a mutation response.

    Args:
            result: JsonObject
            operation: Operation

    Returns:
        BrokerResult: Result of the operation.
    """

    if operation is Operation.ENSURE_NAMESPACE:
        _require_keys(
            result,
            {"term", "revision", "namespace_id", "namespace_revision"},
            "namespace result",
        )
        return NamespaceResult(
            _integer(result["term"], "term"),
            _integer(result["revision"], "revision"),
            _string(result["namespace_id"], "namespace_id"),
            _integer(result["namespace_revision"], "namespace_revision"),
        )
    if operation is Operation.PUBLISH_TASK:
        _require_keys(
            result,
            {"term", "revision", "task_id", "task_revision", "status"},
            "task publish result",
        )
        return TaskPublishedResult(
            _integer(result["term"], "term"),
            _integer(result["revision"], "revision"),
            _string(result["task_id"], "task_id"),
            _integer(result["task_revision"], "task_revision"),
            _task_status(result["status"]),
        )
    if operation in {
        Operation.ENSURE_GROUP,
        Operation.JOIN_GROUP,
        Operation.LEAVE_GROUP,
    }:
        _require_keys(
            result,
            {
                "term",
                "revision",
                "group_id",
                "generation",
                "group_revision",
                "member_count",
            },
            "consumer group result",
        )
        return ConsumerGroupResult(
            _integer(result["term"], "term"),
            _integer(result["revision"], "revision"),
            _string(result["group_id"], "group_id"),
            _integer(result["generation"], "generation"),
            _integer(result["group_revision"], "group_revision"),
            _integer(result["member_count"], "member_count"),
        )
    if operation is Operation.HEARTBEAT:
        _require_keys(
            result,
            {
                "term",
                "revision",
                "group_id",
                "member_id",
                "generation",
                "member_revision",
            },
            "heartbeat result",
        )
        return HeartbeatResult(
            _integer(result["term"], "term"),
            _integer(result["revision"], "revision"),
            _string(result["group_id"], "group_id"),
            _string(result["member_id"], "member_id"),
            _integer(result["generation"], "generation"),
            _integer(result["member_revision"], "member_revision"),
        )
    if operation is Operation.CLAIM_TASK:
        _require_keys(
            result,
            {
                "term",
                "revision",
                "task_id",
                "objective",
                "task_revision",
                "lease_id",
                "lease_epoch",
                "lease_expires_at_ms",
                "generation",
            },
            "task claim result",
        )
        optional = [
            result["task_id"],
            result["objective"],
            result["task_revision"],
            result["lease_id"],
            result["lease_epoch"],
            result["lease_expires_at_ms"],
        ]
        if any(value is None for value in optional) and not all(
            value is None for value in optional
        ):
            raise ProtocolError(
                "task claim result must contain a complete lease payload or all null fields"
            )
        return TaskClaimResult(
            _integer(result["term"], "term"),
            _integer(result["revision"], "revision"),
            _optional_string(result["task_id"], "task_id"),
            _optional_string(result["objective"], "objective"),
            _optional_integer(result["task_revision"], "task_revision"),
            _optional_string(result["lease_id"], "lease_id"),
            _optional_integer(result["lease_epoch"], "lease_epoch"),
            _optional_integer(result["lease_expires_at_ms"], "lease_expires_at_ms"),
            _integer(result["generation"], "generation"),
        )
    if operation is Operation.RENEW_TASK:
        _require_keys(
            result,
            {
                "term",
                "revision",
                "task_id",
                "task_revision",
                "lease_id",
                "lease_epoch",
                "lease_expires_at_ms",
                "generation",
            },
            "task renew result",
        )
        return TaskLeaseRenewedResult(
            _integer(result["term"], "term"),
            _integer(result["revision"], "revision"),
            _string(result["task_id"], "task_id"),
            _integer(result["task_revision"], "task_revision"),
            _string(result["lease_id"], "lease_id"),
            _integer(result["lease_epoch"], "lease_epoch"),
            _integer(result["lease_expires_at_ms"], "lease_expires_at_ms"),
            _integer(result["generation"], "generation"),
        )
    if operation is Operation.COMPLETE_TASK:
        _require_keys(
            result,
            {"term", "revision", "task_id", "task_revision", "status"},
            "task complete result",
        )
        return TaskCompletedResult(
            _integer(result["term"], "term"),
            _integer(result["revision"], "revision"),
            _string(result["task_id"], "task_id"),
            _integer(result["task_revision"], "task_revision"),
            _task_status(result["status"]),
        )
    raise ProtocolError(f"unsupported mutation response operation: {operation.value}")


def _decode_envelope(
    frame: bytes,
    request_id: str,
    version: int,
    *,
    disposition_required: bool,
) -> JsonObject:
    """Decode and validate the top-level protocol envelope.

    Args:
            frame: bytes
            request_id: str
            version: int
            disposition_required: bool

    Returns:
        JsonObject: Result of the operation.
    """

    try:
        raw_value: object = json.loads(frame)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError(f"response frame is not valid JSON: {error}") from error
    value = _normalize_json_value(raw_value)
    if not isinstance(value, dict):
        raise ProtocolError("response frame must be a JSON object")
    if value.get("version") != version:
        raise ProtocolError(
            f"unsupported response protocol version {value.get('version')!r}"
        )
    if value.get("request_id") != request_id:
        raise ProtocolError(
            f"response request_id {value.get('request_id')!r} does not match expected {request_id!r}"
        )
    if not isinstance(value.get("ok"), bool):
        raise ProtocolError("response ok must be boolean")
    if value["ok"]:
        if "result" not in value or value.get("error") is not None:
            raise ProtocolError("success response must contain result and no error")
        return value
    if value.get("result") is not None or "error" not in value:
        raise ProtocolError("error response must contain error and no result")
    error = value["error"]
    if not isinstance(error, dict):
        raise ProtocolError("error must be a JSON object")
    expected_error_keys = (
        {"code", "message", "disposition"}
        if disposition_required
        else {"code", "message"}
    )
    _require_keys(error, expected_error_keys, "error")
    try:
        code = BrokerErrorCode(_string(error["code"], "error.code"))
    except ValueError as exc:
        raise ProtocolError(f"unknown Broker error code {error['code']!r}") from exc
    message = _string(error["message"], "error.message")
    if len(message.encode("utf-8")) > 4096:
        raise ProtocolError("error.message exceeds 4096 bytes")
    disposition = None
    if disposition_required:
        try:
            disposition = ErrorDisposition(
                _string(error["disposition"], "error.disposition")
            )
        except ValueError as exc:
            raise ProtocolError(
                f"unknown error disposition {error['disposition']!r}"
            ) from exc
    raise BrokerError(code, message, disposition)


def _success_result(envelope: JsonObject) -> JsonObject:
    """Extract and normalize the success payload from an envelope.

    Args:
            envelope: JsonObject

    Returns:
        JsonObject: Result of the operation.
    """

    result = envelope["result"]
    if not isinstance(result, dict):
        raise ProtocolError("success result must be a JSON object")
    return result


def _require_keys(value: JsonObject, expected: set[str], label: str) -> None:
    """Ensure required keys are present in a decoded dictionary.

    Args:
            value: JsonObject
            expected: set[str]
            label: str

    Returns:
        None: No value.
    """

    if set(value) != expected:
        raise ProtocolError(f"{label} fields do not match the protocol schema")


def _integer(value: JsonValue, label: str) -> int:
    """Validate and normalize an integer field from protocol data.

    Args:
            value: JsonValue
            label: str

    Returns:
        int: Result of the operation.
    """

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProtocolError(f"{label} must be an unsigned integer")
    return value


def _optional_integer(value: JsonValue, label: str) -> int | None:
    """Optionally validate an integer field from protocol data.

    Args:
            value: JsonValue
            label: str

    Returns:
        int | None: Result of the operation.
    """

    return None if value is None else _integer(value, label)


def _string(value: JsonValue, label: str) -> str:
    """Validate and normalize a required string field from protocol data.

    Args:
            value: JsonValue
            label: str

    Returns:
        str: Result of the operation.
    """

    if not isinstance(value, str):
        raise ProtocolError(f"{label} must be a string")
    return value


def _optional_string(value: JsonValue, label: str) -> str | None:
    """Optionally validate a string field from protocol data.

    Args:
            value: JsonValue
            label: str

    Returns:
        str | None: Result of the operation.
    """

    return None if value is None else _string(value, label)


def _task_status(value: JsonValue) -> str:
    """Validate and normalize a task status value from protocol data.

    Args:
            value: JsonValue

    Returns:
        str: Result of the operation.
    """

    status = _string(value, "status")
    if status not in {"queued", "leased", "completed"}:
        raise ProtocolError(f"unknown task status {status!r}")
    return status


def _normalize_json_value(value: object) -> JsonValue:
    """Normalize untyped stdlib JSON output into the SDK's explicit JSON value type."""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ProtocolError("response frame contains a non-finite JSON number")
        return value
    if isinstance(value, list):
        return [_normalize_json_value(item) for item in value]
    if isinstance(value, dict):
        normalized: JsonObject = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ProtocolError("response JSON object keys must be strings")
            normalized[key] = _normalize_json_value(item)
        return normalized
    raise ProtocolError(
        f"response frame contains unsupported JSON value {type(value).__name__}"
    )

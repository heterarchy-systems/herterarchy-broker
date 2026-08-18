from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import TypedDict, cast

from .errors import OperationsError, OperationsErrorCode, ProtocolError
from .models import ConsumerGroupDescription, ConsumerGroupPage, ConsumerGroupSummary

OPERATIONS_SCHEMA_VERSION = 1
MAX_GROUP_PAGE_SIZE = 8
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


class _DescribeGroupRequest(TypedDict):
    schema_version: int
    operation: str
    group_id: str


class _ListGroupsRequest(TypedDict, total=False):
    schema_version: int
    operation: str
    limit: int
    after_group_id: str


def validate_group_id(group_id: str) -> str:
    """Validate a Consumer Group identifier against the Broker domain contract."""

    if not isinstance(group_id, str) or not _IDENTIFIER_RE.fullmatch(group_id):
        raise ValueError(
            "group_id must be 1..=128 ASCII bytes, start alphanumeric, and then use only alphanumeric/_/./:/-"
        )
    return group_id


def encode_describe_group(group_id: str) -> bytes:
    """Encode one read-only describe-group operations request."""

    request: _DescribeGroupRequest = {
        "schema_version": OPERATIONS_SCHEMA_VERSION,
        "operation": "describe_group",
        "group_id": validate_group_id(group_id),
    }
    return _serialize(request)


def encode_list_groups(limit: int, after_group_id: str | None) -> bytes:
    """Encode one bounded list-groups operations request."""

    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 1 <= limit <= MAX_GROUP_PAGE_SIZE
    ):
        raise ValueError(f"limit must be in 1..={MAX_GROUP_PAGE_SIZE}")
    request: _ListGroupsRequest = {
        "schema_version": OPERATIONS_SCHEMA_VERSION,
        "operation": "list_groups",
        "limit": limit,
    }
    if after_group_id is not None:
        request["after_group_id"] = validate_group_id(after_group_id)
    return _serialize(request)


def decode_describe_group(frame: bytes) -> ConsumerGroupDescription:
    """Decode a strict describe-group operations response."""

    value = _decode_frame(frame)
    _raise_operations_error(value)
    _require_fields(
        value,
        {
            "schema_version",
            "operation",
            "status",
            "broker_term",
            "broker_revision",
            "group",
        },
    )
    if (
        value.get("schema_version") != OPERATIONS_SCHEMA_VERSION
        or value.get("operation") != "describe_group"
    ):
        raise ProtocolError("operations describe_group schema/operation mismatch")
    if value.get("status") != "ok":
        raise ProtocolError("operations describe_group status must be ok")
    group_value = value.get("group")
    if not isinstance(group_value, dict):
        raise ProtocolError("operations describe_group group must be an object")
    return ConsumerGroupDescription(
        broker_term=_positive_int(value.get("broker_term"), "broker_term"),
        broker_revision=_non_negative_int(
            value.get("broker_revision"), "broker_revision"
        ),
        group=_decode_group_summary(group_value),
    )


def decode_list_groups(frame: bytes) -> ConsumerGroupPage:
    """Decode a strict bounded list-groups operations response."""

    value = _decode_frame(frame)
    _raise_operations_error(value)
    _require_fields(
        value,
        {
            "schema_version",
            "operation",
            "status",
            "broker_term",
            "broker_revision",
            "groups",
            "next_after_group_id",
        },
    )
    if (
        value.get("schema_version") != OPERATIONS_SCHEMA_VERSION
        or value.get("operation") != "list_groups"
    ):
        raise ProtocolError("operations list_groups schema/operation mismatch")
    if value.get("status") != "ok":
        raise ProtocolError("operations list_groups status must be ok")
    groups_value = value.get("groups")
    if not isinstance(groups_value, list):
        raise ProtocolError("operations list_groups groups must be an array")
    if len(groups_value) > MAX_GROUP_PAGE_SIZE:
        raise ProtocolError("operations list_groups exceeded page-size bound")
    groups: list[ConsumerGroupSummary] = []
    for item in groups_value:
        if not isinstance(item, dict):
            raise ProtocolError("operations list_groups group entry must be an object")
        groups.append(_decode_group_summary(item))
    next_after = value.get("next_after_group_id")
    if next_after is not None:
        if not isinstance(next_after, str):
            raise ProtocolError("operations next_after_group_id must be string or null")
        validate_group_id(next_after)
    return ConsumerGroupPage(
        broker_term=_positive_int(value.get("broker_term"), "broker_term"),
        broker_revision=_non_negative_int(
            value.get("broker_revision"), "broker_revision"
        ),
        groups=tuple(groups),
        next_after_group_id=next_after,
    )


def _serialize(request: Mapping[str, object]) -> bytes:
    try:
        return (
            json.dumps(
                request,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            ).encode("utf-8")
            + b"\n"
        )
    except (TypeError, ValueError) as error:
        raise ProtocolError(
            f"operations request is not JSON serializable: {error}"
        ) from error


def _decode_frame(frame: bytes) -> dict[str, object]:
    if not frame.endswith(b"\n"):
        raise ProtocolError("operations response lacked newline terminator")
    try:
        value: object = json.loads(frame)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError(f"invalid operations JSON: {error}") from error
    if not isinstance(value, dict):
        raise ProtocolError("operations response must be a JSON object")
    return cast(dict[str, object], value)


def _raise_operations_error(value: Mapping[str, object]) -> None:
    if value.get("status") != "error":
        return
    _require_fields(value, {"schema_version", "status", "code"})
    if value.get("schema_version") != OPERATIONS_SCHEMA_VERSION:
        raise ProtocolError("operations error schema_version mismatch")
    code = value.get("code")
    if not isinstance(code, str):
        raise ProtocolError("operations error code must be a string")
    try:
        parsed = OperationsErrorCode(code)
    except ValueError as error:
        raise ProtocolError(f"unknown operations error code: {code}") from error
    raise OperationsError(parsed)


def _decode_group_summary(value: Mapping[str, object]) -> ConsumerGroupSummary:
    _require_fields(
        value,
        {"group_id", "namespace_id", "generation", "group_revision", "consumer_count"},
    )
    group_id = value.get("group_id")
    namespace_id = value.get("namespace_id")
    if not isinstance(group_id, str):
        raise ProtocolError("group_id must be a string")
    if not isinstance(namespace_id, str):
        raise ProtocolError("namespace_id must be a string")
    validate_group_id(group_id)
    if not _IDENTIFIER_RE.fullmatch(namespace_id):
        raise ProtocolError("namespace_id violated identifier contract")
    return ConsumerGroupSummary(
        group_id=group_id,
        namespace_id=namespace_id,
        generation=_positive_int(value.get("generation"), "generation"),
        group_revision=_positive_int(value.get("group_revision"), "group_revision"),
        consumer_count=_non_negative_int(value.get("consumer_count"), "consumer_count"),
    )


def _require_fields(value: Mapping[str, object], expected: set[str]) -> None:
    if set(value) != expected:
        raise ProtocolError(
            f"operations response fields mismatch: expected {sorted(expected)}"
        )


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ProtocolError(f"operations {label} must be a positive integer")
    return value


def _non_negative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProtocolError(f"operations {label} must be a non-negative integer")
    return value

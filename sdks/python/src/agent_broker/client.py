from __future__ import annotations

import socket
from collections.abc import Callable
from itertools import count
from typing import Self, TypeVar

from .errors import BrokerError, ErrorDisposition, ProtocolError, TransportError
from .models import (
    BrokerClientConfig,
    BrokerResult,
    CommandIdentity,
    ConsumerGroupResult,
    HealthResult,
    HeartbeatResult,
    NamespaceResult,
    OwnerAcquisitionResult,
    RetryPolicy,
    TaskClaimResult,
    TaskCompletedResult,
    TaskLeaseRenewedResult,
    TaskPublishedResult,
)
from .protocol import (
    JsonObject,
    Operation,
    decode_health_response,
    decode_mutation_response,
    decode_owner_acquire_response,
    encode_health,
    encode_owner_acquire,
    encode_owner_mutation,
    expect_mutation_result_type,
)

_T = TypeVar("_T")
_DEFAULT_RETRY_POLICY = RetryPolicy()


class BrokerClient:
    """Synchronous reusable Broker client for protocol v1/v3 requests.

    Args:
            None

    Returns:
        None: No value.
    """

    def __init__(self, config: BrokerClientConfig | None = None) -> None:
        """Initialize and validate the component state.

        Args:
                config: BrokerClientConfig | None

        Returns:
            None: No value.
        """

        self.config = config or BrokerClientConfig()
        self._socket: socket.socket | None = None
        self._reader = None
        self._request_ids = count(1)

    def __enter__(self) -> Self:
        """Enter the synchronous client context.

        Args:
                None

        Returns:
            BrokerClient: Result of the operation.
        """

        return self

    def __exit__(self, *_exc: object) -> None:
        """Exit the synchronous client context.

        Args:
                *_exc: object

        Returns:
            None: No value.
        """

        self.close()

    def close(self) -> None:
        """Close all client-owned resources.

        Args:
                None

        Returns:
            None: No value.
        """

        reader, sock = self._reader, self._socket
        self._reader = None
        self._socket = None
        if reader is not None:
            try:
                reader.close()
            except OSError:
                pass
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def health(self, *, request_id: str | None = None) -> HealthResult:
        """Check broker health and return the health metadata response.

        Args:
                request_id: str | None

        Returns:
            HealthResult: Result of the operation.
        """

        request_id = request_id or self._next_request_id()
        frame = encode_health(request_id)
        response = self._round_trip(frame)
        return decode_health_response(response, request_id)

    def acquire_owner(
        self,
        *,
        session_id: str,
        expected_owner_epoch: int,
        owner_instance_id: str,
        request_id: str | None = None,
        retry: RetryPolicy = _DEFAULT_RETRY_POLICY,
    ) -> OwnerAcquisitionResult:
        """Acquire or refresh ownership for a session and return the ownership result.

        Args:
                session_id: str
                expected_owner_epoch: int
                owner_instance_id: str
                request_id: str | None
                retry: RetryPolicy

        Returns:
            OwnerAcquisitionResult: Result of the operation.
        """

        request_id = request_id or self._next_request_id()
        frame = encode_owner_acquire(
            request_id, session_id, expected_owner_epoch, owner_instance_id
        )
        return self._retry_exact(
            frame,
            retry,
            lambda response: decode_owner_acquire_response(response, request_id),
        )

    def ensure_namespace(
        self,
        identity: CommandIdentity,
        *,
        namespace_id: str,
        request_id: str | None = None,
        retry: RetryPolicy = _DEFAULT_RETRY_POLICY,
    ) -> NamespaceResult:
        """Ensure a namespace exists and return the resulting namespace data.

        Args:
                identity: CommandIdentity
                namespace_id: str
                request_id: str | None
                retry: RetryPolicy

        Returns:
            NamespaceResult: Result of the operation.
        """

        return expect_mutation_result_type(
            self._mutate(
                Operation.ENSURE_NAMESPACE,
                {"namespace_id": namespace_id},
                identity,
                request_id,
                retry,
            ),
            NamespaceResult,
            Operation.ENSURE_NAMESPACE,
        )

    def publish_task(
        self,
        identity: CommandIdentity,
        *,
        namespace_id: str,
        task_id: str,
        objective: str,
        request_id: str | None = None,
        retry: RetryPolicy = _DEFAULT_RETRY_POLICY,
    ) -> TaskPublishedResult:
        """Publish a task under the given namespace and return the mutation result.

        Args:
                identity: CommandIdentity
                namespace_id: str
                task_id: str
                objective: str
                request_id: str | None
                retry: RetryPolicy

        Returns:
            TaskPublishedResult: Result of the operation.
        """

        return expect_mutation_result_type(
            self._mutate(
                Operation.PUBLISH_TASK,
                {
                    "namespace_id": namespace_id,
                    "task_id": task_id,
                    "objective": objective,
                },
                identity,
                request_id,
                retry,
            ),
            TaskPublishedResult,
            Operation.PUBLISH_TASK,
        )

    def ensure_group(
        self,
        identity: CommandIdentity,
        *,
        namespace_id: str,
        group_id: str,
        request_id: str | None = None,
        retry: RetryPolicy = _DEFAULT_RETRY_POLICY,
    ) -> ConsumerGroupResult:
        """Ensure a consumer group exists for the namespace and return the result.

        Args:
                identity: CommandIdentity
                namespace_id: str
                group_id: str
                request_id: str | None
                retry: RetryPolicy

        Returns:
            ConsumerGroupResult: Result of the operation.
        """

        return expect_mutation_result_type(
            self._mutate(
                Operation.ENSURE_GROUP,
                {"namespace_id": namespace_id, "group_id": group_id},
                identity,
                request_id,
                retry,
            ),
            ConsumerGroupResult,
            Operation.ENSURE_GROUP,
        )

    def join_group(
        self,
        identity: CommandIdentity,
        *,
        group_id: str,
        member_id: str,
        capabilities: tuple[str, ...] | list[str],
        request_id: str | None = None,
        retry: RetryPolicy = _DEFAULT_RETRY_POLICY,
    ) -> ConsumerGroupResult:
        """Register or update a group member and return the join result.

        Args:
                identity: CommandIdentity
                group_id: str
                member_id: str
                capabilities: tuple[str, ...] | list[str]
                request_id: str | None
                retry: RetryPolicy

        Returns:
            ConsumerGroupResult: Result of the operation.
        """

        if len(capabilities) > 64:
            raise ValueError("capabilities may contain at most 64 entries")
        return expect_mutation_result_type(
            self._mutate(
                Operation.JOIN_GROUP,
                {
                    "group_id": group_id,
                    "member_id": member_id,
                    "capabilities": tuple(capabilities),
                },
                identity,
                request_id,
                retry,
            ),
            ConsumerGroupResult,
            Operation.JOIN_GROUP,
        )

    def heartbeat(
        self,
        identity: CommandIdentity,
        *,
        group_id: str,
        member_id: str,
        expected_generation: int,
        request_id: str | None = None,
        retry: RetryPolicy = _DEFAULT_RETRY_POLICY,
    ) -> HeartbeatResult:
        """Send a heartbeat for group membership and return heartbeat status.

        Args:
                identity: CommandIdentity
                group_id: str
                member_id: str
                expected_generation: int
                request_id: str | None
                retry: RetryPolicy

        Returns:
            HeartbeatResult: Result of the operation.
        """

        return expect_mutation_result_type(
            self._mutate(
                Operation.HEARTBEAT,
                {
                    "group_id": group_id,
                    "member_id": member_id,
                    "expected_generation": expected_generation,
                },
                identity,
                request_id,
                retry,
            ),
            HeartbeatResult,
            Operation.HEARTBEAT,
        )

    def leave_group(
        self,
        identity: CommandIdentity,
        *,
        group_id: str,
        member_id: str,
        expected_generation: int,
        request_id: str | None = None,
        retry: RetryPolicy = _DEFAULT_RETRY_POLICY,
    ) -> ConsumerGroupResult:
        """Remove a member from a group and advance generation as needed.

        Args:
                identity: CommandIdentity
                group_id: str
                member_id: str
                expected_generation: int
                request_id: str | None
                retry: RetryPolicy

        Returns:
            ConsumerGroupResult: Result of the operation.
        """

        return expect_mutation_result_type(
            self._mutate(
                Operation.LEAVE_GROUP,
                {
                    "group_id": group_id,
                    "member_id": member_id,
                    "expected_generation": expected_generation,
                },
                identity,
                request_id,
                retry,
            ),
            ConsumerGroupResult,
            Operation.LEAVE_GROUP,
        )

    def claim_task(
        self,
        identity: CommandIdentity,
        *,
        group_id: str,
        member_id: str,
        expected_term: int,
        expected_generation: int,
        lease_id: str,
        lease_duration_ms: int,
        request_id: str | None = None,
        retry: RetryPolicy = _DEFAULT_RETRY_POLICY,
    ) -> TaskClaimResult:
        """Attempt to claim a claimable task and return the claimed task details.

        Args:
                identity: CommandIdentity
                group_id: str
                member_id: str
                expected_term: int
                expected_generation: int
                lease_id: str
                lease_duration_ms: int
                request_id: str | None
                retry: RetryPolicy

        Returns:
            TaskClaimResult: Result of the operation.
        """

        payload: JsonObject = {
            "group_id": group_id,
            "member_id": member_id,
            "expected_term": expected_term,
            "expected_generation": expected_generation,
            "lease_id": lease_id,
            "lease_duration_ms": lease_duration_ms,
        }
        return expect_mutation_result_type(
            self._mutate(Operation.CLAIM_TASK, payload, identity, request_id, retry),
            TaskClaimResult,
            Operation.CLAIM_TASK,
        )

    def renew_task(
        self,
        identity: CommandIdentity,
        *,
        task_id: str,
        group_id: str,
        member_id: str,
        expected_term: int,
        expected_generation: int,
        expected_lease_epoch: int,
        lease_id: str,
        lease_duration_ms: int,
        request_id: str | None = None,
        retry: RetryPolicy = _DEFAULT_RETRY_POLICY,
    ) -> TaskLeaseRenewedResult:
        """Renew an active task lease and return the updated lease state.

        Args:
                identity: CommandIdentity
                task_id: str
                group_id: str
                member_id: str
                expected_term: int
                expected_generation: int
                expected_lease_epoch: int
                lease_id: str
                lease_duration_ms: int
                request_id: str | None
                retry: RetryPolicy

        Returns:
            TaskLeaseRenewedResult: Result of the operation.
        """

        payload: JsonObject = {
            "task_id": task_id,
            "group_id": group_id,
            "member_id": member_id,
            "expected_term": expected_term,
            "expected_generation": expected_generation,
            "expected_lease_epoch": expected_lease_epoch,
            "lease_id": lease_id,
            "lease_duration_ms": lease_duration_ms,
        }
        return expect_mutation_result_type(
            self._mutate(Operation.RENEW_TASK, payload, identity, request_id, retry),
            TaskLeaseRenewedResult,
            Operation.RENEW_TASK,
        )

    def complete_task(
        self,
        identity: CommandIdentity,
        *,
        task_id: str,
        group_id: str,
        member_id: str,
        expected_term: int,
        expected_generation: int,
        expected_lease_epoch: int,
        lease_id: str,
        result: str,
        request_id: str | None = None,
        retry: RetryPolicy = _DEFAULT_RETRY_POLICY,
    ) -> TaskCompletedResult:
        """Complete a claimed task and persist the task completion result.

        Args:
                identity: CommandIdentity
                task_id: str
                group_id: str
                member_id: str
                expected_term: int
                expected_generation: int
                expected_lease_epoch: int
                lease_id: str
                result: str
                request_id: str | None
                retry: RetryPolicy

        Returns:
            TaskCompletedResult: Result of the operation.
        """

        payload: JsonObject = {
            "task_id": task_id,
            "group_id": group_id,
            "member_id": member_id,
            "expected_term": expected_term,
            "expected_generation": expected_generation,
            "expected_lease_epoch": expected_lease_epoch,
            "lease_id": lease_id,
            "result": result,
        }
        return expect_mutation_result_type(
            self._mutate(Operation.COMPLETE_TASK, payload, identity, request_id, retry),
            TaskCompletedResult,
            Operation.COMPLETE_TASK,
        )

    def _mutate(
        self,
        operation: Operation,
        payload: JsonObject,
        identity: CommandIdentity,
        request_id: str | None,
        retry: RetryPolicy,
    ) -> BrokerResult:
        """Build, send, and decode a typed mutation operation.

        Args:
                operation: Operation
                payload: JsonObject
                identity: CommandIdentity
                request_id: str | None
                retry: RetryPolicy

        Returns:
            BrokerResult: Result of the operation.
        """

        request_id = request_id or self._next_request_id()
        frame = encode_owner_mutation(
            request_id,
            operation,
            payload,
            session_id=identity.session_id,
            owner_epoch=identity.owner_epoch,
            owner_instance_id=identity.owner_instance_id,
            sequence=identity.sequence,
        )
        return self._retry_exact(
            frame,
            retry,
            lambda response: decode_mutation_response(response, request_id, operation),
        )

    def _retry_exact(
        self,
        frame: bytes,
        policy: RetryPolicy,
        decode: Callable[[bytes], _T],
    ) -> _T:
        """Retry the framed operation while preserving exact request identity.

        Args:
                frame: bytes
                policy: RetryPolicy
                decode: Callable[[bytes], _T]

        Returns:
            _T: Decoded operation result.
        """

        last_error: BrokerError | TransportError | None = None
        for attempt in range(1, policy.max_attempts + 1):
            try:
                response = self._round_trip(frame)
                return decode(response)
            except BrokerError as error:
                last_error = error
                if (
                    error.disposition is not ErrorDisposition.UNKNOWN
                    or attempt == policy.max_attempts
                ):
                    raise
                self.close()
            except TransportError as error:
                last_error = error
                if attempt == policy.max_attempts:
                    raise
                self.close()
        if last_error is None:
            raise TransportError("retry policy completed without an attempt result")
        raise last_error

    def _round_trip(self, frame: bytes) -> bytes:
        """Run one bounded request/response round-trip over the broker socket.

        Args:
                frame: bytes

        Returns:
            bytes: Result of the operation.
        """

        try:
            self._ensure_connection()
            if self._socket is None or self._reader is None:
                raise TransportError("Broker connection was not initialized")
            self._socket.sendall(frame)
            response = self._reader.readline(self.config.max_response_frame_bytes + 1)
            if not response:
                raise TransportError(
                    "Broker closed the connection before a complete response"
                )
            if len(response) > self.config.max_response_frame_bytes:
                raise ProtocolError(
                    f"protocol frame is at least {len(response)} bytes; maximum is {self.config.max_response_frame_bytes}"
                )
            if not response.endswith(b"\n"):
                raise ProtocolError(
                    "Broker response exceeded the bounded frame or lacked a newline terminator"
                )
            return response
        except (ProtocolError, TransportError):
            self.close()
            raise
        except OSError as error:
            self.close()
            raise TransportError(str(error)) from error

    def _ensure_connection(self) -> None:
        """Establish a healthy client connection and cache it for reuse.

        Args:
                None

        Returns:
            None: No value.
        """

        if self._socket is not None:
            return
        try:
            sock = socket.create_connection(
                (self.config.host, self.config.port),
                timeout=self.config.timeout_seconds,
            )
            sock.settimeout(self.config.timeout_seconds)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._socket = sock
            self._reader = sock.makefile("rb")
        except OSError as error:
            self.close()
            raise TransportError(str(error)) from error

    def _next_request_id(self) -> str:
        """Generate the next deterministic request id for this client.

        Args:
                None

        Returns:
            str: Result of the operation.
        """

        return f"py-client-{next(self._request_ids)}"

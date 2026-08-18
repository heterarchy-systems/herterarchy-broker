from __future__ import annotations

import asyncio
import hashlib
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


class AsyncBrokerClient:
    """Native-asyncio Broker client for protocol v1/v3 requests.

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
        self._request_ids = count(1)

    async def __aenter__(self) -> Self:
        """Enter the asynchronous client context.

        Args:
                None

        Returns:
            AsyncBrokerClient: Result of the operation.
        """

        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Exit the asynchronous client context.

        Args:
                *_exc: object

        Returns:
            None: No value.
        """

        await self.aclose()

    async def aclose(self) -> None:
        """Close client-owned resources.

        Args:
            None

        Returns:
            None: Computed result.
        """

    async def health(self, *, request_id: str | None = None) -> HealthResult:
        """Check broker health and return the health metadata response.

        Args:
                request_id: str | None

        Returns:
            HealthResult: Result of the operation.
        """

        request_id = request_id or self._next_request_id()
        response = await self._round_trip(encode_health(request_id))
        return decode_health_response(response, request_id)

    async def acquire_owner(
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

        request_id = request_id or _owner_request_id(
            session_id,
            expected_owner_epoch,
            owner_instance_id,
        )
        frame = encode_owner_acquire(
            request_id,
            session_id,
            expected_owner_epoch,
            owner_instance_id,
        )
        return await self._retry_exact(
            frame,
            retry,
            lambda response: decode_owner_acquire_response(response, request_id),
        )

    async def ensure_namespace(
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
            await self._mutate(
                Operation.ENSURE_NAMESPACE,
                {"namespace_id": namespace_id},
                identity,
                request_id,
                retry,
            ),
            NamespaceResult,
            Operation.ENSURE_NAMESPACE,
        )

    async def publish_task(
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
            await self._mutate(
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

    async def ensure_group(
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
            await self._mutate(
                Operation.ENSURE_GROUP,
                {"namespace_id": namespace_id, "group_id": group_id},
                identity,
                request_id,
                retry,
            ),
            ConsumerGroupResult,
            Operation.ENSURE_GROUP,
        )

    async def join_group(
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
            await self._mutate(
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

    async def heartbeat(
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
            await self._mutate(
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

    async def leave_group(
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
            await self._mutate(
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

    async def claim_task(
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

        return expect_mutation_result_type(
            await self._mutate(
                Operation.CLAIM_TASK,
                {
                    "group_id": group_id,
                    "member_id": member_id,
                    "expected_term": expected_term,
                    "expected_generation": expected_generation,
                    "lease_id": lease_id,
                    "lease_duration_ms": lease_duration_ms,
                },
                identity,
                request_id,
                retry,
            ),
            TaskClaimResult,
            Operation.CLAIM_TASK,
        )

    async def renew_task(
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

        return expect_mutation_result_type(
            await self._mutate(
                Operation.RENEW_TASK,
                {
                    "task_id": task_id,
                    "group_id": group_id,
                    "member_id": member_id,
                    "expected_term": expected_term,
                    "expected_generation": expected_generation,
                    "expected_lease_epoch": expected_lease_epoch,
                    "lease_id": lease_id,
                    "lease_duration_ms": lease_duration_ms,
                },
                identity,
                request_id,
                retry,
            ),
            TaskLeaseRenewedResult,
            Operation.RENEW_TASK,
        )

    async def complete_task(
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

        return expect_mutation_result_type(
            await self._mutate(
                Operation.COMPLETE_TASK,
                {
                    "task_id": task_id,
                    "group_id": group_id,
                    "member_id": member_id,
                    "expected_term": expected_term,
                    "expected_generation": expected_generation,
                    "expected_lease_epoch": expected_lease_epoch,
                    "lease_id": lease_id,
                    "result": result,
                },
                identity,
                request_id,
                retry,
            ),
            TaskCompletedResult,
            Operation.COMPLETE_TASK,
        )

    async def _mutate(
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

        request_id = request_id or _owned_request_id(identity)
        frame = encode_owner_mutation(
            request_id,
            operation,
            payload,
            session_id=identity.session_id,
            owner_epoch=identity.owner_epoch,
            owner_instance_id=identity.owner_instance_id,
            sequence=identity.sequence,
        )
        return await self._retry_exact(
            frame,
            retry,
            lambda response: decode_mutation_response(response, request_id, operation),
        )

    async def _retry_exact(
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
            _T: Result of the operation.
        """

        last_error: BrokerError | TransportError | None = None
        for attempt in range(1, policy.max_attempts + 1):
            try:
                return decode(await self._round_trip(frame))
            except BrokerError as error:
                last_error = error
                if (
                    error.disposition is not ErrorDisposition.UNKNOWN
                    or attempt == policy.max_attempts
                ):
                    raise
            except TransportError as error:
                last_error = error
                if attempt == policy.max_attempts:
                    raise
        if last_error is None:
            raise TransportError("retry policy completed without an attempt result")
        raise last_error

    async def _round_trip(self, frame: bytes) -> bytes:
        """Run one bounded request/response round-trip over the broker socket.

        Args:
                frame: bytes

        Returns:
            bytes: Result of the operation.
        """

        return await _round_trip_frame(
            host=self.config.host,
            port=self.config.port,
            timeout_seconds=float(self.config.timeout_seconds),
            max_response_frame_bytes=self.config.max_response_frame_bytes,
            frame=frame,
        )

    def _next_request_id(self) -> str:
        """Generate the next deterministic request id for this client.

        Args:
                None

        Returns:
            str: Result of the operation.
        """

        return f"py-async-client-{next(self._request_ids)}"


async def _round_trip_frame(
    *,
    host: str,
    port: int,
    timeout_seconds: float,
    max_response_frame_bytes: int,
    frame: bytes,
) -> bytes:
    """Perform one bounded async request/response round-trip.

    Args:
            host: str
            port: int
            timeout_seconds: float
            max_response_frame_bytes: int
            frame: bytes

    Returns:
        bytes: Result of the operation.
    """

    writer: asyncio.StreamWriter | None = None
    deadline: float | None = None
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        async with asyncio.timeout_at(deadline):
            reader, writer = await asyncio.open_connection(
                host,
                port,
                limit=max_response_frame_bytes + 1,
            )
            writer.write(frame)
            await writer.drain()
            try:
                response = await reader.readuntil(b"\n")
            except asyncio.IncompleteReadError as error:
                raise TransportError(
                    "Broker closed the connection before a complete response"
                ) from error
            except asyncio.LimitOverrunError as error:
                raise ProtocolError(
                    "Broker response exceeded the configured bounded frame"
                ) from error
            if len(response) > max_response_frame_bytes:
                raise ProtocolError(
                    "protocol frame is at least "
                    f"{len(response)} bytes; maximum is {max_response_frame_bytes}"
                )
            return response
    except TimeoutError as error:
        raise TransportError(
            "Broker async round trip exceeded its end-to-end deadline"
        ) from error
    except (ProtocolError, TransportError):
        raise
    except OSError as error:
        raise TransportError(str(error)) from error
    finally:
        if writer is not None:
            writer.close()
            task = asyncio.current_task()
            if task is None or task.cancelling() == 0:
                try:
                    loop = asyncio.get_running_loop()
                    remaining = (
                        0.0 if deadline is None else max(0.0, deadline - loop.time())
                    )
                    if remaining > 0.0:
                        async with asyncio.timeout(remaining):
                            await writer.wait_closed()
                except (TimeoutError, OSError):
                    pass


def _owner_request_id(
    session_id: str,
    expected_owner_epoch: int,
    owner_instance_id: str,
) -> str:
    """Build a deterministic owner-acquisition request identifier.

    Args:
            session_id: str
            expected_owner_epoch: int
            owner_instance_id: str

    Returns:
        str: Result of the operation.
    """

    material = f"{session_id}\x1f{expected_owner_epoch}\x1f{owner_instance_id}".encode()
    digest = hashlib.sha256(material).hexdigest()[:32]
    return f"py-async-owner-{digest}"


def _owned_request_id(identity: CommandIdentity) -> str:
    """Build a deterministic owner-mutation request identifier.

    Args:
            identity: CommandIdentity

    Returns:
        str: Result of the operation.
    """

    material = (
        f"{identity.session_id}\x1f{identity.owner_epoch}\x1f"
        f"{identity.owner_instance_id}\x1f{identity.sequence}"
    ).encode()
    digest = hashlib.sha256(material).hexdigest()[:32]
    return f"py-async-owned-{digest}"

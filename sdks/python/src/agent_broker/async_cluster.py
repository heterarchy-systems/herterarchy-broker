from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Self, TypeVar

from .async_client import (
    AsyncBrokerClient,
    _owned_request_id,
    _owner_request_id,
    _round_trip_frame,
)
from .cluster import _OPERATIONS_READINESS_REQUEST, validate_readiness
from .errors import (
    BrokerError,
    ClusterRoutingError,
    ErrorDisposition,
    InvalidOperationsResponse,
    MultipleWriteReadyLeaders,
    NoWriteReadyLeader,
    ProtocolError,
    TransportError,
)
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
    StaticClusterConfig,
    StaticClusterNode,
    TaskClaimResult,
    TaskCompletedResult,
    TaskLeaseRenewedResult,
    TaskPublishedResult,
)
from .protocol import (
    JsonObject,
    Operation,
    decode_mutation_response,
    decode_owner_acquire_response,
    encode_owner_acquire,
    encode_owner_mutation,
    expect_mutation_result_type,
)

_T = TypeVar("_T")


class AsyncStaticClusterBrokerClient:
    """Async fail-closed client for a fixed three-node cluster topology.

    Args:
            None

    Returns:
        None: No value.
    """

    def __init__(self, config: StaticClusterConfig) -> None:
        """Initialize and validate the component state.

        Args:
                config: StaticClusterConfig

        Returns:
            None: No value.
        """

        self.config = config

    async def __aenter__(self) -> Self:
        """Enter the asynchronous client context.

        Args:
                None

        Returns:
            AsyncStaticClusterBrokerClient: Result of the operation.
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
        """Close client-owned resources; operations own their transports.

        Args:
            None

        Returns:
            None: Computed result.
        """

    async def discover_write_leader(self) -> StaticClusterNode:
        """Discover exactly one verified write-ready leader node.

        Args:
                None

        Returns:
            StaticClusterNode: Result of the operation.
        """

        tasks: list[asyncio.Task[tuple[bool, ClusterRoutingError | None]]] = []
        async with asyncio.TaskGroup() as group:
            for node in self.config.nodes:
                tasks.append(group.create_task(self._probe_readiness(node)))

        ready: list[StaticClusterNode] = []
        for node, task in zip(self.config.nodes, tasks, strict=True):
            is_ready, error = task.result()
            if error is not None:
                raise error
            if is_ready:
                ready.append(node)
        if not ready:
            raise NoWriteReadyLeader("no write-ready leader was verified")
        if len(ready) != 1:
            raise MultipleWriteReadyLeaders(tuple(node.node_id for node in ready))
        return ready[0]

    async def health(self, *, request_id: str | None = None) -> HealthResult:
        """Check broker health and return the health metadata response.

        Args:
                request_id: str | None

        Returns:
            HealthResult: Result of the operation.
        """

        leader = await self.discover_write_leader()
        client = self._direct_client(leader)
        return await client.health(request_id=request_id)

    async def acquire_owner(
        self,
        *,
        session_id: str,
        expected_owner_epoch: int,
        owner_instance_id: str,
        request_id: str | None = None,
        retry: RetryPolicy | None = None,
    ) -> OwnerAcquisitionResult:
        """Acquire or refresh ownership for a session and return the ownership result.

        Args:
                session_id: str
                expected_owner_epoch: int
                owner_instance_id: str
                request_id: str | None
                retry: RetryPolicy | None

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
        return await self._route_exact(
            frame,
            retry or self.config.retry,
            lambda response: decode_owner_acquire_response(response, request_id),
        )

    async def ensure_namespace(
        self,
        identity: CommandIdentity,
        *,
        namespace_id: str,
        request_id: str | None = None,
        retry: RetryPolicy | None = None,
    ) -> NamespaceResult:
        """Ensure a namespace exists and return the resulting namespace data.

        Args:
                identity: CommandIdentity
                namespace_id: str
                request_id: str | None
                retry: RetryPolicy | None

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
        retry: RetryPolicy | None = None,
    ) -> TaskPublishedResult:
        """Publish a task under the given namespace and return the mutation result.

        Args:
                identity: CommandIdentity
                namespace_id: str
                task_id: str
                objective: str
                request_id: str | None
                retry: RetryPolicy | None

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
        retry: RetryPolicy | None = None,
    ) -> ConsumerGroupResult:
        """Ensure a consumer group exists for the namespace and return the result.

        Args:
                identity: CommandIdentity
                namespace_id: str
                group_id: str
                request_id: str | None
                retry: RetryPolicy | None

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
        retry: RetryPolicy | None = None,
    ) -> ConsumerGroupResult:
        """Register or update a group member and return the join result.

        Args:
                identity: CommandIdentity
                group_id: str
                member_id: str
                capabilities: tuple[str, ...] | list[str]
                request_id: str | None
                retry: RetryPolicy | None

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
        retry: RetryPolicy | None = None,
    ) -> HeartbeatResult:
        """Send a heartbeat for group membership and return heartbeat status.

        Args:
                identity: CommandIdentity
                group_id: str
                member_id: str
                expected_generation: int
                request_id: str | None
                retry: RetryPolicy | None

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
        retry: RetryPolicy | None = None,
    ) -> ConsumerGroupResult:
        """Remove a member from a group and advance generation as needed.

        Args:
                identity: CommandIdentity
                group_id: str
                member_id: str
                expected_generation: int
                request_id: str | None
                retry: RetryPolicy | None

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
        retry: RetryPolicy | None = None,
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
                retry: RetryPolicy | None

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
        retry: RetryPolicy | None = None,
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
                retry: RetryPolicy | None

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
        retry: RetryPolicy | None = None,
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
                retry: RetryPolicy | None

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
        retry: RetryPolicy | None,
    ) -> BrokerResult:
        """Build, send, and decode a typed mutation operation.

        Args:
                operation: Operation
                payload: JsonObject
                identity: CommandIdentity
                request_id: str | None
                retry: RetryPolicy | None

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
        return await self._route_exact(
            frame,
            retry or self.config.retry,
            lambda response: decode_mutation_response(response, request_id, operation),
        )

    async def _route_exact(
        self,
        frame: bytes,
        retry: RetryPolicy,
        decode: Callable[[bytes], _T],
    ) -> _T:
        """Route a request to a single verified leader and decode its reply.

        Args:
                frame: bytes
                retry: RetryPolicy
                decode: Callable[[bytes], _T]

        Returns:
            _T: Result of the operation.
        """

        last_error: BrokerError | TransportError | None = None
        for attempt in range(1, retry.max_attempts + 1):
            leader = await self.discover_write_leader()
            client = self._direct_client(leader)
            try:
                return decode(await client._round_trip(frame))
            except BrokerError as error:
                last_error = error
                if (
                    error.disposition is not ErrorDisposition.UNKNOWN
                    or attempt == retry.max_attempts
                ):
                    raise
            except TransportError as error:
                last_error = error
                if attempt == retry.max_attempts:
                    raise
        if last_error is None:
            raise ClusterRoutingError(
                "cluster retry policy completed without an attempt result"
            )
        raise last_error

    async def _probe_readiness(
        self,
        node: StaticClusterNode,
    ) -> tuple[bool, ClusterRoutingError | None]:
        """Probe a cluster node readiness channel and classify its identity.

        Args:
                node: StaticClusterNode

        Returns:
            tuple[bool, ClusterRoutingError | None]: Result of the operation.
        """

        try:
            response = await _round_trip_frame(
                host=node.host,
                port=node.operations_port,
                timeout_seconds=float(self.config.timeout_seconds),
                max_response_frame_bytes=self.config.max_response_frame_bytes,
                frame=_OPERATIONS_READINESS_REQUEST,
            )
        except TransportError:
            return False, None
        except ProtocolError as error:
            return False, InvalidOperationsResponse(node.node_id, str(error))

        try:
            value = json.loads(response)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            return False, InvalidOperationsResponse(
                node.node_id, f"invalid JSON: {error}"
            )
        try:
            return validate_readiness(node, value), None
        except InvalidOperationsResponse as error:
            return False, error

    def _direct_client(self, node: StaticClusterNode) -> AsyncBrokerClient:
        """Create a direct broker client bound to a specific node.

        Args:
                node: StaticClusterNode

        Returns:
            AsyncBrokerClient: Result of the operation.
        """

        return AsyncBrokerClient(
            BrokerClientConfig(
                host=node.host,
                port=node.broker_port,
                timeout_seconds=self.config.timeout_seconds,
                max_response_frame_bytes=self.config.max_response_frame_bytes,
            )
        )


AsyncClusterBrokerClient = AsyncStaticClusterBrokerClient

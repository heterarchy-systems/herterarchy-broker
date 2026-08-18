from __future__ import annotations

from typing import Self

from .async_client import AsyncBrokerClient
from .models import (
    BrokerClientConfig,
    BrokerResult,
    ConsumerGroupResult,
    HealthResult,
    HeartbeatResult,
    NamespaceResult,
    TaskClaimResult,
    TaskCompletedResult,
    TaskLeaseRenewedResult,
    TaskPublishedResult,
)
from .protocol import (
    JsonObject,
    Operation,
    decode_mutation_response_v1,
    encode_mutation_v1,
    expect_mutation_result_type,
)


class AsyncStandaloneBrokerClient:
    """Async client wrapper for protocol-v1 standalone mutation operations.

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

        self._transport = AsyncBrokerClient(config)

    async def __aenter__(self) -> Self:
        """Enter the asynchronous client context.

        Args:
                None

        Returns:
            AsyncStandaloneBrokerClient: Result of the operation.
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
        """Close all client-owned asynchronous resources.

        Args:
                None

        Returns:
            None: No value.
        """

        await self._transport.aclose()

    async def health(self, *, request_id: str | None = None) -> HealthResult:
        """Check broker health and return the health metadata response.

        Args:
                request_id: str | None

        Returns:
            HealthResult: Result of the operation.
        """

        return await self._transport.health(request_id=request_id)

    async def ensure_namespace(
        self,
        *,
        namespace_id: str,
        request_id: str | None = None,
    ) -> NamespaceResult:
        """Ensure a namespace exists and return the resulting namespace data.

        Args:
                namespace_id: str
                request_id: str | None

        Returns:
            NamespaceResult: Result of the operation.
        """

        return expect_mutation_result_type(
            await self._mutate_once(
                Operation.ENSURE_NAMESPACE, {"namespace_id": namespace_id}, request_id
            ),
            NamespaceResult,
            Operation.ENSURE_NAMESPACE,
        )

    async def publish_task(
        self,
        *,
        namespace_id: str,
        task_id: str,
        objective: str,
        request_id: str | None = None,
    ) -> TaskPublishedResult:
        """Publish a task under the given namespace and return the mutation result.

        Args:
                namespace_id: str
                task_id: str
                objective: str
                request_id: str | None

        Returns:
            TaskPublishedResult: Result of the operation.
        """

        return expect_mutation_result_type(
            await self._mutate_once(
                Operation.PUBLISH_TASK,
                {
                    "namespace_id": namespace_id,
                    "task_id": task_id,
                    "objective": objective,
                },
                request_id,
            ),
            TaskPublishedResult,
            Operation.PUBLISH_TASK,
        )

    async def ensure_group(
        self,
        *,
        namespace_id: str,
        group_id: str,
        request_id: str | None = None,
    ) -> ConsumerGroupResult:
        """Ensure a consumer group exists for the namespace and return the result.

        Args:
                namespace_id: str
                group_id: str
                request_id: str | None

        Returns:
            ConsumerGroupResult: Result of the operation.
        """

        return expect_mutation_result_type(
            await self._mutate_once(
                Operation.ENSURE_GROUP,
                {"namespace_id": namespace_id, "group_id": group_id},
                request_id,
            ),
            ConsumerGroupResult,
            Operation.ENSURE_GROUP,
        )

    async def join_group(
        self,
        *,
        group_id: str,
        member_id: str,
        capabilities: tuple[str, ...] | list[str],
        request_id: str | None = None,
    ) -> ConsumerGroupResult:
        """Register or update a group member and return the join result.

        Args:
                group_id: str
                member_id: str
                capabilities: tuple[str, ...] | list[str]
                request_id: str | None

        Returns:
            ConsumerGroupResult: Result of the operation.
        """

        if len(capabilities) > 64:
            raise ValueError("capabilities may contain at most 64 entries")
        return expect_mutation_result_type(
            await self._mutate_once(
                Operation.JOIN_GROUP,
                {
                    "group_id": group_id,
                    "member_id": member_id,
                    "capabilities": tuple(capabilities),
                },
                request_id,
            ),
            ConsumerGroupResult,
            Operation.JOIN_GROUP,
        )

    async def heartbeat(
        self,
        *,
        group_id: str,
        member_id: str,
        expected_generation: int,
        request_id: str | None = None,
    ) -> HeartbeatResult:
        """Send a heartbeat for group membership and return heartbeat status.

        Args:
                group_id: str
                member_id: str
                expected_generation: int
                request_id: str | None

        Returns:
            HeartbeatResult: Result of the operation.
        """

        return expect_mutation_result_type(
            await self._mutate_once(
                Operation.HEARTBEAT,
                {
                    "group_id": group_id,
                    "member_id": member_id,
                    "expected_generation": expected_generation,
                },
                request_id,
            ),
            HeartbeatResult,
            Operation.HEARTBEAT,
        )

    async def leave_group(
        self,
        *,
        group_id: str,
        member_id: str,
        expected_generation: int,
        request_id: str | None = None,
    ) -> ConsumerGroupResult:
        """Remove a member from a group and advance generation as needed.

        Args:
                group_id: str
                member_id: str
                expected_generation: int
                request_id: str | None

        Returns:
            ConsumerGroupResult: Result of the operation.
        """

        return expect_mutation_result_type(
            await self._mutate_once(
                Operation.LEAVE_GROUP,
                {
                    "group_id": group_id,
                    "member_id": member_id,
                    "expected_generation": expected_generation,
                },
                request_id,
            ),
            ConsumerGroupResult,
            Operation.LEAVE_GROUP,
        )

    async def claim_task(
        self,
        *,
        group_id: str,
        member_id: str,
        expected_term: int,
        expected_generation: int,
        lease_id: str,
        lease_duration_ms: int,
        request_id: str | None = None,
    ) -> TaskClaimResult:
        """Attempt to claim a claimable task and return the claimed task details.

        Args:
                group_id: str
                member_id: str
                expected_term: int
                expected_generation: int
                lease_id: str
                lease_duration_ms: int
                request_id: str | None

        Returns:
            TaskClaimResult: Result of the operation.
        """

        return expect_mutation_result_type(
            await self._mutate_once(
                Operation.CLAIM_TASK,
                {
                    "group_id": group_id,
                    "member_id": member_id,
                    "expected_term": expected_term,
                    "expected_generation": expected_generation,
                    "lease_id": lease_id,
                    "lease_duration_ms": lease_duration_ms,
                },
                request_id,
            ),
            TaskClaimResult,
            Operation.CLAIM_TASK,
        )

    async def renew_task(
        self,
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
    ) -> TaskLeaseRenewedResult:
        """Renew an active task lease and return the updated lease state.

        Args:
                task_id: str
                group_id: str
                member_id: str
                expected_term: int
                expected_generation: int
                expected_lease_epoch: int
                lease_id: str
                lease_duration_ms: int
                request_id: str | None

        Returns:
            TaskLeaseRenewedResult: Result of the operation.
        """

        return expect_mutation_result_type(
            await self._mutate_once(
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
                request_id,
            ),
            TaskLeaseRenewedResult,
            Operation.RENEW_TASK,
        )

    async def complete_task(
        self,
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
    ) -> TaskCompletedResult:
        """Complete a claimed task and persist the task completion result.

        Args:
                task_id: str
                group_id: str
                member_id: str
                expected_term: int
                expected_generation: int
                expected_lease_epoch: int
                lease_id: str
                result: str
                request_id: str | None

        Returns:
            TaskCompletedResult: Result of the operation.
        """

        return expect_mutation_result_type(
            await self._mutate_once(
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
                request_id,
            ),
            TaskCompletedResult,
            Operation.COMPLETE_TASK,
        )

    async def _mutate_once(
        self,
        operation: Operation,
        payload: JsonObject,
        request_id: str | None,
    ) -> BrokerResult:
        """Send one protocol-v1 mutation call and decode its response.

        Args:
                operation: Operation
                payload: JsonObject
                request_id: str | None

        Returns:
            BrokerResult: Result of the operation.
        """

        request_id = request_id or self._transport._next_request_id()
        frame = encode_mutation_v1(request_id, operation, payload)
        response = await self._transport._round_trip(frame)
        return decode_mutation_response_v1(response, request_id, operation)

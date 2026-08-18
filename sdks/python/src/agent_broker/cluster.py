from __future__ import annotations

import json
import socket
from collections.abc import Callable
from itertools import count
from typing import TypeVar

from .client import BrokerClient
from .errors import (
    BrokerError,
    ClusterRoutingError,
    ErrorDisposition,
    InvalidOperationsResponse,
    MultipleWriteReadyLeaders,
    NoWriteReadyLeader,
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

_OPERATIONS_READINESS_REQUEST = b'{"schema_version":1,"operation":"readiness"}\n'
_T = TypeVar("_T")


class StaticClusterBrokerClient:
    """Fail-closed client for a fixed three-node cluster topology.

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
        self._request_ids = count(1)

    def discover_write_leader(self) -> StaticClusterNode:
        """Discover exactly one verified write-ready leader node.

        Args:
                None

        Returns:
            StaticClusterNode: Result of the operation.
        """

        ready: list[StaticClusterNode] = []
        for node in self.config.nodes:
            if self._probe_readiness(node):
                ready.append(node)
        if not ready:
            raise NoWriteReadyLeader("no write-ready leader was verified")
        if len(ready) != 1:
            raise MultipleWriteReadyLeaders(tuple(node.node_id for node in ready))
        return ready[0]

    def health(self, *, request_id: str | None = None) -> HealthResult:
        """Check broker health and return the health metadata response.

        Args:
                request_id: str | None

        Returns:
            HealthResult: Result of the operation.
        """

        leader = self.discover_write_leader()
        client = self._direct_client(leader)
        try:
            return client.health(request_id=request_id)
        finally:
            client.close()

    def acquire_owner(
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

        request_id = request_id or self._next_request_id()
        frame = encode_owner_acquire(
            request_id,
            session_id,
            expected_owner_epoch,
            owner_instance_id,
        )
        return self._route_exact(
            frame,
            retry or self.config.retry,
            lambda response: decode_owner_acquire_response(response, request_id),
        )

    def ensure_namespace(
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
            self._mutate(
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
            self._mutate(
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
            self._mutate(
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

    def _mutate(
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
        return self._route_exact(
            frame,
            retry or self.config.retry,
            lambda response: decode_mutation_response(response, request_id, operation),
        )

    def _route_exact(
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

        last_error: AgentRouteError | None = None
        for attempt in range(1, retry.max_attempts + 1):
            leader = self.discover_write_leader()
            client = self._direct_client(leader)
            try:
                response = client._round_trip(frame)
                return decode(response)
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
            finally:
                client.close()
        if last_error is None:
            raise ClusterRoutingError(
                "cluster retry policy completed without an attempt result"
            )
        raise last_error

    def _probe_readiness(self, node: StaticClusterNode) -> bool:
        """Probe a cluster node readiness channel and classify its identity.

        Args:
                node: StaticClusterNode

        Returns:
            bool: Result of the operation.
        """

        try:
            with socket.create_connection(
                (node.host, node.operations_port), timeout=self.config.timeout_seconds
            ) as sock:
                sock.settimeout(self.config.timeout_seconds)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.sendall(_OPERATIONS_READINESS_REQUEST)
                with sock.makefile("rb") as reader:
                    response = reader.readline(self.config.max_response_frame_bytes + 1)
        except OSError:
            return False

        if not response:
            return False
        if len(response) > self.config.max_response_frame_bytes:
            raise InvalidOperationsResponse(
                node.node_id, "response frame exceeded configured bound"
            )
        if not response.endswith(b"\n"):
            raise InvalidOperationsResponse(
                node.node_id, "response lacked newline terminator"
            )
        try:
            value: object = json.loads(response)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise InvalidOperationsResponse(
                node.node_id, f"invalid JSON: {error}"
            ) from error
        return validate_readiness(node, value)

    def _direct_client(self, node: StaticClusterNode) -> BrokerClient:
        """Create a direct broker client bound to a specific node.

        Args:
                node: StaticClusterNode

        Returns:
            BrokerClient: Result of the operation.
        """

        return BrokerClient(
            BrokerClientConfig(
                host=node.host,
                port=node.broker_port,
                timeout_seconds=self.config.timeout_seconds,
                max_response_frame_bytes=self.config.max_response_frame_bytes,
            )
        )

    def _next_request_id(self) -> str:
        """Generate the next deterministic request id for this client.

        Args:
                None

        Returns:
            str: Result of the operation.
        """

        return f"py-cluster-{next(self._request_ids)}"


AgentRouteError = BrokerError | TransportError
ClusterBrokerClient = StaticClusterBrokerClient


def validate_readiness(node: StaticClusterNode, value: object) -> bool:
    """Validate one operations-v1 readiness payload against its configured node identity.

    Args:
        node: StaticClusterNode
        value: object

    Returns:
        bool: Computed result.
    """

    if not isinstance(value, dict):
        raise InvalidOperationsResponse(node.node_id, "response must be a JSON object")
    if value.get("schema_version") != 1 or value.get("operation") != "readiness":
        raise InvalidOperationsResponse(
            node.node_id, "schema_version/operation mismatch"
        )
    write_ready = value.get("write_ready")
    if not isinstance(write_ready, bool):
        raise InvalidOperationsResponse(node.node_id, "write_ready must be boolean")
    if not write_ready:
        return False
    if (
        value.get("live") is not True
        or value.get("reason") != "ready"
        or value.get("maintenance_authority") is not True
    ):
        raise InvalidOperationsResponse(
            node.node_id, "write-ready response lacked live/ready/maintenance authority"
        )
    consensus = value.get("consensus")
    if not isinstance(consensus, dict):
        raise InvalidOperationsResponse(node.node_id, "consensus must be an object")
    if consensus.get("status") != "ready" or consensus.get("write_ready") is not True:
        raise InvalidOperationsResponse(
            node.node_id, "write-ready response had non-ready consensus"
        )
    progress = consensus.get("progress")
    if not isinstance(progress, dict):
        raise InvalidOperationsResponse(
            node.node_id, "write-ready consensus lacked progress"
        )
    reported_node = progress.get("node_id")
    current_leader = progress.get("current_leader")
    if isinstance(reported_node, bool) or not isinstance(reported_node, int):
        raise InvalidOperationsResponse(
            node.node_id, "progress.node_id must be unsigned"
        )
    if isinstance(current_leader, bool) or not isinstance(current_leader, int):
        raise InvalidOperationsResponse(
            node.node_id, "progress.current_leader must be unsigned"
        )
    if reported_node < 0 or current_leader < 0:
        raise InvalidOperationsResponse(
            node.node_id, "progress identity values must be unsigned"
        )
    if reported_node != node.node_id or current_leader != node.node_id:
        raise InvalidOperationsResponse(
            node.node_id,
            "write-ready identity mismatch: "
            f"configured={node.node_id}, reported={reported_node}, current_leader={current_leader}",
        )
    return True

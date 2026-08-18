from __future__ import annotations

from dataclasses import dataclass
from ipaddress import ip_address

DEFAULT_MAX_FRAME_BYTES = 128 * 1024
MIN_MAX_FRAME_BYTES = 4 * 1024
MAX_MAX_FRAME_BYTES = 1024 * 1024
DEFAULT_OPERATIONS_MAX_FRAME_BYTES = 16 * 1024
MIN_OPERATIONS_MAX_FRAME_BYTES = 256
MAX_OPERATIONS_MAX_FRAME_BYTES = 16 * 1024


def _positive_int(value: int, label: str) -> None:
    """Validate that a field is a positive integer.

    Args:
            value: int
            label: str

    Returns:
        None: No value.
    """

    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")


def _non_empty(value: str, label: str) -> None:
    """Validate that a field is a non-empty string.

    Args:
            value: str
            label: str

    Returns:
        None: No value.
    """

    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string")


@dataclass(frozen=True, slots=True)
class BrokerClientConfig:
    """Validation and defaults for a broker client transport configuration.

    Args:
            host: str
            port: int
            timeout_seconds: float
            max_response_frame_bytes: int

    Returns:
        None: No value.
    """

    host: str = "127.0.0.1"
    port: int = 8811
    timeout_seconds: float = 5.0
    max_response_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES

    def __post_init__(self) -> None:
        """Validate dataclass fields before object creation completes.

        Args:
            None

        Returns:
            None: No value.
        """

        try:
            address = ip_address(self.host)
        except ValueError as error:
            raise ValueError("host must be a literal loopback IP address") from error
        if not address.is_loopback:
            raise ValueError("host must be a loopback IP address")
        if (
            isinstance(self.port, bool)
            or not isinstance(self.port, int)
            or not 1 <= self.port <= 65535
        ):
            raise ValueError("port must be in 1..=65535")
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or self.timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be positive")
        if (
            not MIN_MAX_FRAME_BYTES
            <= self.max_response_frame_bytes
            <= MAX_MAX_FRAME_BYTES
        ):
            raise ValueError("max_response_frame_bytes must be in 4096..=1048576")


@dataclass(frozen=True, slots=True)
class OperationsClientConfig:
    """Read-only operations transport configuration.

    Args:
        host: Literal loopback IP address.
        port: Read-only operations TCP port.
        timeout_seconds: Connect/read/write timeout.
        max_response_frame_bytes: Maximum accepted response frame size.

    Returns:
        None: No value.
    """

    host: str = "127.0.0.1"
    port: int = 8812
    timeout_seconds: float = 2.0
    max_response_frame_bytes: int = DEFAULT_OPERATIONS_MAX_FRAME_BYTES

    def __post_init__(self) -> None:
        """Validate operations transport bounds."""

        try:
            address = ip_address(self.host)
        except ValueError as error:
            raise ValueError("host must be a literal loopback IP address") from error
        if not address.is_loopback:
            raise ValueError("host must be a loopback IP address")
        if (
            isinstance(self.port, bool)
            or not isinstance(self.port, int)
            or not 1 <= self.port <= 65535
        ):
            raise ValueError("port must be in 1..=65535")
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or self.timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be positive")
        if (
            not MIN_OPERATIONS_MAX_FRAME_BYTES
            <= self.max_response_frame_bytes
            <= MAX_OPERATIONS_MAX_FRAME_BYTES
        ):
            raise ValueError("max_response_frame_bytes must be in 256..=16384")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Configuration for bounded retry attempts on recoverable requests.

    Args:
            max_attempts: int

    Returns:
        None: No value.
    """

    max_attempts: int = 1

    def __post_init__(self) -> None:
        """Validate dataclass fields before object creation completes.

        Args:
            None

        Returns:
            None: No value.
        """

        _positive_int(self.max_attempts, "max_attempts")


@dataclass(frozen=True, slots=True)
class StaticClusterNode:
    """Static cluster node descriptor used for direct TCP routing.

    Args:
            node_id: int
            broker_port: int
            operations_port: int
            host: str

    Returns:
        None: No value.
    """

    node_id: int
    broker_port: int
    operations_port: int
    host: str = "127.0.0.1"

    def __post_init__(self) -> None:
        """Validate dataclass fields before object creation completes.

        Args:
            None

        Returns:
            None: No value.
        """

        _positive_int(self.node_id, "node_id")
        try:
            address = ip_address(self.host)
        except ValueError as error:
            raise ValueError("host must be a literal loopback IP address") from error
        if not address.is_loopback:
            raise ValueError("host must be a loopback IP address")
        for value, label in (
            (self.broker_port, "broker_port"),
            (self.operations_port, "operations_port"),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= 65535
            ):
                raise ValueError(f"{label} must be in 1..=65535")


@dataclass(frozen=True, slots=True)
class StaticClusterConfig:
    """Settings for a fixed three-node cluster client configuration.

    Args:
            nodes: tuple[StaticClusterNode, StaticClusterNode, StaticClusterNode]
            timeout_seconds: float
            max_response_frame_bytes: int
            retry: RetryPolicy

    Returns:
        None: No value.
    """

    nodes: tuple[StaticClusterNode, StaticClusterNode, StaticClusterNode]
    timeout_seconds: float = 2.0
    max_response_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES
    retry: RetryPolicy = RetryPolicy(max_attempts=3)

    def __post_init__(self) -> None:
        """Validate dataclass fields before object creation completes.

        Args:
            None

        Returns:
            None: No value.
        """

        if len(self.nodes) != 3:
            raise ValueError("static cluster requires exactly three nodes")
        if len({node.node_id for node in self.nodes}) != 3:
            raise ValueError("static cluster node_id values must be unique")
        broker_endpoints = {(node.host, node.broker_port) for node in self.nodes}
        operations_endpoints = {
            (node.host, node.operations_port) for node in self.nodes
        }
        if len(broker_endpoints) != 3 or len(operations_endpoints) != 3:
            raise ValueError(
                "static cluster broker and operations endpoints must be unique"
            )
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or self.timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be positive")
        if (
            not MIN_MAX_FRAME_BYTES
            <= self.max_response_frame_bytes
            <= MAX_MAX_FRAME_BYTES
        ):
            raise ValueError("max_response_frame_bytes must be in 4096..=1048576")


@dataclass(frozen=True, slots=True)
class CommandIdentity:
    """Owner identity tuple used for idempotent owner-scoped mutations.

    Args:
            session_id: str
            owner_epoch: int
            owner_instance_id: str
            sequence: int

    Returns:
        None: No value.
    """

    session_id: str
    owner_epoch: int
    owner_instance_id: str
    sequence: int

    def __post_init__(self) -> None:
        """Validate dataclass fields before object creation completes.

        Args:
            None

        Returns:
            None: No value.
        """

        _non_empty(self.session_id, "session_id")
        _positive_int(self.owner_epoch, "owner_epoch")
        _non_empty(self.owner_instance_id, "owner_instance_id")
        _positive_int(self.sequence, "sequence")


@dataclass(frozen=True, slots=True)
class HealthResult:
    """Decoded broker health response.

    Args:
            protocol_version: int
            term: int
            revision: int

    Returns:
        None: No value.
    """

    protocol_version: int
    term: int
    revision: int


@dataclass(frozen=True, slots=True)
class OwnerAcquisitionResult:
    """Decoded owner acquisition response.

    Args:
            owner_epoch: int

    Returns:
        None: No value.
    """

    owner_epoch: int


@dataclass(frozen=True, slots=True)
class NamespaceResult:
    """Decoded namespace mutation response.

    Args:
            term: int
            revision: int
            namespace_id: str
            namespace_revision: int

    Returns:
        None: No value.
    """

    term: int
    revision: int
    namespace_id: str
    namespace_revision: int


@dataclass(frozen=True, slots=True)
class TaskPublishedResult:
    """Decoded task publication response.

    Args:
            term: int
            revision: int
            task_id: str
            task_revision: int
            status: str

    Returns:
        None: No value.
    """

    term: int
    revision: int
    task_id: str
    task_revision: int
    status: str


@dataclass(frozen=True, slots=True)
class ConsumerGroupResult:
    """Decoded consumer-group mutation response.

    Args:
            term: int
            revision: int
            group_id: str
            generation: int
            group_revision: int
            member_count: int

    Returns:
        None: No value.
    """

    term: int
    revision: int
    group_id: str
    generation: int
    group_revision: int
    member_count: int


@dataclass(frozen=True, slots=True)
class HeartbeatResult:
    """Decoded heartbeat mutation response.

    Args:
            term: int
            revision: int
            group_id: str
            member_id: str
            generation: int
            member_revision: int

    Returns:
        None: No value.
    """

    term: int
    revision: int
    group_id: str
    member_id: str
    generation: int
    member_revision: int


@dataclass(frozen=True, slots=True)
class TaskClaimResult:
    """Decoded task claim response.

    Args:
            term: int
            revision: int
            task_id: str | None
            objective: str | None
            task_revision: int | None
            lease_id: str | None
            lease_epoch: int | None
            lease_expires_at_ms: int | None
            generation: int

    Returns:
        None: No value.
    """

    term: int
    revision: int
    task_id: str | None
    objective: str | None
    task_revision: int | None
    lease_id: str | None
    lease_epoch: int | None
    lease_expires_at_ms: int | None
    generation: int


@dataclass(frozen=True, slots=True)
class TaskLeaseRenewedResult:
    """Decoded lease renew response.

    Args:
            term: int
            revision: int
            task_id: str
            task_revision: int
            lease_id: str
            lease_epoch: int
            lease_expires_at_ms: int
            generation: int

    Returns:
        None: No value.
    """

    term: int
    revision: int
    task_id: str
    task_revision: int
    lease_id: str
    lease_epoch: int
    lease_expires_at_ms: int
    generation: int


@dataclass(frozen=True, slots=True)
class TaskCompletedResult:
    """Decoded task completion response.

    Args:
            term: int
            revision: int
            task_id: str
            task_revision: int
            status: str

    Returns:
        None: No value.
    """

    term: int
    revision: int
    task_id: str
    task_revision: int
    status: str


@dataclass(frozen=True, slots=True)
class ConsumerGroupSummary:
    """Authoritative read-only Consumer Group summary."""

    group_id: str
    namespace_id: str
    generation: int
    group_revision: int
    consumer_count: int


@dataclass(frozen=True, slots=True)
class ConsumerGroupDescription:
    """Authoritative single-group operations response."""

    broker_term: int
    broker_revision: int
    group: ConsumerGroupSummary


@dataclass(frozen=True, slots=True)
class ConsumerGroupPage:
    """Bounded authoritative Consumer Group directory page."""

    broker_term: int
    broker_revision: int
    groups: tuple[ConsumerGroupSummary, ...]
    next_after_group_id: str | None


BrokerResult = (
    NamespaceResult
    | TaskPublishedResult
    | ConsumerGroupResult
    | HeartbeatResult
    | TaskClaimResult
    | TaskLeaseRenewedResult
    | TaskCompletedResult
)

from __future__ import annotations

from enum import Enum


class BrokerErrorCode(str, Enum):
    """Broker protocol error code enumeration used across wire errors.

    Args:
            None

    Returns:
        None: No value.
    """

    INVALID_REQUEST = "INVALID_REQUEST"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    CAPACITY_EXCEEDED = "CAPACITY_EXCEEDED"
    STALE_FENCE = "STALE_FENCE"
    PERSISTENCE_ERROR = "PERSISTENCE_ERROR"
    TRANSPORT_ERROR = "TRANSPORT_ERROR"
    COMMIT_OUTCOME_UNKNOWN = "COMMIT_OUTCOME_UNKNOWN"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class ErrorDisposition(str, Enum):
    """Classification of how a request outcome is finalized.

    Args:
            None

    Returns:
        None: No value.
    """

    COMMITTED = "COMMITTED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


class AgentBrokerError(Exception):
    """Base class for all SDK-level failure types.

    Args:
            None

    Returns:
        None: No value.
    """


class ProtocolError(AgentBrokerError):
    """Raised when protocol payload or framing is invalid.

    Args:
            None

    Returns:
        None: No value.
    """


class TransportError(AgentBrokerError):
    """Raised when connection or transport I/O fails.

    Args:
            None

    Returns:
        None: No value.
    """


class ClusterRoutingError(AgentBrokerError):
    """Raised for cluster-discovery or routing failures that close operation safety.

    Args:
            None

    Returns:
        None: No value.
    """


class OperationsErrorCode(str, Enum):
    """Stable read-only operations error tokens returned by the Broker."""

    READ_AUTHORITY_UNAVAILABLE = "read_authority_unavailable"
    BROKER_FAIL_STOPPED = "broker_fail_stopped"
    BROKER_READ_FAILED = "broker_read_failed"
    STATE_OWNER_SATURATED = "state_owner_saturated"
    STATE_OWNER_UNAVAILABLE = "state_owner_unavailable"
    NOT_FOUND = "not_found"


class OperationsError(AgentBrokerError):
    """Typed Broker operations failure."""

    def __init__(self, code: OperationsErrorCode) -> None:
        """Initialize one typed operations failure."""

        super().__init__(f"broker operations request failed: {code.value}")
        self.code = code


class NoWriteReadyLeader(ClusterRoutingError):
    """Raised when no verified write-ready leader can be identified.

    Args:
            None

    Returns:
        None: No value.
    """


class MultipleWriteReadyLeaders(ClusterRoutingError):
    """Raised when more than one verified write-ready leader is discovered.

    Args:
            None

    Returns:
        None: No value.
    """

    def __init__(self, node_ids: tuple[int, ...]) -> None:
        """Initialize and validate the component state.

        Args:
                node_ids: tuple[int, ...]

        Returns:
            None: No value.
        """

        super().__init__(f"multiple write-ready leaders reported: {node_ids}")
        self.node_ids = node_ids


class InvalidOperationsResponse(ClusterRoutingError):
    """Raised when an operations readiness response is structurally invalid.

    Args:
            None

    Returns:
        None: No value.
    """

    def __init__(self, node_id: int, reason: str) -> None:
        """Initialize and validate the component state.

        Args:
                node_id: int
                reason: str

        Returns:
            None: No value.
        """

        super().__init__(
            f"node {node_id} returned invalid operations readiness: {reason}"
        )
        self.node_id = node_id
        self.reason = reason


class BrokerError(AgentBrokerError):
    """Typed error payload surfaced from protocol-level business responses.

    Args:
            None

    Returns:
        None: No value.
    """

    def __init__(
        self,
        code: BrokerErrorCode,
        message: str,
        disposition: ErrorDisposition | None = None,
    ) -> None:
        """Initialize and validate the component state.

        Args:
                code: BrokerErrorCode
                message: str
                disposition: ErrorDisposition | None

        Returns:
            None: No value.
        """

        super().__init__(message)
        self.code = code
        self.message = message
        self.disposition = disposition

    def __repr__(self) -> str:
        """Return a string representation for debugging this exception.

        Args:
            None

        Returns:
            str: Result of the operation.
        """

        return (
            f"BrokerError(code={self.code.value!r}, message={self.message!r}, "
            f"disposition={self.disposition.value if self.disposition else None!r})"
        )

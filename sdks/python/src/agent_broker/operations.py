from __future__ import annotations

import socket

from .errors import TransportError
from .models import ConsumerGroupDescription, ConsumerGroupPage, OperationsClientConfig
from .operations_protocol import (
    decode_describe_group,
    decode_list_groups,
    encode_describe_group,
    encode_list_groups,
)


class BrokerOperationsClient:
    """Synchronous read-only client for the Broker operations-v1 endpoint."""

    def __init__(self, config: OperationsClientConfig | None = None) -> None:
        """Initialize a read-only operations client."""

        self.config = config or OperationsClientConfig()

    def describe_group(self, group_id: str) -> ConsumerGroupDescription:
        """Return the authoritative current summary for one Consumer Group."""

        return decode_describe_group(self._round_trip(encode_describe_group(group_id)))

    def list_groups(
        self,
        *,
        limit: int = 8,
        after_group_id: str | None = None,
    ) -> ConsumerGroupPage:
        """Return one bounded authoritative Consumer Group directory page."""

        return decode_list_groups(
            self._round_trip(encode_list_groups(limit, after_group_id))
        )

    def _round_trip(self, frame: bytes) -> bytes:
        try:
            with socket.create_connection(
                (self.config.host, self.config.port),
                timeout=self.config.timeout_seconds,
            ) as sock:
                sock.settimeout(self.config.timeout_seconds)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.sendall(frame)
                with sock.makefile("rb") as reader:
                    response = reader.readline(self.config.max_response_frame_bytes + 1)
        except OSError as error:
            raise TransportError(f"operations transport failed: {error}") from error
        if not response:
            raise TransportError("operations server closed without a response")
        if len(response) > self.config.max_response_frame_bytes:
            raise TransportError("operations response frame exceeded configured bound")
        return response

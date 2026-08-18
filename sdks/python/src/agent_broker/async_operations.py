from __future__ import annotations

import asyncio

from .errors import TransportError
from .models import ConsumerGroupDescription, ConsumerGroupPage, OperationsClientConfig
from .operations_protocol import (
    decode_describe_group,
    decode_list_groups,
    encode_describe_group,
    encode_list_groups,
)


class AsyncBrokerOperationsClient:
    """Native-async read-only client for the Broker operations-v1 endpoint."""

    def __init__(self, config: OperationsClientConfig | None = None) -> None:
        """Initialize a native-async read-only operations client."""

        self.config = config or OperationsClientConfig()

    async def describe_group(self, group_id: str) -> ConsumerGroupDescription:
        """Return the authoritative current summary for one Consumer Group."""

        return decode_describe_group(
            await self._round_trip(encode_describe_group(group_id))
        )

    async def list_groups(
        self,
        *,
        limit: int = 8,
        after_group_id: str | None = None,
    ) -> ConsumerGroupPage:
        """Return one bounded authoritative Consumer Group directory page."""

        return decode_list_groups(
            await self._round_trip(encode_list_groups(limit, after_group_id))
        )

    async def _round_trip(self, frame: bytes) -> bytes:
        writer: asyncio.StreamWriter | None = None
        try:
            async with asyncio.timeout(self.config.timeout_seconds):
                reader, writer = await asyncio.open_connection(
                    self.config.host,
                    self.config.port,
                    limit=self.config.max_response_frame_bytes + 1,
                )
                writer.write(frame)
                await writer.drain()
                response = await reader.readline()
        except (OSError, TimeoutError) as error:
            raise TransportError(f"operations transport failed: {error}") from error
        except asyncio.LimitOverrunError as error:
            raise TransportError(
                "operations response frame exceeded configured bound"
            ) from error
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass
        if not response:
            raise TransportError("operations server closed without a response")
        if len(response) > self.config.max_response_frame_bytes:
            raise TransportError("operations response frame exceeded configured bound")
        return response

from __future__ import annotations

import asyncio
import json
import socket
import threading
import unittest
from collections.abc import Callable, Coroutine
from typing import Self

from agent_broker import (
    AsyncBrokerOperationsClient,
    BrokerOperationsClient,
    OperationsClientConfig,
    OperationsError,
    OperationsErrorCode,
    ProtocolError,
)


def _encoded(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode() + b"\n"


class OperationsClientTests(unittest.TestCase):
    @staticmethod
    def _server(
        responses: tuple[bytes, ...],
    ) -> tuple[int, threading.Thread, list[bytes]]:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(len(responses))
        port = listener.getsockname()[1]
        observed: list[bytes] = []

        def serve() -> None:
            try:
                for response in responses:
                    conn, _ = listener.accept()
                    with conn, conn.makefile("rb") as reader:
                        observed.append(reader.readline())
                        conn.sendall(response)
            finally:
                listener.close()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        return port, thread, observed

    def test_describe_and_list_groups_return_typed_results(self) -> None:
        describe = _encoded(
            {
                "schema_version": 1,
                "operation": "describe_group",
                "status": "ok",
                "broker_term": 3,
                "broker_revision": 9,
                "group": {
                    "group_id": "backend-company",
                    "namespace_id": "project-a",
                    "generation": 4,
                    "group_revision": 7,
                    "consumer_count": 2,
                },
            }
        )
        listing = _encoded(
            {
                "schema_version": 1,
                "operation": "list_groups",
                "status": "ok",
                "broker_term": 3,
                "broker_revision": 9,
                "groups": [
                    {
                        "group_id": "backend-company",
                        "namespace_id": "project-a",
                        "generation": 4,
                        "group_revision": 7,
                        "consumer_count": 2,
                    }
                ],
                "next_after_group_id": None,
            }
        )
        port, thread, observed = self._server((describe, listing))
        client = BrokerOperationsClient(OperationsClientConfig(port=port))

        description = client.describe_group("backend-company")
        page = client.list_groups(limit=8)
        thread.join(timeout=2)

        self.assertEqual(description.group.consumer_count, 2)
        self.assertEqual(page.groups, (description.group,))
        self.assertIsNone(page.next_after_group_id)
        self.assertEqual(
            json.loads(observed[0]),
            {
                "group_id": "backend-company",
                "operation": "describe_group",
                "schema_version": 1,
            },
        )
        self.assertEqual(json.loads(observed[1])["limit"], 8)

    def test_operations_errors_and_malformed_frames_are_typed(self) -> None:
        error = _encoded(
            {
                "schema_version": 1,
                "status": "error",
                "code": "read_authority_unavailable",
            }
        )
        malformed = _encoded(
            {
                "schema_version": 1,
                "operation": "list_groups",
                "status": "ok",
                "broker_term": 1,
                "broker_revision": 0,
                "groups": [],
            }
        )
        port, thread, _ = self._server((error, malformed))
        client = BrokerOperationsClient(OperationsClientConfig(port=port))

        with self.assertRaises(OperationsError) as raised:
            client.list_groups()
        self.assertIs(
            raised.exception.code, OperationsErrorCode.READ_AUTHORITY_UNAVAILABLE
        )
        with self.assertRaises(ProtocolError):
            client.list_groups()
        thread.join(timeout=2)


class _AsyncServerHarness:
    def __init__(
        self,
        handler: Callable[
            [asyncio.StreamReader, asyncio.StreamWriter],
            Coroutine[object, object, None],
        ],
    ) -> None:
        self._handler = handler
        self._server: asyncio.Server | None = None
        self._tasks: set[asyncio.Task[None]] = set()

    async def __aenter__(self) -> Self:
        def admit(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            task = asyncio.create_task(self._handler(reader, writer))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        self._server = await asyncio.start_server(admit, "127.0.0.1", 0)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self._tasks:
            await asyncio.gather(*self._tasks)

    @property
    def port(self) -> int:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("async operations test server is not active")
        return int(self._server.sockets[0].getsockname()[1])


class AsyncOperationsClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_async_operations_client_handles_concurrent_reads(
        self,
    ) -> None:
        async def handler(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                request = json.loads(await reader.readuntil(b"\n"))
                group_id = request["group_id"]
                writer.write(
                    _encoded(
                        {
                            "schema_version": 1,
                            "operation": "describe_group",
                            "status": "ok",
                            "broker_term": 5,
                            "broker_revision": 12,
                            "group": {
                                "group_id": group_id,
                                "namespace_id": "project-a",
                                "generation": 2,
                                "group_revision": 3,
                                "consumer_count": 1,
                            },
                        }
                    )
                )
                await writer.drain()
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass

        async with _AsyncServerHarness(handler) as server:
            client = AsyncBrokerOperationsClient(
                OperationsClientConfig(port=server.port, timeout_seconds=1.0)
            )
            results = await asyncio.gather(
                *(client.describe_group(f"company-{index}") for index in range(10))
            )

        self.assertEqual(len(results), 10)
        self.assertEqual({result.group.consumer_count for result in results}, {1})


if __name__ == "__main__":
    unittest.main()

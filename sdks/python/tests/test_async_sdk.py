from __future__ import annotations

import asyncio
import json
import unittest
from collections.abc import Callable, Coroutine
from typing import Self

from agent_broker import (
    AsyncBrokerClient,
    AsyncClusterBrokerClient,
    AsyncStandaloneBrokerClient,
    BrokerClientConfig,
    CommandIdentity,
    InvalidOperationsResponse,
    MultipleWriteReadyLeaders,
    ProtocolError,
    RetryPolicy,
    StaticClusterConfig,
    StaticClusterNode,
    TransportError,
)


class _AsyncServerHarness:
    def __init__(
        self,
        handler: Callable[
            [asyncio.StreamReader, asyncio.StreamWriter],
            Coroutine[object, object, None],
        ],
    ) -> None:
        self._handler = handler
        self._task_group: asyncio.TaskGroup | None = None
        self._server: asyncio.Server | None = None

    async def __aenter__(self) -> Self:
        self._task_group = asyncio.TaskGroup()
        await self._task_group.__aenter__()

        def admit(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            if self._task_group is None:
                writer.close()
                return
            self._task_group.create_task(self._handler(reader, writer))

        self._server = await asyncio.start_server(admit, "127.0.0.1", 0)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self._task_group is not None:
            await self._task_group.__aexit__(exc_type, exc, tb)

    @property
    def port(self) -> int:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("async test server is not active")
        return int(self._server.sockets[0].getsockname()[1])


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        await writer.wait_closed()
    except OSError:
        pass


class AsyncBrokerClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_health_uses_native_event_loop_transport_concurrently(self) -> None:
        observed_request_ids: list[str] = []

        async def handler(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                request = json.loads(await reader.readuntil(b"\n"))
                request_id = request["request_id"]
                observed_request_ids.append(request_id)
                response = {
                    "version": 1,
                    "request_id": request_id,
                    "ok": True,
                    "result": {"protocol_version": 1, "term": 4, "revision": 9},
                }
                writer.write(
                    json.dumps(response, separators=(",", ":")).encode() + b"\n"
                )
                await writer.drain()
            finally:
                await _close_writer(writer)

        async with _AsyncServerHarness(handler) as server:
            client = AsyncBrokerClient(
                BrokerClientConfig(port=server.port, timeout_seconds=1.0)
            )
            results = await asyncio.gather(*(client.health() for _ in range(20)))

        self.assertTrue(
            all(result.term == 4 and result.revision == 9 for result in results)
        )
        self.assertEqual(len(observed_request_ids), 20)
        self.assertEqual(len(set(observed_request_ids)), 20)

    async def test_transport_retry_reuses_exact_serialized_frame(self) -> None:
        observed: list[bytes] = []
        attempt = 0

        async def handler(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            nonlocal attempt
            try:
                frame = await reader.readuntil(b"\n")
                observed.append(frame)
                attempt += 1
                if attempt == 2:
                    request_id = json.loads(frame)["request_id"]
                    response = {
                        "version": 3,
                        "request_id": request_id,
                        "ok": True,
                        "result": {
                            "term": 1,
                            "revision": 1,
                            "namespace_id": "ns",
                            "namespace_revision": 1,
                        },
                    }
                    writer.write(
                        json.dumps(response, separators=(",", ":")).encode() + b"\n"
                    )
                    await writer.drain()
            finally:
                await _close_writer(writer)

        async with _AsyncServerHarness(handler) as server:
            client = AsyncBrokerClient(
                BrokerClientConfig(port=server.port, timeout_seconds=1.0)
            )
            result = await client.ensure_namespace(
                CommandIdentity("session-async", 1, "worker-a", 1),
                namespace_id="ns",
                retry=RetryPolicy(max_attempts=2),
            )

        self.assertEqual(result.namespace_id, "ns")
        self.assertEqual(len(observed), 2)
        self.assertEqual(observed[0], observed[1])

    async def test_cancellation_propagates_after_request_submission(self) -> None:
        request_received = asyncio.Event()
        observed: list[bytes] = []

        async def handler(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                observed.append(await reader.readuntil(b"\n"))
                request_received.set()
                await reader.read()
            finally:
                await _close_writer(writer)

        async with _AsyncServerHarness(handler) as server:
            client = AsyncBrokerClient(
                BrokerClientConfig(port=server.port, timeout_seconds=2.0)
            )
            task = asyncio.create_task(
                client.ensure_namespace(
                    CommandIdentity("session-cancel", 1, "worker-a", 1),
                    namespace_id="ns",
                )
            )
            await asyncio.wait_for(request_received.wait(), timeout=1.0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertEqual(len(observed), 1)

    async def test_timeout_is_bounded_transport_failure(self) -> None:
        request_received = asyncio.Event()

        async def handler(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                await reader.readuntil(b"\n")
                request_received.set()
                await reader.read()
            finally:
                await _close_writer(writer)

        async with _AsyncServerHarness(handler) as server:
            client = AsyncBrokerClient(
                BrokerClientConfig(port=server.port, timeout_seconds=0.05)
            )
            with self.assertRaises(TransportError):
                await client.health()
            self.assertTrue(request_received.is_set())

    async def test_oversized_response_fails_protocol_bound(self) -> None:
        async def handler(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                await reader.readuntil(b"\n")
                writer.write(b"x" * 4097 + b"\n")
                await writer.drain()
            finally:
                await _close_writer(writer)

        async with _AsyncServerHarness(handler) as server:
            client = AsyncBrokerClient(
                BrokerClientConfig(
                    port=server.port,
                    timeout_seconds=1.0,
                    max_response_frame_bytes=4096,
                )
            )
            with self.assertRaises(ProtocolError):
                await client.health()

    async def test_standalone_ambiguous_transport_does_not_retry_mutation(self) -> None:
        observed: list[bytes] = []

        async def handler(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                observed.append(await reader.readuntil(b"\n"))
            finally:
                await _close_writer(writer)

        async with _AsyncServerHarness(handler) as server:
            client = AsyncStandaloneBrokerClient(
                BrokerClientConfig(port=server.port, timeout_seconds=1.0)
            )
            with self.assertRaises(TransportError):
                await client.ensure_namespace(
                    namespace_id="ns", request_id="standalone-once"
                )

        self.assertEqual(len(observed), 1)


class AsyncClusterRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def _operations_server(
        self,
        *,
        node_id: int,
        write_ready: bool,
        reported_node: int | None = None,
        current_leader: int | None = None,
    ) -> _AsyncServerHarness:
        if write_ready:
            response = {
                "schema_version": 1,
                "operation": "readiness",
                "live": True,
                "write_ready": True,
                "reason": "ready",
                "maintenance_authority": True,
                "consensus": {
                    "status": "ready",
                    "write_ready": True,
                    "progress": {
                        "node_id": reported_node
                        if reported_node is not None
                        else node_id,
                        "current_leader": current_leader
                        if current_leader is not None
                        else node_id,
                    },
                },
            }
        else:
            response = {
                "schema_version": 1,
                "operation": "readiness",
                "write_ready": False,
            }
        encoded = json.dumps(response, separators=(",", ":")).encode() + b"\n"

        async def handler(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                await reader.readuntil(b"\n")
                writer.write(encoded)
                await writer.drain()
            finally:
                await _close_writer(writer)

        harness = _AsyncServerHarness(handler)
        await harness.__aenter__()
        return harness

    async def _router(
        self,
        readiness: tuple[tuple[bool, int | None, int | None], ...],
    ) -> tuple[AsyncClusterBrokerClient, list[_AsyncServerHarness]]:
        harnesses: list[_AsyncServerHarness] = []
        nodes: list[StaticClusterNode] = []
        for index, (write_ready, reported_node, current_leader) in enumerate(
            readiness, start=1
        ):
            harness = await self._operations_server(
                node_id=index,
                write_ready=write_ready,
                reported_node=reported_node,
                current_leader=current_leader,
            )
            harnesses.append(harness)
            nodes.append(
                StaticClusterNode(
                    node_id=index,
                    broker_port=19_000 + index,
                    operations_port=harness.port,
                )
            )
        config = StaticClusterConfig(nodes=(nodes[0], nodes[1], nodes[2]))
        return AsyncClusterBrokerClient(config), harnesses

    async def _close_harnesses(self, harnesses: list[_AsyncServerHarness]) -> None:
        for harness in reversed(harnesses):
            await harness.__aexit__(None, None, None)

    async def test_discovers_exactly_one_verified_ready_node(self) -> None:
        router, harnesses = await self._router(
            ((False, None, None), (True, None, None), (False, None, None))
        )
        try:
            leader = await router.discover_write_leader()
        finally:
            await self._close_harnesses(harnesses)
        self.assertEqual(leader.node_id, 2)

    async def test_multiple_ready_nodes_fail_closed(self) -> None:
        router, harnesses = await self._router(
            ((True, None, None), (True, None, None), (False, None, None))
        )
        try:
            with self.assertRaises(MultipleWriteReadyLeaders):
                await router.discover_write_leader()
        finally:
            await self._close_harnesses(harnesses)

    async def test_ready_identity_mismatch_fails_closed(self) -> None:
        router, harnesses = await self._router(
            ((False, None, None), (True, 1, 2), (False, None, None))
        )
        try:
            with self.assertRaises(InvalidOperationsResponse):
                await router.discover_write_leader()
        finally:
            await self._close_harnesses(harnesses)

    async def test_failover_rediscovery_reuses_exact_serialized_frame(self) -> None:
        failed_over = asyncio.Event()
        node_one_frames: list[bytes] = []
        node_two_frames: list[bytes] = []

        async def broker_one(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                node_one_frames.append(await reader.readuntil(b"\n"))
                failed_over.set()
            finally:
                await _close_writer(writer)

        async def broker_two(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                frame = await reader.readuntil(b"\n")
                node_two_frames.append(frame)
                request_id = json.loads(frame)["request_id"]
                response = {
                    "version": 3,
                    "request_id": request_id,
                    "ok": True,
                    "result": {
                        "term": 2,
                        "revision": 2,
                        "namespace_id": "ns",
                        "namespace_revision": 1,
                    },
                }
                writer.write(
                    json.dumps(response, separators=(",", ":")).encode() + b"\n"
                )
                await writer.drain()
            finally:
                await _close_writer(writer)

        def readiness_payload(node_id: int, ready: bool) -> bytes:
            if not ready:
                value = {
                    "schema_version": 1,
                    "operation": "readiness",
                    "write_ready": False,
                }
            else:
                value = {
                    "schema_version": 1,
                    "operation": "readiness",
                    "live": True,
                    "write_ready": True,
                    "reason": "ready",
                    "maintenance_authority": True,
                    "consensus": {
                        "status": "ready",
                        "write_ready": True,
                        "progress": {"node_id": node_id, "current_leader": node_id},
                    },
                }
            return json.dumps(value, separators=(",", ":")).encode() + b"\n"

        def operations_handler(node_id: int):
            async def handler(
                reader: asyncio.StreamReader, writer: asyncio.StreamWriter
            ) -> None:
                try:
                    await reader.readuntil(b"\n")
                    if node_id == 1:
                        ready = not failed_over.is_set()
                    elif node_id == 2:
                        ready = failed_over.is_set()
                    else:
                        ready = False
                    writer.write(readiness_payload(node_id, ready))
                    await writer.drain()
                finally:
                    await _close_writer(writer)

            return handler

        async with (
            _AsyncServerHarness(broker_one) as broker_one_server,
            _AsyncServerHarness(broker_two) as broker_two_server,
            _AsyncServerHarness(operations_handler(1)) as ops_one,
            _AsyncServerHarness(operations_handler(2)) as ops_two,
            _AsyncServerHarness(operations_handler(3)) as ops_three,
        ):
            config = StaticClusterConfig(
                nodes=(
                    StaticClusterNode(1, broker_one_server.port, ops_one.port),
                    StaticClusterNode(2, broker_two_server.port, ops_two.port),
                    StaticClusterNode(3, 19_003, ops_three.port),
                ),
                timeout_seconds=1.0,
                retry=RetryPolicy(max_attempts=2),
            )
            client = AsyncClusterBrokerClient(config)
            result = await client.ensure_namespace(
                CommandIdentity("session-failover", 1, "worker-a", 1),
                namespace_id="ns",
                request_id="cluster-exact-frame",
            )

        self.assertEqual(result.namespace_id, "ns")
        self.assertEqual(len(node_one_frames), 1)
        self.assertEqual(len(node_two_frames), 1)
        self.assertEqual(node_one_frames[0], node_two_frames[0])


if __name__ == "__main__":
    unittest.main()

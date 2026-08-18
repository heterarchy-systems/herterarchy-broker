from __future__ import annotations

import json
import socket
import threading
import unittest

from agent_broker import (
    BrokerClient,
    BrokerClientConfig,
    BrokerError,
    BrokerErrorCode,
    CommandIdentity,
    ErrorDisposition,
    InvalidOperationsResponse,
    MultipleWriteReadyLeaders,
    ProtocolError,
    RetryPolicy,
    StaticClusterBrokerClient,
    StaticClusterConfig,
    StaticClusterNode,
)
from agent_broker.protocol import (
    Operation,
    decode_mutation_response,
    encode_owner_acquire,
    encode_owner_mutation,
    validate_request_id,
)


class ProtocolTests(unittest.TestCase):
    def test_request_id_contract(self) -> None:
        self.assertEqual(validate_request_id("abc-1_:x.y"), "abc-1_:x.y")
        for invalid in ("", "-starts-wrong", "a" * 129, "한글"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                validate_request_id(invalid)

    def test_v3_owner_frame_is_explicit_and_newline_delimited(self) -> None:
        identity = CommandIdentity("session-1", 2, "worker-a", 7)
        frame = encode_owner_mutation(
            "req-1",
            Operation.ENSURE_NAMESPACE,
            {"namespace_id": "ns"},
            session_id=identity.session_id,
            owner_epoch=identity.owner_epoch,
            owner_instance_id=identity.owner_instance_id,
            sequence=identity.sequence,
        )
        self.assertTrue(frame.endswith(b"\n"))
        value = json.loads(frame)
        self.assertEqual(value["version"], 3)
        self.assertEqual(value["command_session_id"], "session-1")
        self.assertEqual(value["owner_epoch"], 2)
        self.assertEqual(value["owner_instance_id"], "worker-a")
        self.assertEqual(value["command_sequence"], 7)

    def test_owner_acquisition_epoch_must_be_positive(self) -> None:
        with self.assertRaises(ValueError):
            encode_owner_acquire("req-owner", "session-1", 0, "worker-a")

    def test_v3_error_disposition_is_typed(self) -> None:
        frame = (
            json.dumps(
                {
                    "version": 3,
                    "request_id": "req-1",
                    "ok": False,
                    "error": {
                        "code": "STALE_FENCE",
                        "message": "stale",
                        "disposition": "REJECTED",
                    },
                }
            ).encode()
            + b"\n"
        )
        with self.assertRaises(BrokerError) as raised:
            decode_mutation_response(frame, "req-1", Operation.ENSURE_NAMESPACE)
        self.assertIs(raised.exception.code, BrokerErrorCode.STALE_FENCE)
        self.assertIs(raised.exception.disposition, ErrorDisposition.REJECTED)

    def test_response_correlation_is_strict(self) -> None:
        frame = b'{"version":3,"request_id":"other","ok":true,"result":{"term":1,"revision":1,"namespace_id":"ns","namespace_revision":1}}\n'
        with self.assertRaises(ProtocolError):
            decode_mutation_response(frame, "req-1", Operation.ENSURE_NAMESPACE)

    def test_config_bounds_and_loopback_only(self) -> None:
        BrokerClientConfig()
        with self.assertRaises(ValueError):
            BrokerClientConfig(host="0.0.0.0")
        with self.assertRaises(ValueError):
            BrokerClientConfig(timeout_seconds=0)
        with self.assertRaises(ValueError):
            BrokerClientConfig(max_response_frame_bytes=4095)
        with self.assertRaises(ValueError):
            BrokerClientConfig(max_response_frame_bytes=1024 * 1024 + 1)


class ExactRetryTests(unittest.TestCase):
    def test_transport_retry_reuses_exact_serialized_frame(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(2)
        port = listener.getsockname()[1]
        observed: list[bytes] = []
        server_error: list[OSError] = []

        def server() -> None:
            try:
                for attempt in range(2):
                    conn, _ = listener.accept()
                    with conn, conn.makefile("rb") as reader:
                        observed.append(reader.readline())
                        if attempt == 1:
                            conn.sendall(
                                b'{"version":3,"request_id":"req-retry","ok":true,"result":{"term":1,"revision":1,"namespace_id":"ns","namespace_revision":1}}\n'
                            )
            except OSError as error:
                server_error.append(error)
            finally:
                listener.close()

        thread = threading.Thread(target=server, daemon=True)
        thread.start()
        client = BrokerClient(BrokerClientConfig(port=port, timeout_seconds=1.0))
        try:
            result = client.ensure_namespace(
                CommandIdentity("session-1", 1, "worker-a", 1),
                namespace_id="ns",
                request_id="req-retry",
                retry=RetryPolicy(max_attempts=2),
            )
        finally:
            client.close()
        thread.join(timeout=2)
        self.assertFalse(server_error)
        self.assertEqual(result.namespace_id, "ns")
        self.assertEqual(len(observed), 2)
        self.assertEqual(observed[0], observed[1])


class StaticClusterRoutingTests(unittest.TestCase):
    @staticmethod
    def _operations_server(
        *,
        node_id: int,
        write_ready: bool,
        reported_node: int | None = None,
        current_leader: int | None = None,
    ) -> tuple[int, threading.Thread, list[OSError]]:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        server_error: list[OSError] = []
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

        def server() -> None:
            try:
                conn, _ = listener.accept()
                with conn, conn.makefile("rb") as reader:
                    reader.readline()
                    conn.sendall(encoded)
            except OSError as error:
                server_error.append(error)
            finally:
                listener.close()

        thread = threading.Thread(target=server, daemon=True)
        thread.start()
        return port, thread, server_error

    def _router(
        self,
        readiness: tuple[tuple[bool, int | None, int | None], ...],
    ) -> tuple[StaticClusterBrokerClient, list[threading.Thread], list[list[OSError]]]:
        nodes: list[StaticClusterNode] = []
        threads: list[threading.Thread] = []
        server_errors: list[list[OSError]] = []
        for index, (write_ready, reported_node, current_leader) in enumerate(
            readiness, start=1
        ):
            port, thread, errors = self._operations_server(
                node_id=index,
                write_ready=write_ready,
                reported_node=reported_node,
                current_leader=current_leader,
            )
            nodes.append(
                StaticClusterNode(
                    node_id=index,
                    broker_port=19_000 + index,
                    operations_port=port,
                )
            )
            threads.append(thread)
            server_errors.append(errors)
        config = StaticClusterConfig(nodes=(nodes[0], nodes[1], nodes[2]))
        return StaticClusterBrokerClient(config), threads, server_errors

    def test_discovers_exactly_one_verified_ready_node(self) -> None:
        router, threads, server_errors = self._router(
            ((False, None, None), (True, None, None), (False, None, None))
        )
        leader = router.discover_write_leader()
        self.assertEqual(leader.node_id, 2)
        for thread in threads:
            thread.join(timeout=2)
        self.assertFalse(any(server_errors))

    def test_multiple_ready_nodes_fail_closed(self) -> None:
        router, threads, server_errors = self._router(
            ((True, None, None), (True, None, None), (False, None, None))
        )
        with self.assertRaises(MultipleWriteReadyLeaders):
            router.discover_write_leader()
        for thread in threads:
            thread.join(timeout=2)
        self.assertFalse(any(server_errors))

    def test_ready_identity_mismatch_fails_closed(self) -> None:
        router, threads, server_errors = self._router(
            ((False, None, None), (True, 1, 2), (False, None, None))
        )
        with self.assertRaises(InvalidOperationsResponse):
            router.discover_write_leader()
        for thread in threads:
            thread.join(timeout=2)
        self.assertFalse(any(server_errors))


if __name__ == "__main__":
    unittest.main()

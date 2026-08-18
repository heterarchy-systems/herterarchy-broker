from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from agent_broker import BrokerClientConfig, StandaloneBrokerClient, TransportError


@unittest.skipUnless(
    os.environ.get("AGENT_BROKER_RUN_RUST_INTEGRATION") == "1",
    "set AGENT_BROKER_RUN_RUST_INTEGRATION=1 to run the real Rust broker integration",
)
class RustStandaloneIntegrationTests(unittest.TestCase):
    def test_health_mutation_and_restart_recovery(self) -> None:
        repo = Path(__file__).resolve().parents[3]
        subprocess.run(
            [
                "cargo",
                "build",
                "-q",
                "-p",
                "agent-broker-runtime",
                "--bin",
                "agentbrokerd",
            ],
            cwd=repo,
            check=True,
        )
        binary = repo / "target" / "debug" / "agentbrokerd"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        with tempfile.TemporaryDirectory(prefix="agent-broker-python-sdk-") as temp_dir:
            state_path = Path(temp_dir) / "state"
            command = [
                str(binary),
                "serve",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--state-path",
                str(state_path),
            ]

            def start_broker() -> subprocess.Popen[str]:
                return subprocess.Popen(
                    command,
                    cwd=repo,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                )

            def stop_broker(process: subprocess.Popen[str]) -> None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
                if process.stderr is not None:
                    process.stderr.close()

            def wait_for_health(
                process: subprocess.Popen[str], client: StandaloneBrokerClient
            ):
                deadline = time.monotonic() + 10.0
                while True:
                    if process.poll() is not None:
                        stderr = (
                            process.stderr.read() if process.stderr is not None else ""
                        )
                        self.fail(f"agentbrokerd exited before readiness: {stderr}")
                    try:
                        return client.health()
                    except TransportError:
                        if time.monotonic() >= deadline:
                            self.fail(
                                "agentbrokerd did not become reachable within 10 seconds"
                            )
                        time.sleep(0.05)

            process = start_broker()
            client = StandaloneBrokerClient(
                BrokerClientConfig(port=port, timeout_seconds=1.0)
            )
            try:
                health = wait_for_health(process, client)
                self.assertEqual(health.protocol_version, 1)
                namespace = client.ensure_namespace(
                    namespace_id="python-sdk-integration",
                )
                self.assertEqual(namespace.namespace_id, "python-sdk-integration")
                self.assertGreaterEqual(namespace.revision, 1)

                committed_revision = namespace.revision
                client.close()
                stop_broker(process)

                process = start_broker()
                client = StandaloneBrokerClient(
                    BrokerClientConfig(port=port, timeout_seconds=1.0)
                )
                recovered_health = wait_for_health(process, client)
                self.assertGreaterEqual(recovered_health.revision, committed_revision)

                recovered_namespace = client.ensure_namespace(
                    namespace_id="python-sdk-integration",
                )
                self.assertEqual(
                    recovered_namespace.namespace_id, "python-sdk-integration"
                )
                self.assertEqual(recovered_namespace.revision, committed_revision)
            finally:
                client.close()
                if process.poll() is None:
                    stop_broker(process)


if __name__ == "__main__":
    unittest.main()

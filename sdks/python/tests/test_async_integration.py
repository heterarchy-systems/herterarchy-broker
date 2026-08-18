from __future__ import annotations

import asyncio
import os
import socket
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from agent_broker import (
    AsyncClusterBrokerClient,
    AsyncStandaloneBrokerClient,
    BrokerClientConfig,
    CommandIdentity,
    NoWriteReadyLeader,
    StaticClusterConfig,
    StaticClusterNode,
    TransportError,
)


def _reserve_loopback_ports(count: int) -> tuple[int, ...]:
    reservations = [
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) for _ in range(count)
    ]
    try:
        for reservation in reservations:
            reservation.bind(("127.0.0.1", 0))
        return tuple(int(reservation.getsockname()[1]) for reservation in reservations)
    finally:
        for reservation in reservations:
            reservation.close()


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=3.0)
    except TimeoutError:
        process.kill()
        await asyncio.wait_for(process.wait(), timeout=3.0)


@unittest.skipUnless(
    os.environ.get("AGENT_BROKER_RUN_ASYNC_RUST_INTEGRATION") == "1",
    "set AGENT_BROKER_RUN_ASYNC_RUST_INTEGRATION=1 to use prebuilt Rust broker binaries",
)
class AsyncRustStandaloneIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_health_mutation_and_restart_recovery(self) -> None:
        repo = Path(__file__).resolve().parents[3]
        binary = repo / "target" / "debug" / "agentbrokerd"
        self.assertTrue(binary.is_file(), f"prebuilt Rust Broker is missing: {binary}")
        port, operations_port = _reserve_loopback_ports(2)

        with tempfile.TemporaryDirectory(
            prefix="agent-broker-python-async-sdk-"
        ) as temp_dir:
            state_path = Path(temp_dir) / "state"
            command = (
                str(binary),
                "serve",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--operations-port",
                str(operations_port),
                "--state-path",
                str(state_path),
            )

            async def start_broker() -> asyncio.subprocess.Process:
                return await asyncio.create_subprocess_exec(
                    *command,
                    cwd=repo,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )

            async def wait_for_health(
                process: asyncio.subprocess.Process,
                client: AsyncStandaloneBrokerClient,
            ):
                deadline = asyncio.get_running_loop().time() + 10.0
                while True:
                    if process.returncode is not None:
                        stderr = (
                            await process.stderr.read()
                            if process.stderr is not None
                            else b""
                        )
                        self.fail(
                            "agentbrokerd exited before readiness: "
                            f"{stderr.decode(errors='replace')}"
                        )
                    try:
                        return await client.health()
                    except TransportError:
                        if asyncio.get_running_loop().time() >= deadline:
                            self.fail(
                                "agentbrokerd did not become reachable within 10 seconds"
                            )
                        await asyncio.sleep(0.05)

            process = await start_broker()
            client = AsyncStandaloneBrokerClient(
                BrokerClientConfig(port=port, timeout_seconds=1.0)
            )
            try:
                health = await wait_for_health(process, client)
                self.assertEqual(health.protocol_version, 1)
                namespace = await client.ensure_namespace(
                    namespace_id="python-async-sdk-integration"
                )
                self.assertEqual(namespace.namespace_id, "python-async-sdk-integration")
                committed_revision = namespace.revision

                await _stop_process(process)
                process = await start_broker()
                recovered_health = await wait_for_health(process, client)
                self.assertGreaterEqual(recovered_health.revision, committed_revision)

                recovered_namespace = await client.ensure_namespace(
                    namespace_id="python-async-sdk-integration"
                )
                self.assertEqual(
                    recovered_namespace.namespace_id, "python-async-sdk-integration"
                )
                self.assertEqual(recovered_namespace.revision, committed_revision)
            finally:
                await _stop_process(process)


@unittest.skipUnless(
    os.environ.get("AGENT_BROKER_RUN_ASYNC_CLUSTER_INTEGRATION") == "1",
    "set AGENT_BROKER_RUN_ASYNC_CLUSTER_INTEGRATION=1 to use a prebuilt Rust three-node cluster",
)
class AsyncRustClusterIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_leader_failover_restart_and_full_owner_aware_lifecycle(self) -> None:
        repo = Path(__file__).resolve().parents[3]
        binary = repo / "target" / "debug" / "agentbrokerd"
        tls_generator = repo / "target" / "debug" / "examples" / "generate_cluster_tls"
        self.assertTrue(binary.is_file(), f"prebuilt Rust Broker is missing: {binary}")
        self.assertTrue(
            tls_generator.is_file(),
            f"prebuilt TLS fixture generator is missing: {tls_generator}",
        )

        reserved_ports = _reserve_loopback_ports(9)
        ports = reserved_ports[0:3]
        operations_ports = reserved_ports[3:6]
        raft_ports = reserved_ports[6:9]

        with tempfile.TemporaryDirectory(
            prefix="agent-broker-python-async-cluster-"
        ) as temp_dir:
            root = Path(temp_dir)
            tls_dir = root / "tls"
            generator = await asyncio.create_subprocess_exec(
                str(tls_generator),
                str(tls_dir),
                "1",
                "2",
                "3",
                cwd=repo,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, generator_stderr = await generator.communicate()
            self.assertEqual(
                generator.returncode,
                0,
                generator_stderr.decode(errors="replace"),
            )

            raft_nodes = [
                item
                for node_id, raft_port in enumerate(raft_ports, start=1)
                for item in ("--raft-node", f"{node_id}=127.0.0.1:{raft_port}")
            ]

            def cluster_command(node_id: int, *, bootstrap: bool) -> tuple[str, ...]:
                command = [
                    str(binary),
                    "serve-cluster",
                    "--node-id",
                    str(node_id),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(ports[node_id - 1]),
                    "--operations-port",
                    str(operations_ports[node_id - 1]),
                    "--raft-host",
                    "127.0.0.1",
                    "--raft-port",
                    str(raft_ports[node_id - 1]),
                    *raft_nodes,
                    "--raft-tls-dir",
                    str(tls_dir),
                    "--state-path",
                    str(root / f"node-{node_id}.redb"),
                ]
                if bootstrap:
                    command.append("--bootstrap")
                return tuple(command)

            async def start_node(node_id: int) -> asyncio.subprocess.Process:
                return await asyncio.create_subprocess_exec(
                    *cluster_command(node_id, bootstrap=node_id == 1),
                    cwd=repo,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )

            processes: dict[int, asyncio.subprocess.Process] = {}
            try:
                for node_id in (2, 3, 1):
                    processes[node_id] = await start_node(node_id)

                client = AsyncClusterBrokerClient(
                    StaticClusterConfig(
                        nodes=(
                            StaticClusterNode(1, ports[0], operations_ports[0]),
                            StaticClusterNode(2, ports[1], operations_ports[1]),
                            StaticClusterNode(3, ports[2], operations_ports[2]),
                        ),
                        timeout_seconds=1.0,
                    )
                )
                suffix = uuid4().hex
                session_id = f"python-async-cluster-session-{suffix}"
                namespace_id = f"python-async-cluster-{suffix}"
                owner_instance = "python-async-cluster-worker"

                leader = await self._wait_for_leader(
                    client, processes, timeout_seconds=20.0
                )
                self.assertIn(leader.node_id, {1, 2, 3})

                health = await client.health()
                self.assertEqual(health.protocol_version, 1)
                owner = await client.acquire_owner(
                    session_id=session_id,
                    expected_owner_epoch=1,
                    owner_instance_id=owner_instance,
                )
                self.assertEqual(owner.owner_epoch, 1)
                namespace = await client.ensure_namespace(
                    CommandIdentity(session_id, owner.owner_epoch, owner_instance, 1),
                    namespace_id=namespace_id,
                )
                self.assertEqual(namespace.namespace_id, namespace_id)

                initial_leader_id = leader.node_id
                await _stop_process(processes[initial_leader_id])
                failover_leader = await self._wait_for_leader(
                    client,
                    {
                        node_id: process
                        for node_id, process in processes.items()
                        if node_id != initial_leader_id
                    },
                    timeout_seconds=20.0,
                    excluded_node_id=initial_leader_id,
                )
                self.assertNotEqual(failover_leader.node_id, initial_leader_id)

                task_id = f"python-async-task-{suffix}"
                group_id = f"python-async-group-{suffix}"
                member_id = f"python-async-member-{suffix}"
                lease_id = f"python-async-lease-{suffix}"

                published = await client.publish_task(
                    CommandIdentity(session_id, owner.owner_epoch, owner_instance, 2),
                    namespace_id=namespace_id,
                    task_id=task_id,
                    objective="verify-python-async-sdk-three-node-lifecycle",
                )
                self.assertEqual(published.task_id, task_id)
                self.assertEqual(published.status, "queued")

                group = await client.ensure_group(
                    CommandIdentity(session_id, owner.owner_epoch, owner_instance, 3),
                    namespace_id=namespace_id,
                    group_id=group_id,
                )
                self.assertEqual(group.group_id, group_id)

                joined = await client.join_group(
                    CommandIdentity(session_id, owner.owner_epoch, owner_instance, 4),
                    group_id=group_id,
                    member_id=member_id,
                    capabilities=("python", "asyncio", "sdk"),
                )
                self.assertEqual(joined.member_count, 1)

                heartbeat = await client.heartbeat(
                    CommandIdentity(session_id, owner.owner_epoch, owner_instance, 5),
                    group_id=group_id,
                    member_id=member_id,
                    expected_generation=joined.generation,
                )
                claimed = await client.claim_task(
                    CommandIdentity(session_id, owner.owner_epoch, owner_instance, 6),
                    group_id=group_id,
                    member_id=member_id,
                    expected_term=heartbeat.term,
                    expected_generation=heartbeat.generation,
                    lease_id=lease_id,
                    lease_duration_ms=5_000,
                )
                self.assertEqual(claimed.task_id, task_id)
                self.assertIsNotNone(claimed.lease_epoch)

                renewed = await client.renew_task(
                    CommandIdentity(session_id, owner.owner_epoch, owner_instance, 7),
                    task_id=task_id,
                    group_id=group_id,
                    member_id=member_id,
                    expected_term=claimed.term,
                    expected_generation=claimed.generation,
                    expected_lease_epoch=claimed.lease_epoch,
                    lease_id=lease_id,
                    lease_duration_ms=5_000,
                )
                completed = await client.complete_task(
                    CommandIdentity(session_id, owner.owner_epoch, owner_instance, 8),
                    task_id=task_id,
                    group_id=group_id,
                    member_id=member_id,
                    expected_term=renewed.term,
                    expected_generation=renewed.generation,
                    expected_lease_epoch=renewed.lease_epoch,
                    lease_id=lease_id,
                    result="python-async-sdk-complete",
                )
                self.assertEqual(completed.status, "completed")

                processes[initial_leader_id] = await start_node(initial_leader_id)
                await self._wait_for_leader(client, processes, timeout_seconds=20.0)

                left = await client.leave_group(
                    CommandIdentity(session_id, owner.owner_epoch, owner_instance, 9),
                    group_id=group_id,
                    member_id=member_id,
                    expected_generation=heartbeat.generation,
                )
                self.assertEqual(left.member_count, 0)
            finally:
                async with asyncio.TaskGroup() as group:
                    for process in processes.values():
                        group.create_task(_stop_process(process))

    async def _wait_for_leader(
        self,
        client: AsyncClusterBrokerClient,
        processes: dict[int, asyncio.subprocess.Process],
        *,
        timeout_seconds: float,
        excluded_node_id: int | None = None,
    ) -> StaticClusterNode:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            exited = [
                (node_id, process.returncode)
                for node_id, process in processes.items()
                if process.returncode is not None
            ]
            if exited:
                self.fail(
                    f"three-node Broker process exited before readiness: {exited}"
                )
            try:
                leader = await client.discover_write_leader()
                if excluded_node_id is None or leader.node_id != excluded_node_id:
                    return leader
            except NoWriteReadyLeader:
                pass
            if asyncio.get_running_loop().time() >= deadline:
                self.fail(
                    "three-node Broker did not expose the expected write-ready leader in time"
                )
            await asyncio.sleep(0.05)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from uuid import uuid4

from agent_broker import (
    ClusterBrokerClient,
    CommandIdentity,
    NoWriteReadyLeader,
    StaticClusterConfig,
    StaticClusterNode,
)


@unittest.skipUnless(
    os.environ.get("AGENT_BROKER_RUN_CLUSTER_INTEGRATION") == "1",
    "set AGENT_BROKER_RUN_CLUSTER_INTEGRATION=1 with a real three-node cluster",
)
class RustClusterIntegrationTests(unittest.TestCase):
    def test_routes_owner_aware_write_to_verified_leader(self) -> None:
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

        reservations = [
            socket.socket(socket.AF_INET, socket.SOCK_STREAM) for _ in range(9)
        ]
        try:
            for reservation in reservations:
                reservation.bind(("127.0.0.1", 0))
            reserved_ports = tuple(
                reservation.getsockname()[1] for reservation in reservations
            )
        finally:
            for reservation in reservations:
                reservation.close()
        ports = reserved_ports[0:3]
        operations_ports = reserved_ports[3:6]
        raft_ports = reserved_ports[6:9]

        with tempfile.TemporaryDirectory(
            prefix="agent-broker-python-cluster-sdk-"
        ) as temp_dir:
            root = Path(temp_dir)
            tls_dir = root / "tls"
            subprocess.run(
                [
                    "cargo",
                    "run",
                    "-q",
                    "-p",
                    "agent-broker-consensus",
                    "--example",
                    "generate_cluster_tls",
                    "--",
                    str(tls_dir),
                    "1",
                    "2",
                    "3",
                ],
                cwd=repo,
                check=True,
            )

            raft_nodes = [
                item
                for node_id, raft_port in enumerate(raft_ports, start=1)
                for item in ("--raft-node", f"{node_id}=127.0.0.1:{raft_port}")
            ]

            def cluster_command(node_id: int, *, bootstrap: bool) -> list[str]:
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
                return command

            processes: dict[int, subprocess.Popen[bytes]] = {}
            try:
                for node_id in (2, 3, 1):
                    processes[node_id] = subprocess.Popen(
                        cluster_command(node_id, bootstrap=node_id == 1),
                        cwd=repo,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )

                client = ClusterBrokerClient(
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
                session_id = f"python-sdk-cluster-session-{suffix}"
                namespace_id = f"python-sdk-cluster-{suffix}"
                deadline = time.monotonic() + 20.0
                while True:
                    exited = [
                        process.returncode
                        for process in processes.values()
                        if process.poll() is not None
                    ]
                    if exited:
                        self.fail(
                            f"three-node Broker process exited before readiness: {exited}"
                        )
                    try:
                        leader = client.discover_write_leader()
                        break
                    except NoWriteReadyLeader:
                        if time.monotonic() >= deadline:
                            self.fail(
                                "three-node Broker did not expose one write-ready leader within 20 seconds"
                            )
                        time.sleep(0.05)
                self.assertIn(leader.node_id, {1, 2, 3})

                health = client.health()
                self.assertEqual(health.protocol_version, 1)
                owner = client.acquire_owner(
                    session_id=session_id,
                    expected_owner_epoch=1,
                    owner_instance_id="python-sdk-cluster-worker",
                    request_id="cluster-owner",
                )
                self.assertEqual(owner.owner_epoch, 1)
                identity = CommandIdentity(
                    session_id,
                    owner.owner_epoch,
                    "python-sdk-cluster-worker",
                    1,
                )
                namespace = client.ensure_namespace(
                    identity,
                    namespace_id=namespace_id,
                    request_id="cluster-namespace",
                )
                self.assertEqual(namespace.namespace_id, namespace_id)

                initial_leader_id = leader.node_id
                stopped_leader = processes[initial_leader_id]
                stopped_leader.terminate()
                try:
                    stopped_leader.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    stopped_leader.kill()
                    stopped_leader.wait(timeout=3)

                failover_deadline = time.monotonic() + 20.0
                while True:
                    try:
                        failover_leader = client.discover_write_leader()
                        if failover_leader.node_id != initial_leader_id:
                            break
                    except NoWriteReadyLeader:
                        pass
                    if time.monotonic() >= failover_deadline:
                        self.fail(
                            "Python SDK did not discover a new write-ready leader after leader stop"
                        )
                    time.sleep(0.05)
                self.assertNotEqual(failover_leader.node_id, initial_leader_id)

                task_id = f"python-sdk-task-{suffix}"
                group_id = f"python-sdk-group-{suffix}"
                member_id = f"python-sdk-member-{suffix}"
                lease_id = f"python-sdk-lease-{suffix}"

                published = client.publish_task(
                    CommandIdentity(
                        session_id, owner.owner_epoch, "python-sdk-cluster-worker", 2
                    ),
                    namespace_id=namespace_id,
                    task_id=task_id,
                    objective="verify-python-sdk-three-node-lifecycle",
                    request_id="cluster-publish",
                )
                self.assertEqual(published.task_id, task_id)
                self.assertEqual(published.status, "queued")

                group = client.ensure_group(
                    CommandIdentity(
                        session_id, owner.owner_epoch, "python-sdk-cluster-worker", 3
                    ),
                    namespace_id=namespace_id,
                    group_id=group_id,
                    request_id="cluster-group",
                )
                self.assertEqual(group.group_id, group_id)

                joined = client.join_group(
                    CommandIdentity(
                        session_id, owner.owner_epoch, "python-sdk-cluster-worker", 4
                    ),
                    group_id=group_id,
                    member_id=member_id,
                    capabilities=("python", "sdk"),
                    request_id="cluster-join",
                )
                self.assertEqual(joined.group_id, group_id)
                self.assertEqual(joined.member_count, 1)

                heartbeat = client.heartbeat(
                    CommandIdentity(
                        session_id, owner.owner_epoch, "python-sdk-cluster-worker", 5
                    ),
                    group_id=group_id,
                    member_id=member_id,
                    expected_generation=joined.generation,
                    request_id="cluster-heartbeat",
                )
                self.assertEqual(heartbeat.member_id, member_id)

                claimed = client.claim_task(
                    CommandIdentity(
                        session_id, owner.owner_epoch, "python-sdk-cluster-worker", 6
                    ),
                    group_id=group_id,
                    member_id=member_id,
                    expected_term=heartbeat.term,
                    expected_generation=heartbeat.generation,
                    lease_id=lease_id,
                    lease_duration_ms=5_000,
                    request_id="cluster-claim",
                )
                self.assertEqual(claimed.task_id, task_id)
                self.assertEqual(claimed.lease_id, lease_id)
                self.assertIsNotNone(claimed.lease_epoch)

                renewed = client.renew_task(
                    CommandIdentity(
                        session_id, owner.owner_epoch, "python-sdk-cluster-worker", 7
                    ),
                    task_id=task_id,
                    group_id=group_id,
                    member_id=member_id,
                    expected_term=claimed.term,
                    expected_generation=claimed.generation,
                    expected_lease_epoch=claimed.lease_epoch,
                    lease_id=lease_id,
                    lease_duration_ms=5_000,
                    request_id="cluster-renew",
                )
                self.assertEqual(renewed.task_id, task_id)
                self.assertEqual(renewed.lease_id, lease_id)

                completed = client.complete_task(
                    CommandIdentity(
                        session_id, owner.owner_epoch, "python-sdk-cluster-worker", 8
                    ),
                    task_id=task_id,
                    group_id=group_id,
                    member_id=member_id,
                    expected_term=renewed.term,
                    expected_generation=renewed.generation,
                    expected_lease_epoch=renewed.lease_epoch,
                    lease_id=lease_id,
                    result="python-sdk-complete",
                    request_id="cluster-complete",
                )
                self.assertEqual(completed.task_id, task_id)
                self.assertEqual(completed.status, "completed")

                processes[initial_leader_id] = subprocess.Popen(
                    cluster_command(
                        initial_leader_id, bootstrap=initial_leader_id == 1
                    ),
                    cwd=repo,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                restart_deadline = time.monotonic() + 20.0
                while True:
                    restarted = processes[initial_leader_id]
                    if restarted.poll() is not None:
                        self.fail(
                            f"restarted Broker {initial_leader_id} exited with {restarted.returncode}"
                        )
                    try:
                        client.discover_write_leader()
                        break
                    except NoWriteReadyLeader:
                        if time.monotonic() >= restart_deadline:
                            self.fail(
                                "cluster did not remain write-ready while stopped Broker restarted"
                            )
                        time.sleep(0.05)

                left = client.leave_group(
                    CommandIdentity(
                        session_id, owner.owner_epoch, "python-sdk-cluster-worker", 9
                    ),
                    group_id=group_id,
                    member_id=member_id,
                    expected_generation=heartbeat.generation,
                    request_id="cluster-leave",
                )
                self.assertEqual(left.group_id, group_id)
                self.assertEqual(left.member_count, 0)
            finally:
                for process in processes.values():
                    if process.poll() is None:
                        process.terminate()
                for process in processes.values():
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=3)


if __name__ == "__main__":
    unittest.main()

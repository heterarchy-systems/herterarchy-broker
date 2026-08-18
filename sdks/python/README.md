# Heterarchy Agent Broker Python SDK

This directory is an external client SDK for the Rust Agent Broker. It is **not** a Python Broker implementation and contains no consensus, persistence, state-machine, or server authority.

It is also **not** an FFI/PyO3 wrapper. Python never loads or executes the Rust Broker implementation in-process:

```text
Python application
        |
        v
agent_broker Python SDK
        |
        v
TCP / Broker Protocol
        |
        v
Rust agentbrokerd
```

The Broker Protocol is the source of truth. This package is an optional typed convenience/safety layer for callers; `agentbrokerd` does not depend on the Python SDK.

The SDK intentionally mirrors the Rust protocol/client safety boundary:

- standalone mode uses protocol-v1 `health` plus one-attempt mutations with no automatic retry;
- cluster mode uses owner-aware protocol-v3 mutations;
- `CommandIdentity` is caller-owned and explicitly carries command session, owner epoch, owner instance, and sequence;
- the SDK never auto-increments a command sequence or performs automatic owner takeover;
- optional retries resend the exact same serialized frame only after transport failure or a v3 `UNKNOWN` disposition;
- `COMMITTED`, `REJECTED`, and `UNKNOWN` are exposed as typed error dispositions.
- synchronous and native-`asyncio` clients are both supported; async networking uses `asyncio` streams directly and does not wrap the synchronous socket client in worker threads.
- `BrokerOperationsClient` and `AsyncBrokerOperationsClient` use the separate read-only operations endpoint for authoritative Consumer Group discovery without mutating Broker state.

## Local use

```bash
cd sdks/python
uv sync --locked --dev
```

```python
from agent_broker import StandaloneBrokerClient

with StandaloneBrokerClient() as client:
    print(client.health())
    result = client.ensure_namespace(
        namespace_id="example",
    )
    print(result)
```

Standalone protocol-v1 mutations are sent exactly once. The SDK does not automatically retry an ambiguous standalone mutation.

Broker-authoritative Group inspection is intentionally separate from mutation clients:

```python
from agent_broker import BrokerOperationsClient

operations = BrokerOperationsClient()
page = operations.list_groups(limit=8)
company = operations.describe_group("backend-company")
print(page.groups)
print(company.group.consumer_count)
```

Standalone defaults to Broker protocol port `8811` and operations port `8812`. `list_groups()` is
bounded and cursor-based; cluster deployments only return authoritative Group reads from a current
leader that can prove quorum/linearizable read authority.

For a three-node cluster, use `ClusterBrokerClient` (alias of `StaticClusterBrokerClient`) with explicit owner acquisition and `CommandIdentity`. `BrokerClient` is the lower-level direct-node client; it does not perform cluster discovery. A caller that wants to issue sequence `2` must persist/decide that transition itself after interpreting the prior outcome. The SDK deliberately does not hide that durability boundary.

```python
from agent_broker import (
    ClusterBrokerClient,
    CommandIdentity,
    StaticClusterConfig,
    StaticClusterNode,
)

client = ClusterBrokerClient(
    StaticClusterConfig(
        nodes=(
            StaticClusterNode(1, 8811, 8812),
            StaticClusterNode(2, 8821, 8822),
            StaticClusterNode(3, 8831, 8832),
        )
    )
)

leader = client.discover_write_leader()
owner = client.acquire_owner(
    session_id="my-session",
    expected_owner_epoch=1,
    owner_instance_id="worker-a",
)
namespace = client.ensure_namespace(
    CommandIdentity("my-session", owner.owner_epoch, "worker-a", 1),
    namespace_id="project-a",
)
```

Static cluster discovery is intentionally fail-closed: the SDK requires exactly one `operations-v1` endpoint to report identity-consistent write readiness. Dynamic membership and server-driven redirect metadata are outside this SDK milestone.

## Native asyncio

The async surface is exported from the package root and from the convenience namespace
`agent_broker.aio`. Both import paths resolve to the same canonical native-asyncio direct,
standalone, and static-cluster implementations; there is no duplicate transport stack and no
synchronous-client thread wrapper hidden behind these APIs.

```python
from agent_broker import (
    AsyncClusterBrokerClient,
    CommandIdentity,
    StaticClusterConfig,
    StaticClusterNode,
)

client = AsyncClusterBrokerClient(
    StaticClusterConfig(
        nodes=(
            StaticClusterNode(1, 8811, 8812),
            StaticClusterNode(2, 8821, 8822),
            StaticClusterNode(3, 8831, 8832),
        )
    )
)

leader = await client.discover_write_leader()
owner = await client.acquire_owner(
    session_id="my-session",
    expected_owner_epoch=1,
    owner_instance_id="worker-a",
)
namespace = await client.ensure_namespace(
    CommandIdentity("my-session", owner.owner_epoch, "worker-a", 1),
    namespace_id="project-a",
)
```

`AsyncBrokerClient` and `AsyncStandaloneBrokerClient` use the same protocol contracts for direct-node and standalone mode. The async implementation uses native `asyncio.open_connection()`, bounded newline framing, `StreamWriter.drain()` backpressure, one end-to-end monotonic deadline, and request-owned TCP connections. It does not use `asyncio.to_thread()`, an executor, or a private socket worker thread for network I/O.

Cancellation is deliberately not translated into success or a rejected mutation. If cancellation or a deadline happens after mutation bytes may have reached the Broker, the caller must treat the outcome as potentially ambiguous. Protocol-v3 retries reuse the exact serialized frame/command identity; standalone protocol-v1 still performs no automatic mutation retry.

## Tests

Dependency-free unit tests:

```bash
PYTHONPATH=sdks/python/src python3 -m unittest discover -s sdks/python/tests -p 'test_*.py' -v
```

Actual Rust standalone integration:

```bash
AGENT_BROKER_RUN_RUST_INTEGRATION=1 \
PYTHONPATH=sdks/python/src \
python3 -m unittest discover -s sdks/python/tests -p 'test_rust_integration.py' -v
```

Actual self-hosted Rust three-node mTLS integration:

```bash
AGENT_BROKER_RUN_CLUSTER_INTEGRATION=1 \
PYTHONPATH=sdks/python/src \
python3 -m unittest discover -s sdks/python/tests -p 'test_cluster_integration.py' -v
```

Native-asyncio focused tests plus real Rust restart/failover integration:

```bash
PYTHONPATH=sdks/python/src \
python3 -m unittest -v sdks/python/tests/test_async_sdk.py

AGENT_BROKER_RUN_ASYNC_RUST_INTEGRATION=1 \
AGENT_BROKER_RUN_ASYNC_CLUSTER_INTEGRATION=1 \
PYTHONPATH=sdks/python/src \
python3 -m unittest -v sdks/python/tests/test_async_integration.py
```

The async integration test uses native asyncio subprocess and TCP APIs. It proves standalone restart recovery and a self-hosted three-node mTLS lifecycle with current-leader stop, new-leader discovery, continued owner-aware task/group/lease work, and restart of the stopped Broker.

The cluster integration test owns an ephemeral loopback three-node `agentbrokerd` cluster and generated Raft mTLS fixture. It verifies leader discovery and owner acquisition, stops the currently discovered leader, waits for operations-v1 to prove one new write-ready leader, continues the same owner epoch with the next exact command sequence, completes the namespace/task/group/join/heartbeat/claim/renew/complete/leave lifecycle, and restarts the stopped Broker on its original durable state path.

## Quality and package validation

The locked development toolchain targets Python 3.11 syntax and currently pins Ruff and Pyrefly without adding runtime dependencies:

```bash
cd sdks/python
uv sync --locked --dev
uv run ruff check src tests
uv run ruff format --check src tests
uv run pyrefly check
uv build
```

Before publishing, smoke-test both generated distributions from an isolated environment rather than importing the working tree. Releases from GitHub Actions use PyPI Trusted Publishing/OIDC; the package does not require a long-lived PyPI API token in repository secrets.

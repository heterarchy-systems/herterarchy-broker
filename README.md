<p align="center">
  <img src="./docs/assets/h_broker.png" alt="HETERARCHY Broker" width="100%" />
</p>

<p align="center">
  <a href="https://www.rust-lang.org/"><img alt="Rust" src="https://img.shields.io/badge/Rust-1.97-000000?logo=rust&logoColor=white"></a>
  <a href="https://www.python.org/"><img alt="Python SDK" src="https://img.shields.io/badge/Python%20SDK-3.11%2B-3776AB?logo=python&logoColor=white"></a>
  <a href="https://github.com/heterarchy-systems/herterarchy-broker/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/heterarchy-systems/herterarchy-broker/actions/workflows/ci.yml/badge.svg"></a>
  <img alt="License" src="https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-yellow.svg">
</p>

# herterarchy-broker

`herterarchy-broker` is the coordination broker for the HETERARCHY platform.

It provides a durable, provider-independent place for agents and runtimes to publish work, join consumer groups, claim tasks, renew leases, complete work, and recover safely after failures.

The Broker does **not** know about ChatGPT, Claude, Codex, browser sessions, model names, prompts, or provider-specific state. Those systems are clients. The Broker owns coordination authority.

```text
Agent / Runtime
      │
      ▼
Client SDK / Protocol
      │
      ▼
herterarchy-broker
      │
      ├─ task coordination
      ├─ consumer groups
      │    └─ one GroupCoordinator manages the Broker-wide group registry
      ├─ leases and fencing
      ├─ durable recovery
      └─ standalone / 3-node Raft
```

Consumer Groups are independent provider-neutral Agent Company boundaries. The authoritative
group/Consumer state lives in the replicated Broker state, while one stateless
`GroupCoordinator` subsystem selects groups by `group_id` and applies Consumer lifecycle operations
such as join, heartbeat, leave, stale-Consumer reap, and current-Consumer validation. There is no
separate coordinator process per group and no second group-state authority.

Group registration itself remains a `BrokerStateMachine` cross-entity transition because it is
coupled to namespace capacity accounting and the global Broker revision. Once registered,
Consumer control is routed through `GroupCoordinator`; Consumer removal and Task lease requeue
remain one deterministic Broker transition.

## Why this exists

HETERARCHY is built around autonomous systems that should be able to cooperate without collapsing into one central runtime.

The Broker gives those systems a small shared coordination layer with explicit boundaries:

- **Provider independence** — any runtime can participate through the protocol.
- **Durable work** — accepted coordination state survives process restarts.
- **Safe ownership** — leases, generations, epochs, and terms fence stale workers.
- **Failure recovery** — retries and ambiguous outcomes are handled explicitly instead of being guessed away.
- **Replaceable clients** — Rust, Python, CLI, chat, or future runtimes can share the same Broker authority.

The goal is not to build another agent framework. The goal is to provide a reliable coordination primitive that other agent systems can compose around.

## Quick start

### Run a standalone Broker

Requirements:

- Rust 1.97+

Start the Broker:

```bash
cargo run -p agent-broker-runtime --bin agentbrokerd -- serve \
  --host 127.0.0.1 \
  --port 8811
```

Standalone also exposes the separate read-only `operations-v1` endpoint on `127.0.0.1:8812` by
default. It provides bounded authoritative Group directory reads such as `describe_group` and
`list_groups`; it is not a mutation endpoint.

Check that it is healthy:

```bash
cargo run -q -p agent-broker-runtime --bin agentbrokerd -- \
  health --host 127.0.0.1 --port 8811
```

### Run with Docker

```bash
make docker_build
make docker_up
```

Stop it with:

```bash
make docker_down
```

### Run the three-node development cluster

```bash
make docker_cluster_up
make docker_cluster_smoke
```

The client endpoints are:

```text
node 1  127.0.0.1:8811
node 2  127.0.0.1:8812
node 3  127.0.0.1:8813
```

Stop the cluster with:

```bash
make docker_cluster_down
```

## Python SDK

A dependency-free Python client SDK lives in [`sdks/python`](./sdks/python).

It is a client for the Rust Broker, not a Python Broker implementation. Both synchronous and native-`asyncio` APIs are provided.

Local development:

```bash
cd sdks/python
uv sync --locked --dev
uv run python -m unittest -v tests.test_sdk tests.test_async_sdk tests.test_operations_sdk
```

See [`sdks/python/README.md`](./sdks/python/README.md) for the SDK API and examples.

## Repository layout

```text
crates/
  agent-broker-application/   coordination use cases and state transitions
  agent-broker-client/        Rust client and routing
  agent-broker-consensus/     OpenRaft integration and cluster transport
  agent-broker-protocol/      versioned wire protocol
  agent-broker-runtime/       agentbrokerd and network runtime

sdks/python/                  Python client SDK
compatibility/                frozen compatibility evidence
fuzz/                         protocol and persistence fuzz targets
xtask/                        repository checks and release gates
```

## Development

Run the canonical repository gate before submitting changes:

```bash
make ci
```

Useful focused checks:

```bash
cargo xtask rules
cargo xtask check
cargo xtask test
cargo xtask doctor
```

Real multi-process and three-node failure tests are intentionally kept as local/release evidence rather than ordinary pull-request CI gates.

## Contributing

Contributions are welcome.

Before opening a pull request:

1. Keep provider-specific concepts outside the Broker core.
2. Preserve the existing protocol and durability boundaries unless the change explicitly evolves them.
3. Add focused tests for behavior changes, especially around retries, ownership, persistence, or fencing.
4. Run `make ci` and keep the working tree free of generated artifacts.
5. Explain the problem being solved and the failure modes considered in the pull request.

For larger architectural changes, opening an issue or discussion first is preferred so the authority boundary can be agreed on before implementation.

## License

Rust workspace crates are available under `MIT OR Apache-2.0`. The Python SDK declares `Apache-2.0`.

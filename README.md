# Agent Broker

`herterarchy-broker/` is the standalone, provider-independent work Broker runtime for ChatToCodex and future agent runtimes.

It deliberately does not know about ChatGPT, Chrome, Claude, Kimi, Codex CLI, model names, prompt delivery, OAuth, or browser/session state. Provider runtimes are Broker clients. The Broker owns authoritative work-coordination state.

## Current milestone

The Agent Broker production authority is now **Rust-only**. Rust owns `agentbrokerd`, protocol/runtime implementation, persistence, one-node OpenRaft consensus, the initial three-node OpenRaft cluster path, process E2E, production CI, release-performance gates, container packaging, and fuzz targets. Language SDKs remain external typed clients and do not own Broker authority.

The former CPython reference implementation was retired on 2026-08-16 only after the complete retirement gate passed: Python reference CI, cross-language hard-kill/restart E2E, Rust tests/Clippy/performance, and nightly cargo-fuzz for protocol/snapshot/journal.

The former Python Broker source/package/tests/benchmarks are no longer present. A separate dependency-free Python **client SDK** now lives under `sdks/python/`; it talks to the Rust Broker over the versioned client/operations boundaries and contains no consensus, persistence, authoritative state machine, Raft peer, or Broker server implementation. Language-neutral migration evidence remains under `compatibility/`, immutable seed inputs remain under `fuzz/seeds/`, and deletion identities are recorded under `compatibility/retirement/`.

Implemented now:

- `agentbrokerd serve` Rust standalone process.
- Loopback-only, strictly versioned NDJSON protocol. Frozen protocol-v1 remains byte-compatible; protocol-v2 adds caller-owned command session/sequence retry identity; protocol-v3 adds broker-authoritative owner acquisition plus owner-aware mutation without widening v1/v2.
- Typed synchronous Rust `BrokerClient` with one lazy reusable TCP connection, manual identified/owner acquisition APIs, explicit durable session state, and opt-in bounded automatic exact retry. No hidden identity generation or automatic owner takeover occurs. The `agent-broker-client` crate also has an opt-in `async` feature exposing native-Tokio `AsyncBrokerClient`, `AsyncStaticClusterRouter`, and `AsyncDurableClientSessionStore`; the network path uses `tokio::net` rather than wrapping the synchronous socket client, while durable filesystem locking/fsync is isolated behind an admission-bounded `spawn_blocking` boundary. A real three-process mTLS E2E uses `AsyncStaticClusterRouter` to discover the leader, preserve one owner epoch across a hard leader stop and re-election, continue with the next command sequence, restart the stopped Broker on its original durable state, and commit again after rejoin.
- Static-three-node Rust `StaticClusterRouter` probes bounded `operations-v1` readiness, accepts exactly one identity-verified write-ready leader, fails closed on zero/multiple/mismatched leaders, and re-discovers before bounded exact-frame retry on transport/`UNKNOWN` outcomes. Its async counterpart preserves the same authority and exact-frame rules with concurrent read-only readiness probes and request-owned Tokio connections.
- Dependency-free Python client SDK under `sdks/python/`: synchronous and native-`asyncio` standalone protocol-v1 calls plus static-three-node operations-v1 discovery and owner-aware protocol-v3 mutations. Async networking uses `asyncio.open_connection()` streams directly rather than thread/executor wrappers. Caller-owned session/owner/sequence identity is never silently advanced. Real-process async tests cover standalone restart recovery and a self-hosted three-node mTLS leader-stop/failover/rejoin lifecycle from namespace/task/group membership through heartbeat, claim, renewal, completion, and leave.
- Protocol-v3 can bootstrap a new command session directly at owner epoch 1 without a seed business mutation. Acquiring an existing legacy epoch-1 session with a different owner instance advances to epoch 2 and fences the legacy writer.
- Restart-safe `DurableClientSessionStore` uses an exclusive local process lock and strict versioned JSON. Owner acquisition plus exact owner-aware mutation identity/request are atomically fsynced before network submission; unresolved in-flight state survives process death and blocks takeover until explicitly recovered and acknowledged.
- Durable-client hard-kill E2E waits for a real backend response-complete proxy barrier, kills the client process before local outcome acknowledgment, reopens the store, exact-retries without increasing Broker revision, then allows a new owner epoch and sequence-1 takeover only after recovery.
- Protocol-v3 mutation errors carry commit-aware `COMMITTED | REJECTED | UNKNOWN` disposition while v1/v2 remain byte/schema frozen. Domain business errors are committed session outcomes; owner/epoch/sequence/content pre-check failures are rejected before outcome storage; ambiguous post-submit/transport outcomes remain unknown.
- Identified retry equality is based on client-stable semantic request content. Server-observed timestamps and server-local capacity limits remain authoritative in the first committed Raft entry but do not turn an exact client retry into a conflict; changed client semantic fields still conflict.
- `DurableRetryPolicy` retries only exact persisted owner/session/sequence/request on transport or `UNKNOWN`, always with an explicit positive attempt bound. `COMMITTED` errors durably advance the local sequence, `REJECTED` errors release in-flight without consuming it, and retry exhaustion preserves the exact in-flight record for later recovery.
- Namespace, Task, Consumer Group, Member, heartbeat, leave, claim, lease renewal, completion, and retention lifecycles.
- Fencing by Broker term, Consumer Group generation, lease epoch, lease ID, ownership, and lease expiry.
- Incremental append/flush/fsync journal plus periodic same-directory tempfile + fsync + atomic-replace snapshots.
- Strict startup snapshot+journal replay, final torn-tail repair, middle/semantic corruption fail-stop, and bounded deferred compaction.
- Standalone `ConsensusAdapter` fail-stop poisoning after durability failure.
- Real one-node OpenRaft 0.9.25 consensus with redb-backed Raft log, vote, committed pointer, purge metadata, and snapshot persistence.
- One-node `client_write -> durable Raft log -> 1/1 commit -> existing BrokerStateMachine apply -> response` semantics; no second Broker business state machine.
- Dedicated controller OS thread + Tokio/OpenRaft runtime, with redb blocking I/O kept behind `spawn_blocking` boundaries.
- One-node parity, snapshot/restart, stale-term fencing, and ACK -> hard-process-kill -> reopen durability E2E; valid one-node operation emits zero remote RPC attempts.
- Static three-node `ClusterRaftConsensusAdapter` using the same existing `BrokerStateMachine`, redb Raft storage, and committed-entry-only application semantics.
- Bounded internal TCP Raft RPC for append, vote, and full-snapshot transfer; initial membership converges through one bootstrap voter -> learners -> voters `{1,2,3}`. Cluster Raft transport is mandatory TLS 1.3 mTLS with one trusted cluster CA, per-node certificates, target DNS identity verification, and exact configured `node_id` -> leaf-certificate pinning; there is no plaintext cluster fallback.
- Three-node replication/follower-write rejection tests plus leader shutdown -> 2/3 re-election -> new-term write -> old-node rejoin/catch-up tests.
- Forced snapshot catch-up test stops a follower, advances the majority until the leader snapshots and purges beyond that follower's last available log, then proves the rejoined follower installs a newer snapshot and converges to the exact committed Broker revision; truncated incoming snapshot bytes are rejected before durable or in-memory authoritative state advances.
- Durable-corruption tests cover both semantic Raft metadata corruption and physically invalid redb bytes: the affected node fails closed on repeated startup attempts without silently reinitializing durable state, while the surviving 2/3 quorum continues to commit.
- Full-snapshot retry E2E uses a TLS-opaque TCP proxy that never terminates TLS or parses Raft JSON. It cuts only a snapshot-sized encrypted client burst mid-transfer; OpenRaft retries on a new mTLS connection and the follower converges through a newer snapshot.
- Raft RPC ingress uses eight fixed workers behind a bounded 64-connection queue. A real slow-handshake/slow-peer test proves a separate quorum RPC to the same follower still commits without head-of-line blocking.
- Accepted Raft sockets are explicitly normalized before bounded connect/TLS-handshake/read/write deadlines are applied. The mTLS saturation test holds exactly 8 active + 64 queued sockets before ClientHello, rejects excess connections, drains, and then proves normal cluster progress; invalid plaintext is rejected by the TLS boundary rather than being treated as a Raft frame.
- Repeated leader restart E2E performs three live cycles of current-leader shutdown, surviving-majority commit, durable node reopen/rejoin, and exact idempotent command retry without an extra Broker revision.
- Broker state-owner reply channels are bounded one-shot channels, and accepted client sockets have bounded read/write I/O timeouts instead of allowing idle clients to hold connection slots forever.
- Client-facing state-owner saturation is explicit fail-fast backpressure rather than a blocking socket-worker backlog. An observed active=1/queued=1 test keeps the queue full while 64 consecutive protocol-v3 overload requests return `CAPACITY_EXCEEDED` with `REJECTED`, then drains and proves normal mutation progress. A real three-node churn test also completes 256 fresh connect/health/close cycles across eight threads before a new quorum mutation converges on both followers.
- `cargo xtask ci` also enforces `git diff --check` and protects frozen `compatibility/**` plus `fuzz/seeds/**`; repository-local cargo-deny policy is version-controlled in `deny.toml` even when the optional `cargo-deny` binary is not installed.
- Retry-safe cluster mutation uses durable command session + monotonic sequence state with exact committed-response recovery and explicit `COMMIT_OUTCOME_UNKNOWN` after post-submit response deadlines. Protocol-v2 TCP E2E proves response loss followed by same-identity recovery without an extra business revision.
- Broker-authoritative command-session ownership uses `SessionOwnerEpoch + SessionOwnerInstanceId`. Acquisition is a Raft-committed compare-and-advance operation; retrying the same acquisition after response loss returns the already committed epoch, stale contenders and same-epoch wrong-instance writes are fenced, and the ownership state survives snapshots and leader changes.
- Protocol-v3 exposes that ownership explicitly. A deterministic response-drop proxy waits until the Broker has generated the acquisition response, drops only the client-facing response, and then proves the same owner instance recovers the committed epoch and performs owner-aware exact retries without duplicate business state.
- Cluster timing defaults use heartbeat 100ms and randomized election 1000–2000ms. Docker partition E2E also bounds controlled election churn to a term delta of at most 5; repeated post-tuning runs have advanced term 1 -> 2.
- Docker fault E2E harness keeps Broker client access alive while isolating the old leader from the Raft-only network and is wired to generated read-only Raft mTLS credentials. The current mTLS Compose configuration validates, but the destructive container fault sequence still requires a fresh approved run before current-tree container-level failover/partition/heal evidence is claimed.
- Hardened Docker image plus standalone and three-node Compose topologies; containers run as UID/GID 10001 with a read-only root filesystem, dropped capabilities, dedicated persistent volumes, and read-only mounted Raft mTLS credentials.
- Process ownership lock for one standalone Broker per state path.
- Bounded frame size, connection count, in-flight request count, namespace/task/group/member hot-state capacity, maintenance batch sizes, and retained completed Tasks.
- Single-owner state thread; socket workers never share the mutable state machine through a mutex.
- The bounded state-owner queue exposes read-only active/queued/capacity load. Queue saturation is fail-fast instead of blocking connection threads: protocol-v3 reports `CAPACITY_EXCEEDED` with `REJECTED`, repeated overload leaves queue depth bounded, drain restores progress, and a real three-node E2E survives 256 client connect/health/close churn cycles before another quorum mutation converges.
- Heap/index-backed ready Tasks, lease expirations, completed-task retention, active lease IDs, and per-member leased Task lookup rebuilt deterministically from checkpoints.
- Standalone maintenance for stale-member reaping and completed-task pruning.
- Cluster mode uses the same bounded maintenance policy through a leader-gated state-owner path: followers skip before submitting mutations, the active leader commits reap/prune batches through OpenRaft, and maintenance authority follows leader failover while every batch still passes the normal Raft leader/term write gate.
- Real TCP lifecycle E2E and hard-process-kill/restart durability E2E.
- Strict byte-level protocol/storage conformance against the frozen migration corpora.
- Nightly cargo-fuzz targets for protocol v1, protocol v3, snapshot v1, and journal v1; frozen migration golden seeds remain immutable while protocol-v3 fuzzing uses only ignored mutable corpus data.
- Outbound Raft peer connect is explicitly bounded: static initial-cluster peers must be pre-resolved IP `SocketAddr` values, ordinary and snapshot RPCs share `TcpStream::connect_timeout`, the default deadline is 1s with a validated 30s maximum, and Docker Raft peers use fixed internal bridge IPs so DNS/getaddrinfo is not part of the transport hot path.
- Raft TLS handshake is independently bounded: default 2s with a validated 30s maximum. Both directions require certificates from the configured cluster CA, client verifies `node-{id}.agent-broker.internal`, and the transport exact-matches the authenticated leaf against the configured peer `node_id` before dispatching any Raft RPC.
- Cluster mode exposes a separate bounded read-only `operations-v1` TCP surface on `--operations-port` (default `8812`). `liveness` reports Broker listener lifecycle without probing quorum; `readiness`/`status` combine Broker listener load, `StateOwnerLoad`, OpenRaft term/leader/commit/applied/membership/snapshot progress, Raft RPC load, and maintenance authority. A leader is write-ready only after `OpenRaft::ensure_linearizable` confirms current-term quorum authority; followers, quorum loss, stale isolated leaders, fatal/unavailable consensus, and state-owner saturation all fail readiness closed with machine-readable reasons.

Not implemented / not production-hardened yet:

- Dynamic membership changes beyond the fixed initial three-node topology.
- Dynamic-membership-aware discovery and server-driven redirect metadata. Static initial-three-node client discovery/routing through `operations-v1` is implemented in both the Rust client router and Python cluster SDK.
- Disk-backed resumable snapshot fetching is not implemented. The current OpenRaft `Cursor<Vec<u8>>` type still requires one full receiver destination buffer, but snapshot payload bytes are no longer embedded in JSON or cloned into a second full sender buffer: metadata uses a small JSON header and the body streams in 64 KiB binary chunks under a 256 MiB bound. `cargo xtask perf` enforces a 32 MiB synthetic transfer RSS delta budget of 64 MiB, and the real three-node cut/retry E2E interrupts the binary body mid-transfer.

`agentbrokerd doctor --nodes 3` still validates quorum math only. Real local three-node bootstrap, replication, leader re-election, rejoin, mandatory mTLS, TLS-opaque snapshot retry, plaintext rejection, readiness, corruption fail-stop, bounded-resource faults, static safe client leader discovery/routing, and Python SDK leader-stop/restart continuation have focused evidence. Release/performance gates have fresh passing evidence, while the current mTLS Docker fault sequence still awaits an approved destructive run. The repository also does **not** claim complete release/supply-chain verification when optional tooling such as `cargo-deny` is unavailable, and dynamic membership remains intentionally outside the supported static topology.

## Architecture boundary

```text
Producer / Orchestrator
        |
        v
   BrokerClient
        |
        v
+---------------------------+
| agentbrokerd              |
|                           |
| TCP Protocol / Dispatcher |
|            |              |
| Application Service       |
|            |              |
| ConsensusAdapter          |
|            |              |
| BrokerStateMachine        |
|            |              |
| Journal + Snapshot        |
+---------------------------+
        |
        v
Consumer Groups / Workers
```

Standalone mode uses `StandaloneConsensusAdapter`, one-node Raft mode uses `OneNodeRaftConsensusAdapter`, and the initial three-node mode uses `ClusterRaftConsensusAdapter`. All three apply the same typed commands to the same deterministic `BrokerStateMachine`. OpenRaft may apply application data only after its log entry is committed; multi-node support extends the consensus boundary instead of forking Broker domain semantics.

### Python SDK boundary

The Python SDK under `sdks/python/` is an optional client convenience and safety layer. It is not an FFI/PyO3 wrapper and does not execute Rust inside the Python process:

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

The Broker Protocol remains the source of truth. The Python and Rust SDKs expose that protocol with typed ownership, fencing, retry, and ambiguity semantics; neither SDK is required to run the Broker itself.

### Cluster operations-v1

`serve-cluster` keeps operational observation separate from frozen Broker protocol v1/v2/v3. The operations listener accepts one bounded newline-delimited JSON request per connection:

```json
{"schema_version":1,"operation":"liveness"}
```

`operation` may be `liveness`, `readiness`, or `status`; extra fields and mutation-like operations are rejected. The listener is loopback-only by default, shares the explicit container-bridge exposure policy used by cluster mode, caps request/response frames and concurrent connections, and has bounded per-connection I/O timeouts. Operations data is derived from existing read-only observations and never becomes assignment, fencing, maintenance, or consensus authority.

## Rust workspace

The Edition 2024 resolver-3 workspace includes:

- `agent-broker-domain` — deterministic authoritative Broker state machine and invariants.
- `agent-broker-application` — typed use cases above `ConsensusAdapter`.
- `agent-broker-protocol` — strict frozen protocol-v1 codec, retry-identified protocol-v2, owner-aware protocol-v3, and shared dispatch.
- `agent-broker-storage` — schema-v1 snapshot/journal codec, replay, durability, repair, and compaction.
- `agent-broker-consensus` — standalone commit ordering, one-node OpenRaft/redb, and static three-node TCP OpenRaft replication/bootstrap/failover adapters.
- `agent-broker-client` — typed reusable synchronous Rust client plus optional native-Tokio async client/router/durable-store facade and process-exclusive, atomic-fsync durable protocol-v3 session state.
- `agent-broker-runtime` — process lock, state-owner thread, Broker TCP runtime, bounded operations-v1 runtime, maintenance, and `agentbrokerd`.
- `xtask` — repository-local policy, CI, performance, fuzz, and tooling gates.
- isolated `fuzz/` workspace — protocol/snapshot/journal libFuzzer targets.

Workspace policy keeps `unsafe_code = "forbid"` and strict Clippy warnings. Mature libraries are preferred over bespoke primitives unless a measured requirement justifies custom code.

## Coordination and fencing

The Broker owns Namespace identity, Task publication/state, Consumer Group membership/generations, heartbeat/leave, lease claim/renew/expiry/requeue/completion, Broker term, revisions, stale-member recovery, retention/pruning, and hot-state capacity/backpressure.

A Worker completion is accepted only when all authoritative fencing values still match:

```text
Broker term
+ Consumer Group generation
+ group/member ownership
+ lease ID
+ lease epoch
+ lease not expired
```

Lease renewal extends expiry without changing the lease epoch. Reassignment after expiration increments the epoch and permanently fences the previous holder.

## Durability

Standalone authoritative writes use the Rust journaled repository. Each state mutation produces a typed change set; changed entities are appended to the journal, flushed, and POSIX-fsynced before acknowledgement.

Compaction writes a full snapshot through same-directory temporary file + fsync + atomic replacement, then durably truncates the journal. Recovery supports snapshot plus later journal replay, stale pre-compaction records, monotonic term/revision validation, torn final-record truncation, and Task tombstones. Middle or semantic corruption fails closed.

On Apple platforms the durability path uses the safe `rustix::fs::fsync` wrapper to preserve the POSIX-fsync contract without introducing unsafe code.

## Bounded hot state

Default limits are explicit and conservative:

```text
Namespaces                    64
Retained Tasks / Namespace  4096
Consumer Groups / Namespace   64
Members / Consumer Group     256
TCP connections              256
In-flight requests            64
Protocol frame              128 KiB
Completed Task retention      24 h
Member heartbeat timeout      45 s
```

These are standalone defaults, not distributed-cluster sizing promises. Exact idempotent retries remain valid when capacity is full; genuinely new resources receive `CAPACITY_EXCEEDED`.

## Frozen migration evidence

The removed Python implementation is not required at runtime or in CI. Its externally observable contract is frozen as language-neutral fixtures:

- `compatibility/wire-v1/` — request/response NDJSON bytes.
- `compatibility/storage-v1/` — snapshot/journal bytes.
- `fuzz/seeds/` — immutable seed files mechanically aligned with those compatibility corpora.
- `compatibility/retirement/python-reference-files.sha256` — identities of deleted Python source/package/test/benchmark files.

`cargo xtask extended` verifies the frozen bytes before fuzzing and verifies again after fuzzing that cargo-fuzz did not mutate the golden seed directories. Mutable coverage-discovered inputs live only under ignored `fuzz/corpus/`.

## Performance regression gates

`cargo xtask perf` is the canonical release-profile regression gate. Current repository budgets are:

- cold start <= 750 ms,
- idle RSS <= 32 MiB,
- publish >= 100k ops/s,
- claim + complete >= 50k pairs/s,
- POSIX-fsync durable publish >= 2k ops/s,
- one-node OpenRaft committed write >= 50 ops/s,
- one-node OpenRaft committed write p99 <= 50 ms,
- protocol p99 <= 5 ms,
- snapshot install <= 250 ms,
- recovery <= 250 ms,
- bounded queue saturation <= 1000 ms.

These are regression budgets, not machine-independent performance promises.

## Docker and Docker Hub

The same production image can run either the standalone server or one member of the current static three-node cluster. The image is a Rust 1.97 multi-stage build with a Debian slim runtime, a non-root UID/GID `10001:10001`, native `agentbrokerd health` healthcheck, persistent `/var/lib/agent-broker`, and a read-only root filesystem when launched through the supplied Compose files.

Standalone container:

```bash
make docker_build
make docker_up

# Published only on the host loopback interface by default.
cargo run -q -p agent-broker-runtime --bin agentbrokerd -- \
  health --host 127.0.0.1 --port 8811

make docker_down
```

Three-node development cluster:

```bash
make docker_cluster_up

# Client protocol ports. Internal Raft 18811 is not published to the host.
# node 1 -> 127.0.0.1:8811
# node 2 -> 127.0.0.1:8812
# node 3 -> 127.0.0.1:8813

make docker_cluster_smoke
make docker_cluster_e2e
make docker_cluster_down
```

`compose.cluster.yaml` starts nodes 2 and 3 first, waits for their protocol healthchecks, then starts bootstrap node 1. Broker client traffic and Raft peer traffic use separate Compose networks. Raft uses the internal-only `raft` network with fixed peer IPs (`10.77.0.11:18811`, `10.77.0.12:18811`, `10.77.0.13:18811`); service aliases may exist for container diagnostics, but the Raft configuration does not depend on Docker DNS. The Make targets generate/retain an ignored development TLS fixture under `target/`, while `make docker_cluster_e2e` creates one unique ephemeral CA plus node certificates for the entire run, mounts that directory read-only into all three containers, and removes it at final cleanup. Only Broker client/operations ports are published on host loopback. Each node has its own durable volume. This separation lets the E2E isolate a live old leader from Raft peers without making its client endpoint disappear.

To publish a signed-metadata-capable multi-platform image to Docker Hub, authenticate first and provide the namespace explicitly:

```bash
docker login

make docker_hub_push \
  DOCKERHUB_NAMESPACE=<dockerhub-user-or-org> \
  DOCKERHUB_REPOSITORY=herterarchy-broker \
  DOCKER_TAG=0.1.0
```

The multi-arch check and push targets use a dedicated `docker-container` Buildx builder (`herterarchy-broker-builder`) because the default Docker driver does not support the requested SBOM/provenance attestations. Both target `linux/amd64,linux/arm64` by default; only `docker_hub_push` publishes, while `docker_multiarch_check` builds without pushing.

### GitHub Actions CI and release

`.github/workflows/ci.yml` runs only on standard `ubuntu-latest` GitHub-hosted runners. Pull requests and normal `main` pushes run the canonical Rust harness, the Python 3.11-3.14 compatibility matrix, Ruff/Pyrefly/package validation, real Python-to-Rust standalone/cluster integration, and a multi-platform Docker build with `push: false`. The workflow has repository-wide `contents: read` permission and does not use `pull_request_target` or repository secrets on pull requests.

`.github/workflows/release.yml` is tag-driven (`v*`) and requires the tag version to match `sdks/python/pyproject.toml`. Final validation runs before either publisher job. PyPI publishing uses the `pypi` GitHub environment and OIDC Trusted Publishing; no long-lived PyPI token is stored in the workflow. Configure the matching PyPI Trusted Publisher for this repository, workflow `release.yml`, and environment `pypi` before the first release.

Docker Hub publishing is isolated in its own release job. Configure:

- repository variable `DOCKERHUB_USERNAME` — Docker Hub user or organization namespace;
- repository variable `DOCKERHUB_REPOSITORY` — target repository name;
- repository secret `DOCKERHUB_TOKEN` — Docker Hub personal access token with the minimum publish permissions required by that repository.

The workflow intentionally does not hard-code or infer the Docker Hub namespace/repository and never uses an account password. Stable semantic-version tags produce version, major/minor, major, and `latest` metadata; prereleases do not become `latest`.

## Commands

```bash
cd herterarchy-broker

make ci
make process_e2e
make perf_check
make extended
make doctor
make docker_build
make docker_up
make docker_cluster_e2e
make docker_multiarch_check

cargo xtask rules
cargo xtask check
cargo xtask test
cargo xtask ci
cargo xtask cutover
cargo xtask perf
cargo xtask extended
cargo xtask doctor

cargo run -p agent-broker-runtime --bin agentbrokerd -- serve \
  --host 127.0.0.1 \
  --port 8811
```

`make ci` is Rust-only: production cutover CI + release performance + native Rust process restart/TCP E2E. The release performance probe measures the one-node OpenRaft committed-write path separately from the existing standalone budgets. `cargo xtask extended` is the mandatory nightly fuzz gate and does not silently change the production Homebrew stable toolchain.

On the current machine production commands resolve to Homebrew stable Rust, while fuzz subprocesses explicitly use the rustup nightly proxies with `RUSTUP_TOOLCHAIN=nightly` and `RUSTUP_AUTO_INSTALL=0`.

## Development rules

Broker rules live under `.agents/rules/`; mandatory skills are `rust-production-engineering` and `rust-distributed-broker`. The key constraints remain:

- Broker core never imports ChatToCodex.
- Provider-specific concepts stay outside Broker domain/state.
- Standalone, one-node Raft, and three-node Raft modes use the same deterministic state machine.
- HA cannot be claimed without term/epoch/generation fencing and failure E2E proof.
- PostgreSQL/read models cannot become consensus authority by accident.
- Runtime code does not use panic/unwrap/expect or lint suppression as an escape hatch.
- Durable ordering and bounded resource behavior remain explicit and testable.

## Next milestone

Python retirement and the **one-node OpenRaft equivalence/quality gate** are complete: strict Clippy, workspace CI, parity, snapshot/restart, stale fencing, ACK hard-kill durability, seeded fuzz with immutable golden seeds, and release performance are all canonical repository gates.

The first real three-node OpenRaft slice now exists: bounded TCP RPC, static learner-to-voter bootstrap, majority replication, follower write rejection, leader process shutdown/re-election, new-term fencing, durable node rejoin/log catch-up, forced snapshot catch-up after the leader has purged past a stale follower, and live-leader network partition/healing have focused Rust and Docker evidence. Under partition, an isolated old leader remains client-reachable but cannot ACK a mutation; the 2/3 majority advances in a higher term, and healing returns all three nodes to the exact majority term/revision. Truncated incoming snapshot data is also fail-closed before authoritative state replacement.

The three-node fault matrix now includes durable corruption fail-stop, mandatory snapshot catch-up after leader purge, real binary snapshot-body cut/retry through a TLS-opaque proxy, stale-leader network isolation/healing, slow-peer non-HOL quorum progress, exact bounded TLS-handshake worker/queue saturation, repeated leader restart/idempotency, identified-write quorum-ACK loss, leader-only maintenance, fail-fast application backpressure, bounded connection churn, bounded IP-only outbound Raft connect, mandatory TLS 1.3 mTLS + exact peer pinning, and bounded operations-v1 readiness. The complete focused matrix is green. Static client leader discovery/re-discovery is also implemented and the Python SDK proves leader-stop, surviving-majority rediscovery, continued owner-aware sequencing, and stopped-node restart on a real loopback mTLS cluster. The remaining static-topology release evidence is the freshly approved destructive Docker mTLS fault run plus optional supply-chain tooling; dynamic membership remains deliberately deferred. ChatToCodex integration migration remains a separate architecture change.

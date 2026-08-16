# Agent Broker

`agent-broker/` is the standalone, provider-independent work Broker runtime for ChatToCodex and future agent runtimes.

It deliberately does not know about ChatGPT, Chrome, Claude, Kimi, Codex CLI, model names, prompt delivery, OAuth, or browser/session state. Provider runtimes are Broker clients. The Broker owns authoritative work-coordination state.

## Current milestone

The Agent Broker is now **Rust-only**. Rust owns the live source, `agentbrokerd` executable, protocol/runtime implementation, persistence, process E2E, production CI, release-performance gates, and fuzz targets.

The former CPython reference implementation was retired on 2026-08-16 only after the complete retirement gate passed: Python reference CI, cross-language hard-kill/restart E2E, Rust tests/Clippy/performance, and nightly cargo-fuzz for protocol/snapshot/journal.

The Python source/package/tests/benchmarks are no longer present. Language-neutral migration evidence remains under `compatibility/`, immutable seed inputs remain under `fuzz/seeds/`, and deletion identities are recorded under `compatibility/retirement/`.

Implemented now:

- `agentbrokerd serve` Rust standalone process.
- Loopback-only, versioned NDJSON protocol.
- Typed synchronous Rust `BrokerClient` with one lazy reusable TCP connection and no automatic mutation retries.
- Namespace, Task, Consumer Group, Member, heartbeat, leave, claim, lease renewal, completion, and retention lifecycles.
- Fencing by Broker term, Consumer Group generation, lease epoch, lease ID, ownership, and lease expiry.
- Incremental append/flush/fsync journal plus periodic same-directory tempfile + fsync + atomic-replace snapshots.
- Strict startup snapshot+journal replay, final torn-tail repair, middle/semantic corruption fail-stop, and bounded deferred compaction.
- Standalone `ConsensusAdapter` fail-stop poisoning after durability failure.
- Process ownership lock for one standalone Broker per state path.
- Bounded frame size, connection count, in-flight request count, namespace/task/group/member hot-state capacity, maintenance batch sizes, and retained completed Tasks.
- Single-owner state thread; socket workers never share the mutable state machine through a mutex.
- Heap/index-backed ready Tasks, lease expirations, completed-task retention, active lease IDs, and per-member leased Task lookup rebuilt deterministically from checkpoints.
- Standalone maintenance for stale-member reaping and completed-task pruning.
- Real TCP lifecycle E2E and hard-process-kill/restart durability E2E.
- Strict byte-level protocol/storage conformance against the frozen migration corpora.
- Nightly cargo-fuzz targets for protocol v1, snapshot v1, and journal v1 with immutable golden seed verification.

Not implemented yet:

- Raft log replication.
- Multi-node leader election.
- Cluster membership changes.
- Cross-node Broker transport/authentication.
- Automatic client leader discovery/redirection.
- Multi-node failover.

`agentbrokerd doctor --nodes 3` validates quorum math only. It does **not** mean three-node HA is implemented.

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

Standalone mode uses `StandaloneConsensusAdapter`. Future Raft mode must replace the consensus adapter, not fork Broker domain semantics. The same deterministic state-machine commands and fencing values must be replayable from a replicated log.

## Rust workspace

The Edition 2024 resolver-3 workspace includes:

- `agent-broker-domain` — deterministic authoritative Broker state machine and invariants.
- `agent-broker-application` — typed use cases above `ConsensusAdapter`.
- `agent-broker-protocol` — strict protocol-v1 request/response codec and dispatch.
- `agent-broker-storage` — schema-v1 snapshot/journal codec, replay, durability, repair, and compaction.
- `agent-broker-consensus` — standalone commit ordering and fail-stop adapter.
- `agent-broker-client` — typed reusable synchronous Rust client.
- `agent-broker-runtime` — process lock, state-owner thread, TCP runtime, maintenance, and `agentbrokerd`.
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
- protocol p99 <= 5 ms,
- snapshot install <= 250 ms,
- recovery <= 250 ms,
- bounded queue saturation <= 1000 ms.

These are regression budgets, not machine-independent performance promises.

## Commands

```bash
cd agent-broker

make ci
make process_e2e
make perf_check
make extended
make doctor

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

`make ci` is Rust-only: production cutover CI + release performance + native Rust process restart/TCP E2E. `cargo xtask extended` is the mandatory nightly fuzz gate and does not silently change the production Homebrew stable toolchain.

On the current machine production commands resolve to Homebrew stable Rust, while fuzz subprocesses explicitly use the rustup nightly proxies with `RUSTUP_TOOLCHAIN=nightly` and `RUSTUP_AUTO_INSTALL=0`.

## Development rules

Broker rules live under `.agents/rules/`; mandatory skills are `rust-production-engineering` and `rust-distributed-broker`. The key constraints remain:

- Broker core never imports ChatToCodex.
- Provider-specific concepts stay outside Broker domain/state.
- Standalone and future multi-node modes use the same deterministic state machine.
- HA cannot be claimed without term/epoch/generation fencing and failure E2E proof.
- PostgreSQL/read models cannot become consensus authority by accident.
- Runtime code does not use panic/unwrap/expect or lint suppression as an escape hatch.
- Durable ordering and bounded resource behavior remain explicit and testable.

## Next milestone

Python retirement is complete. The next distributed-systems milestone is **one-node Raft equivalence behind the existing `ConsensusAdapter`**, proving that committed-log application produces the same authoritative state and durability semantics as standalone mode. Only after one-node equivalence and failure tests pass should three-node HA work begin.

---
name: rust-distributed-broker
description: Use for Agent Broker domain, WAL, snapshot, lease, Consumer Group, consensus, Raft, replication, membership, failover, and cluster work. Enforces deterministic state machines, fencing, quorum safety, single-writer authority, bounded recovery, and provider-independent broker boundaries.
version: 1.0.0
---

# Rust Distributed Broker

Apply this skill in addition to `rust-production-engineering` whenever work touches Broker coordination, persistence, consensus, leases, Consumer Groups, or cluster behavior.

## 1. Architectural boundary

The Broker owns coordination state, not agent-provider behavior.

Broker domain may know:

- namespace/work identity
- task/run identity and lifecycle
- Consumer Group identity and generation
- member identity and capability metadata
- assignment/claim state
- lease identity, epoch, expiration, and owner
- broker node identity and cluster membership
- consensus term/log/commit metadata
- retry, timeout, retention, and recovery policy

Broker domain must not know ChatGPT, Chrome, Claude, Kimi, Codex UI, MCP presentation, provider OAuth, browser selectors, model names, or prompt-entry mechanics.

## 2. Deterministic state machine

- The authoritative state machine must be deterministic and replayable.
- It may not read wall-clock time, randomness, sockets, environment variables, filesystem state, or process-local identity while applying a committed command.
- Time, randomness, identifiers, and policy inputs required for a transition must be explicit command fields selected before consensus/commit.
- State-machine outputs are pure consequences of `(previous_state, committed_command)`.
- Standalone and clustered deployments use the same state machine. Never fork business semantics into separate single-node and HA implementations.

## 3. Invalid distributed states must be hard to represent

Model distinct states with enums and state-specific data. For example, a leased task should contain a complete lease object rather than `status = Leased` plus optional lease fields.

Use newtypes for fencing values:

```text
Term
LogIndex
CommitIndex
GroupGeneration
LeaseEpoch
AssignmentRevision
```

Do not compare unrelated counters as raw integers.

## 4. Authority and write path

Authoritative mutation follows this conceptual order:

```text
request
  -> validate syntax/static constraints
  -> leader/term/fencing validation
  -> propose to consensus or standalone commit adapter
  -> durability/quorum commit
  -> deterministic state-machine apply
  -> publish observational/read-model events
  -> respond
```

- A response that claims success must not precede the durability level promised by that operation.
- PostgreSQL, dashboards, caches, metrics, and search indexes are read models only unless an explicit future design changes that contract.
- Read-model failure must not silently change authoritative task ownership.

## 5. Fencing is mandatory

At minimum, worker-owned task mutation must validate the relevant combination of:

- current Broker/consensus term
- Consumer Group generation
- member identity
- lease identity
- lease epoch
- assignment/task revision when used by the protocol
- lease expiration at the authoritative observation time

After leadership or ownership changes, stale actors must fail closed. A delayed old leader, old group member, or expired lease holder must never be able to complete or renew work merely because it still has a network connection.

## 6. Consumer Group semantics

- Group membership change increments generation exactly according to one deterministic rule.
- Assignment/lease commands carry the generation they were based on.
- A member from an old generation must rejoin before taking authoritative action.
- Member timeout/reaping must be bounded and deterministic from an explicit observation timestamp.
- Work owned by a removed member is requeued/reassigned exactly once according to the committed transition.
- A Consumer Group may contain heterogeneous provider runtimes; provider type is not the group identity.

## 7. Lease semantics

- Claim creates a unique active lease and increments the task lease epoch on actual reassignment.
- Renewal extends an existing valid lease without creating a new ownership epoch unless the ownership model explicitly changes.
- Completion requires a valid unexpired matching lease.
- Expiry/requeue is idempotent.
- Duplicate lease IDs across incompatible owners are rejected.
- Lease duration and renewal limits are bounded to prevent unbounded authority retention.

## 8. WAL and snapshot rules

- Persist ordered authoritative mutations, not arbitrary in-memory object graphs.
- Journal/log records carry explicit schema/version information and monotonic ordering metadata.
- Detect revision/log gaps, backward terms, corruption before the tail, and invalid references.
- A torn final record may be recovered only by a documented tail-repair rule; corruption before the tail fails closed.
- Snapshot creation must be atomic from the reader's perspective.
- Snapshot compaction cannot erase entries still required by the durability/replication contract.
- Recovery must reconstruct exactly the same logical state and indexes as uninterrupted execution.
- Storage failures after an in-memory transition cannot be ignored; standalone mode must fail-stop or otherwise guarantee rollback semantics.

## 9. Raft/consensus abstraction

Keep the domain independent of a concrete Raft library.

```text
BrokerApplication
      |
ConsensusAdapter trait
      |----------------------|
StandaloneConsensus       RaftConsensus
```

The adapter contract must make the difference between proposal, durable append, quorum commit, state-machine apply, and response visible enough to preserve correctness.

For a 3-node HA cluster:

- writes require majority quorum, normally 2/3
- loss of quorum stops authoritative writes
- no degraded split-brain write mode
- leadership term change fences stale leaders
- membership changes use the consensus library's supported safe membership mechanism; do not invent ad-hoc peer-list mutation

Do not claim HA until leader election, quorum commit, restart recovery, stale-leader rejection, and one-node-failure continuity are exercised by E2E tests.

## 10. Single-node compatibility

A one-node deployment is a first-class cluster size, not a separate product.

- Same command types
- Same state machine
- Same fencing semantics
- Same persistence format where reasonable
- Same client protocol

The single-node consensus adapter may commit locally, but it must preserve the ordering and durability contract expected by the future clustered adapter.

## 11. Backpressure and bounded memory

Every remotely amplifiable collection or task has a bound or retention policy:

- connections
- in-flight requests
- protocol frame size
- namespaces
- tasks per namespace
- groups per namespace
- members per group
- outstanding leases
- result/objective payload size
- journal size before compaction
- completed-task retention
- retry queues and history

Reject capacity exhaustion with typed errors. Do not let overload silently convert into OOM, unlimited task spawning, or unbounded history growth.

## 12. Concurrency architecture

Prefer one authoritative state-machine owner receiving commands over arbitrary shared-state locking.

A desired runtime shape is:

```text
network tasks
    -> bounded channel
    -> consensus/leader path
    -> committed-entry channel
    -> state-machine owner
    -> result/read-model notifications
```

Use Loom only for small shared-memory primitives if such primitives remain after this design. Do not use Loom as a substitute for distributed fault testing.

## 13. Fault and recovery tests

Before clustered production claims, maintain repeatable tests for:

- process kill before and after WAL append/fsync
- torn journal tail
- corrupted non-tail record
- snapshot + log replay
- lease expiry and reassignment
- stale completion after reassignment
- stale generation after member timeout
- duplicate client retry/idempotency
- leader kill during proposal
- leader kill after local append but before quorum
- leader kill after quorum commit but before client response
- follower restart and catch-up
- quorum loss and recovery
- stale leader attempting a write after a new term
- network partition where only the majority side may make progress

The critical invariant is that a committed task transition is not lost and an uncommitted/stale owner cannot become authoritative after failover.

## 14. Protocol and compatibility

- Wire protocol inputs are untrusted and size-bounded.
- Decode into typed request structures before touching authority.
- Unknown fields/version mismatches fail explicitly unless the protocol version defines forward-compatible extension behavior.
- Keep language-neutral conformance fixtures after Python source retirement so the migrated wire/storage contract remains independently checkable.
- Fuzz protocol, snapshot, and journal decoders.

## 15. Performance policy

Performance optimization must preserve semantics. Measure before changing algorithms.

Track at least:

- cold boot time
- idle RSS
- publish throughput
- claim/complete throughput
- durable single-node write throughput
- p50/p95/p99 request latency
- recovery/replay time versus log size
- snapshot time/size
- leader failover and readiness time once Raft exists

Prefer indexes/heaps/ownership changes that reduce algorithmic work before introducing unsafe code or exotic synchronization.

## 16. Completion gates

Distributed changes are complete only with evidence at the layer they affect. Examples:

```text
pure transition       -> deterministic tests
lease/fencing         -> stale-owner tests
storage               -> crash/replay tests
shared concurrency    -> Loom where applicable
wire parser           -> fuzz target + boundary tests
Raft                   -> multi-process failover/quorum E2E
performance           -> benchmark before/after
```

Never substitute a unit test for an unexercised failure mode that is inherently process-, disk-, network-, or quorum-dependent.

---
name: rust-production-engineering
description: Use for all Rust implementation, review, refactoring, performance, concurrency, dependency, API, and CI work in the Agent Broker. Enforces Rust-first ownership and type modeling, safe concurrency, strict lints, bounded dependencies, panic discipline, async cancellation review, and evidence-backed verification.
version: 1.0.0
---

# Rust Production Engineering

Use this skill whenever Rust code is created or changed under `agent-broker/`. It supplements the inherited ChatToCodex rules and never weakens them.

## 1. Design with the type system

- Make invalid states unrepresentable where practical. Prefer enums with state-specific payloads over structs containing many `Option<T>` fields whose combinations require runtime invariants.
- Use newtypes for domain identities and fencing values such as `TaskId`, `GroupId`, `MemberId`, `LeaseId`, `Term`, `Generation`, `LeaseEpoch`, and `Revision`. Do not pass raw `String` or `u64` across domain APIs when the values have different meanings.
- Prefer immutable values and explicit state transitions. Mutation belongs behind narrow ownership boundaries.
- Use exhaustive `match` for domain state. Avoid wildcard arms when adding a new variant must force a compile error at every transition point.
- Public APIs should expose domain meaning, not implementation storage details.

## 2. Ownership and concurrency

- Prefer single ownership and message passing over shared mutable state.
- Do not default to `Arc<Mutex<T>>`. If a single task/thread can own the state machine, commands should be sent to that owner.
- If a mutex is justified, keep the critical section short and document the protected invariant.
- Never hold a blocking mutex guard across `.await`.
- Do not use an async mutex for ordinary in-memory data merely because the surrounding code is async.
- Blocking disk or CPU work must not run directly on the async reactor. Move it to an explicit blocking boundary or a dedicated worker.
- Every spawned task needs an owner, shutdown path, and bounded lifetime. Detached tasks are forbidden unless explicitly documented as process-lifetime infrastructure.

## 3. Async and cancellation safety

- Review every `.await` as a cancellation point.
- Do not leave authoritative in-memory state partially mutated before an awaited durability or consensus step completes.
- Prefer `validate -> durable/consensus commit -> deterministic apply -> response` for authoritative mutations.
- `select!` branches must be safe if the losing future is dropped at that point.
- Timeouts must define what happens to ownership, leases, pending writes, and retries after cancellation.

## 4. Panic and error policy

- Runtime request paths return typed errors; they do not panic for malformed input, capacity pressure, stale fencing, I/O failure, or unavailable peers.
- Production code should not use `unwrap`, `expect`, `panic!`, `todo!`, or `dbg!` as control flow.
- Test code may use narrow `unwrap`/`expect` only when failure is itself the test harness failure mode.
- Truly impossible internal states require an invariant comment and a focused test. Prefer a typed error over `unreachable!` when corruption or external input could make the state reachable.
- Preserve error sources. Avoid string-only error plumbing when structured context is available.

## 5. Safe Rust first

- Broker crates start with workspace lint `unsafe_code = "forbid"`.
- Do not introduce `unsafe` for performance speculation.
- If profiling eventually proves a need, unsafe code requires a separately reviewed module, documented safety invariants, a safe wrapper API, Miri coverage where supported, fuzzing for its input boundary, and explicit approval to relax the workspace rule.

## 6. Lint and formatting policy

- `cargo fmt --check` is mandatory.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` is mandatory.
- Enable `clippy::pedantic` deliberately and allow individual false positives locally with a reason.
- Do not enable the entire `clippy::restriction` group. Cherry-pick useful restriction lints such as `unwrap_used`, `expect_used`, `panic`, `todo`, and `dbg_macro` for production crates.
- Put shared lint levels in `[workspace.lints]`; member crates inherit them with `[lints] workspace = true`.

## 7. Dependencies

- Every new dependency needs a concrete reason and must be preferable to a small standard-library implementation for the required surface.
- Disable default features when they pull unrelated capabilities.
- Keep protocol/domain/state-machine crates lighter than runtime/server crates.
- Pin the repository toolchain with `rust-toolchain.toml`; do not depend on whichever compiler happens to be installed globally.
- Dependency policy is checked with `cargo-deny` for advisories, licenses, bans/duplicates, and source provenance.

## 8. Testing strategy

Use layered tests instead of relying on one large integration suite:

1. Pure deterministic unit tests for domain/state-machine transitions.
2. Property/generative tests for invariants that span many command sequences.
3. Protocol parser/codec tests for malformed and boundary inputs.
4. Process and crash-recovery tests for WAL/snapshot behavior.
5. Loom tests only for small concurrency primitives that truly share memory.
6. Miri tests for UB-sensitive or unsafe-adjacent code where supported.
7. `cargo-fuzz` targets for untrusted parsers, journal/snapshot decoders, and protocol frames.
8. Benchmarks with explicit regression thresholds for boot, memory, throughput, and tail latency.

A passing Loom or Miri run is evidence for the checked executions, not a proof of total correctness.

## 9. Observability

- Use structured `tracing` spans/events for runtime diagnostics instead of ad-hoc `println!` in long-running services.
- Include stable identifiers such as node, term, group, task, member, lease epoch, and revision when relevant.
- Never log secrets, provider tokens, full prompts, or user content unless a higher-level policy explicitly permits it.
- Metrics and logs are observational; they must never become assignment or consensus authority.

## 10. Verification before completion

For ordinary Rust changes, run the repository harness rather than hand-picking a weaker subset. The target interface is:

```text
cargo xtask rules
cargo xtask ci
```

For concurrency, storage, protocol, or unsafe-adjacent work, run the applicable extended gates:

```text
cargo xtask ci-extended
cargo xtask perf
```

Do not claim a gate passed unless it was actually executed in the current work session.

## Research basis

This policy is grounded in the Rust/Cargo official documentation, Clippy documentation, rustup toolchain-file guidance, rust-analyzer's repository-local `xtask` pattern, Tokio guidance on synchronization and structured tracing, Miri, cargo-fuzz, Loom, cargo-nextest, and cargo-deny. Re-check current upstream documentation before changing tool-specific rules because these tools evolve.

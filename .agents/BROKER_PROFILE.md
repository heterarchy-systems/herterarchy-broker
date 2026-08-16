# Agent Broker Rule Profile

This profile supplements the inherited ChatToCodex development rules for the standalone Broker boundary.

For every Rust Broker change, load `.agents/rules/00-overview.md` first and then only the routed rule files relevant to the current change.

For Rust work, both of these skills are mandatory:

- `.agents/skills/rust-production-engineering/SKILL.md`
- `.agents/skills/rust-distributed-broker/SKILL.md`

The Rust-side verification architecture is defined in `.agents/RUST_DEVELOPMENT_HARNESS.md`. The canonical Rust repository interface is the workspace-local `cargo xtask ...` harness. The existing Python rule harness remains active only for the inherited Python rule corpus during migration.

- The Broker package must not import `chattocodex` or provider-specific runtime modules.
- Agent Broker must build, test, and run independently of the ChatToCodex application. ChatToCodex is a client/runtime integration, not an in-process owner of Broker authority.
- The Python Broker source remains a CPython 3.14t conformance oracle during migration, but it must not own the `agentbrokerd` product executable or production package identity. New production Broker implementation work targets Rust; Python changes are limited to oracle/conformance maintenance until retirement.
- One-node and multi-node deployments share the same authoritative state-machine model; do not create separate standalone and HA business logic.
- Multi-node safety work must be explicit about term/epoch/generation/revision fencing. Never claim HA before leader election, quorum commit, and stale-leader rejection are tested.
- Consensus state and management/read-model state remain separate. PostgreSQL, dashboards, metrics, and logs must not become assignment authority by accident.
- Provider identity and transport details stay outside Broker domain state except for narrow opaque capability metadata required for routing.
- Prefer deterministic, replayable transitions and immutable result/history records over hidden in-memory mutation.
- Do not move existing ChatToCodex Broker code into the Rust runtime until a compatibility/client boundary is locked by tests.

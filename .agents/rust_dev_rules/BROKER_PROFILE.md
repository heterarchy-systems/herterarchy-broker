# Agent Broker Rule Profile

This profile supplements the inherited ChatToCodex development rules for the standalone Broker boundary.

For every Rust Broker change, load `.agents/rust_dev_rules/rules/00-overview.md` first and then only the routed rule files relevant to the current change.

For Rust work, both of these skills are mandatory:

- `.agents/rust_dev_rules/skills/rust-production-engineering/SKILL.md`
- `.agents/rust_dev_rules/skills/rust-distributed-broker/SKILL.md`

The Rust-side verification architecture is defined in `.agents/rust_dev_rules/RUST_DEVELOPMENT_HARNESS.md`. The canonical Rust repository interface is the workspace-local `cargo xtask ...` harness. The retired Python Broker and its old rule harness are not production authorities; a language SDK may exist only behind the typed client protocol boundary.

- The Broker package must not import `chattocodex` or provider-specific runtime modules.
- Agent Broker must build, test, and run independently of the ChatToCodex application. ChatToCodex is a client/runtime integration, not an in-process owner of Broker authority.
- The Python Broker source is retired. Python is permitted under `sdks/python/` only as an external client SDK; it must not contain consensus, persistence, authoritative state-machine, Raft peer, or Broker server authority. Rust continues to own `agentbrokerd` and all production Broker semantics.
- One-node and multi-node deployments share the same authoritative state-machine model; do not create separate standalone and HA business logic.
- Multi-node safety work must be explicit about term/epoch/generation/revision fencing. Never claim HA before leader election, quorum commit, and stale-leader rejection are tested.
- Consensus state and management/read-model state remain separate. PostgreSQL, dashboards, metrics, and logs must not become assignment authority by accident.
- Provider identity and transport details stay outside Broker domain state except for narrow opaque capability metadata required for routing.
- Prefer deterministic, replayable transitions and immutable result/history records over hidden in-memory mutation.
- Do not reintroduce retired ChatToCodex/Python Broker implementation code. External runtimes integrate through the versioned client protocol and SDK boundary.

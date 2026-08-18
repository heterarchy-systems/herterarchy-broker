# Type Strictness Rules

- Production code must pass the repository's Pyrefly configuration without broad ignores.
- Model project IDs, target IDs, request IDs, statuses, revisions, and capacity decisions explicitly.
- `None` represents a real state, not undecided design or caller convenience.
- Narrow unions at the boundary. Core services should not branch over arbitrary transport shapes.
- Callback signatures state their accepted decision and returned result.
- Prefer immutable tuples and mappings for snapshots; use mutable collections only where mutation is owned and bounded.
- Never solve a type error by weakening an unrelated public contract.


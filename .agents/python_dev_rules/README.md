# ChatToCodex Rules Index

This directory is the repository-specific development rule source.

Always read:

1. `규칙.md` — non-negotiable invariants and completion contract.
2. `00-overview.md` — system purpose and authority model.
3. The rule files relevant to the touched boundary.

Rule map:

- `01` architecture and authority boundaries
- `02` package/module placement
- `03` naming
- `04` Pydantic validation
- `05` internal dataclasses
- `06` mapping shapes
- `07` type strictness
- `08` cohesion
- `09` normalization and project identity
- `10` durable filesystem state and concurrency
- `11` native asyncio I/O, deadlines, cancellation, structured concurrency, backpressure, admission, and browser safety
- `12` sync/async errors, ambiguous outcomes, and conditional cleanup
- `13` Ruff, Pyrefly, Node, pytest, and async cancellation/concurrency gates
- `14` scoped refactoring
- `15` agent execution and worktree preservation

These rules constrain implementation; they are not a product backlog and do not authorize unrelated changes.


# Class, Function, and Cohesion Rules

- A class owns one coherent policy, repository, adapter, or mapping responsibility.
- Separate pure state synchronization from browser side effects and filesystem I/O.
- Extract repeated capacity or pruning rules into named pure helpers only when reuse or testability is concrete.
- Treat large services and methods with mixed validation, state mutation, and external effects as review triggers, not automatic rewrite targets.
- Public methods expose complete use cases; callers must not reproduce internal transition order.
- Costly I/O and state mutation must not hide behind properties.
- Prefer deletion and existing patterns over speculative factories or plugin layers.


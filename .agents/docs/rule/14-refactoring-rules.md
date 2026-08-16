# Refactoring Rules

- Refactor only the touched policy or boundary needed for correctness, concurrency defense, memory bounds, or testability.
- Lock current behavior with regression tests before cleanup when coverage is missing.
- Prefer deleting duplicated policy and reusing existing repositories, mappers, coordinators, and lock primitives.
- Keep browser policy, backend capacity policy, and durable persistence separate.
- Do not introduce dependencies, alternate persistence, or broad folder rewrites without explicit scope.
- Preserve public contracts unless the requested safety fix requires a deliberate, tested change.
- Review the final diff for debug code, unbounded collections, duplicate transition logic, and side effects under locks/updaters.


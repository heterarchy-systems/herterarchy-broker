# Async, Locking, and Side-Effect Rules

- Pure validation, mapping, hashing, capacity calculation, and state pruning remain synchronous.
- Async code is for real external waits. Do not parallelize safety-critical browser admission.
- Global provisioning/reopen admission permits at most one in-flight browser action across projects.
- Keep a minimum consecutive admission interval of 10 to 15 seconds.
- Provider rate-limit backoff follows 30, 60, then 120 seconds and saturates at the bounded maximum; do not create aggressive retry loops.
- Re-read tab registration, active/pinned state, wake ownership, and composer content immediately before a destructive or visible browser action.
- Never erase human composer text or close a tab that has become active, pinned, changed, or user-owned.
- Release locks before invoking external callbacks or side effects; reacquire and compare expected state before committing results.


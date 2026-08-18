# Lint, Typecheck, and Test Rules

The canonical full gate is:

```bash
make ci
```

It must run:

- `ruff format --check .`
- `ruff check .`
- `pyrefly check`
- a required Node.js availability check
- `pytest -q`

Node-based Chrome harnesses are required safety tests, not optional skips. Focused tests must cover changed invariants before the full gate, including concurrency races and stale-state cases. Report the exact command and result; tests not run are not verified. Do not weaken configuration or add broad suppressions to manufacture a green result.

For async code, focused verification additionally covers the changed subset of:

- real event-loop concurrency without `to_thread()`/executor wrapping of synchronous network clients;
- cancellation before connect, during network wait, and after a mutation may have been submitted;
- timeout/deadline classification, including preservation of ambiguous mutation outcome semantics;
- bounded frame handling for EOF, missing delimiter, oversized frames, `IncompleteReadError`, and `LimitOverrunError` paths;
- write backpressure (`drain()`), bounded queue/semaphore admission, and connection churn;
- structured-concurrency shutdown with no unintentionally live tasks or leaked transports;
- concurrent use of reusable async client objects, proving requests cannot interleave or corrupt response correlation;
- leader rediscovery/restart/failover for cluster-aware async clients while exact retry identity remains unchanged.
- library event-loop ownership, proving SDK methods work inside an already-running loop and never attempt nested `asyncio.run()`/private loop management;
- the supported Python-version floor, so tests do not accidentally make Python 3.11 support depend on a newer asyncio-only API.

Prefer deterministic events, local test servers, barriers, and bounded deadlines over timing-by-`sleep()` races. Reuse the existing test stack; do not add `pytest-asyncio` or another async test dependency merely for convenience when the standard library (`unittest.IsolatedAsyncioTestCase`, `asyncio.run`, local asyncio servers) is sufficient.


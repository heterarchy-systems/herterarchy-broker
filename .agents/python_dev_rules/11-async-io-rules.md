# Async, Locking, and Side-Effect Rules

## Async boundary

- Pure validation, mapping, hashing, capacity calculation, state pruning, and already-in-memory state transitions remain synchronous.
- Async code is for real external waits: network I/O, subprocess waits, timers, and explicitly isolated blocking I/O boundaries.
- A public async network API must use native `asyncio` transport/stream primitives. Do not implement async networking by wrapping the synchronous socket client in `asyncio.to_thread()`, an executor, or a private worker thread.
- `asyncio.to_thread()` / executors are reserved for unavoidable blocking I/O that has no native async API, such as selected filesystem calls. They are not the default concurrency model and must not be used to disguise blocking network code as async.
- CPU-heavy work does not run on the event-loop thread. Move it to an appropriate bounded execution boundary only when it is actually needed.

## Network streams and framing

- Prefer high-level `asyncio.open_connection()` / stream APIs for ordinary TCP clients unless a lower-level transport is required by a measured or protocol-specific need.
- Configure the reader `limit` from the protocol's bounded frame policy rather than accepting an unrelated unbounded/default buffering contract.
- For newline-framed protocols, use a bounded delimiter read (`readuntil(b"\n")` or an equivalently bounded implementation). Treat `IncompleteReadError`, `LimitOverrunError`, EOF-before-delimiter, and over-limit frames as transport/protocol failures; never accept a partial EOF frame as complete.
- Pair `StreamWriter.write()` with `await writer.drain()` so write-buffer backpressure participates in flow control.
- Close stream writers deterministically. Call `writer.close()` and, on ordinary completion/error cleanup where cancellation semantics permit, await `writer.wait_closed()` without masking the original failure.
- A connection may carry concurrent operations only if the wire protocol explicitly supports multiplexing and response correlation. Otherwise serialize access or, preferably for cancellation-sensitive request/response SDK calls, give one in-flight operation exclusive ownership of its connection.
- Never let cancellation of one request corrupt another request's framing state on a shared stream.

## Deadlines and timeout semantics

- Every external operation has a bounded deadline. Prefer one end-to-end deadline measured from the event loop's monotonic clock (`loop.time()`) and `asyncio.timeout_at()` / `asyncio.timeout()` rather than resetting the full timeout independently for connect, write, drain, and read phases.
- A timeout is not proof that a side-effecting operation did not happen. If request bytes may have reached the Broker or another authoritative service, timeout/cancellation after that point is an ambiguous outcome and must follow the protocol's `UNKNOWN`/idempotency contract.
- Automatic retries after an ambiguous mutation outcome may reuse only the exact serialized request and durable identity required by the protocol. Do not regenerate request IDs, owner epochs, sequences, or mutation bodies during rediscovery/retry.
- Do not use aggressive nested `wait_for()` calls that multiply cancellation paths or silently extend the effective deadline while waiting for child cancellation.

## Cancellation safety

- `asyncio.CancelledError` is control flow, not an ordinary transport failure. Use `try/finally` for cleanup and re-raise cancellation after cleanup; do not swallow it or translate it into success.
- Do not call `uncancel()` in production code unless a documented structured-concurrency requirement makes it unavoidable and a focused test proves the behavior.
- `asyncio.timeout()` and `TaskGroup` rely on cancellation internally. Code used inside them must preserve cancellation rather than catching broad `BaseException` or consuming `CancelledError`.
- Do not use `asyncio.shield()` to pretend a caller-cancelled mutation is safely failed. Shield only a narrowly justified cleanup/ownership operation whose lifetime and result are explicitly retained; never use it to bypass the Broker's ambiguous-outcome contract.
- Once an async mutation crosses the point where bytes may have been written, cancellation must not authorize a different mutation identity. Recovery uses the same exact identity or returns an explicit ambiguous outcome to the caller.

## Structured concurrency and task ownership

- Prefer direct `await` for one child operation and `asyncio.TaskGroup` for a related set of child tasks that should share failure/cancellation lifetime.
- Use `asyncio.gather()` only when its different sibling-failure/cancellation semantics are intentional and tested.
- Do not create fire-and-forget tasks by dropping the result of `asyncio.create_task()`. Every spawned task has an owner that awaits it, keeps a strong reference, or manages it in a `TaskGroup`/explicit registry through shutdown.
- Shutdown paths account for all owned tasks and resources. No test or service may report clean shutdown while request, heartbeat, lease-renewal, or background retry tasks remain live unintentionally.

## Event-loop ownership and version boundary

- Library/SDK code does not call `asyncio.run()` or create/replace the application's event loop internally. It exposes awaitables and lets the application own loop startup/shutdown. `asyncio.run()` or `asyncio.Runner` belongs at a top-level program/test boundary.
- Do not cache or depend on an ambient loop before async execution. When low-level loop access is genuinely required inside a coroutine, use `asyncio.get_running_loop()`.
- An async client may be concurrently reused by multiple Tasks in the same event loop only when its own state/transport design is task-safe. This does not make the client or `asyncio` primitives safe to share across OS threads or independent event loops.
- Do not add new dependencies on global event-loop policy APIs. Prefer high-level runners and explicit `loop_factory` configuration at the application boundary when loop customization is actually needed.
- The Python SDK currently supports Python `>=3.11`, so `TaskGroup` and `asyncio.timeout()` are available. Do not rely unconditionally on APIs introduced after 3.11 (for example newer queue shutdown helpers) without either a compatibility path or an explicit supported-version increase.
- `ExceptionGroup` from `TaskGroup` is part of the failure contract. Handle grouped child failures only at a boundary that can classify them without losing cancellation or silently discarding sibling failures.

## Backpressure and admission

- Async concurrency is bounded. `asyncio.Queue` used for work/admission must have a positive `maxsize` unless unbounded growth is a deliberate, measured, and documented requirement.
- Use bounded semaphores/queues or another explicit admission policy before fan-out. Do not build an unbounded list of network coroutines and pass it to `gather()`.
- Queue/semaphore waits are external waits and therefore receive deadline/cancellation treatment consistent with the caller's operation budget.
- Asyncio synchronization primitives are event-loop primitives, not thread-safety primitives. Do not share them across OS threads or use them as a substitute for filesystem/process locking.
- Do not hold an `asyncio.Lock` across unrelated network, filesystem, callback, or user-visible side effects. Keep critical sections small; snapshot the required state, release the lock, perform the wait, then reacquire and revalidate if a commit is still needed.

## Existing browser/admission safety

- Do not parallelize safety-critical browser admission.
- Global provisioning/reopen admission permits at most one in-flight browser action across projects.
- Keep a minimum consecutive admission interval of 10 to 15 seconds.
- Provider rate-limit backoff follows 30, 60, then 120 seconds and saturates at the bounded maximum; do not create aggressive retry loops.
- Re-read tab registration, active/pinned state, wake ownership, and composer content immediately before a destructive or visible browser action.
- Never erase human composer text or close a tab that has become active, pinned, changed, or user-owned.
- Release locks before invoking external callbacks or side effects; reacquire and compare expected state before committing results.
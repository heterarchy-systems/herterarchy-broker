# Error and Exception Rules

- Distinguish validation, binding conflict, stale revision, capacity, admission, provider rate limit, filesystem, and browser adapter errors.
- Async boundaries also distinguish caller cancellation, deadline expiry, transport failure, protocol/framing failure, and authoritative ambiguous mutation outcome. A timeout/cancelled wait is not automatically equivalent to a rejected mutation.
- Boundary errors are structured and contain safe operation/resource context plus recoverability.
- Never include owner tokens, OAuth material, local paths in browser responses, or raw credentials in errors.
- Catch broad exceptions only at a recovery boundary that records failure, applies bounded backoff, or converts to a structured error.
- Do not catch or downgrade `asyncio.CancelledError` as an ordinary transport error. If explicitly caught for cleanup, re-raise it when cleanup is complete.
- Convert built-in `TimeoutError` only at a boundary that knows whether the operation is read-only, definitely unsubmitted, or potentially committed. Preserve `UNKNOWN` semantics for side-effecting operations whose request may have reached authority.
- Stream `IncompleteReadError`/`LimitOverrunError`, malformed framing, and EOF-before-complete-frame remain distinct from clean application responses.
- Never silently ignore an exception or convert a concurrency conflict into success.
- Cleanup after failed provisioning is conditional: re-read identity and user-ownership state before removing only the runtime-created orphan.


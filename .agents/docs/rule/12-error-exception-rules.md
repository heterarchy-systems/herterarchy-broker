# Error and Exception Rules

- Distinguish validation, binding conflict, stale revision, capacity, admission, provider rate limit, filesystem, and browser adapter errors.
- Boundary errors are structured and contain safe operation/resource context plus recoverability.
- Never include owner tokens, OAuth material, local paths in browser responses, or raw credentials in errors.
- Catch broad exceptions only at a recovery boundary that records failure, applies bounded backoff, or converts to a structured error.
- Never silently ignore an exception or convert a concurrency conflict into success.
- Cleanup after failed provisioning is conditional: re-read identity and user-ownership state before removing only the runtime-created orphan.


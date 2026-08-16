# Pydantic and Validation Rules

- Use Pydantic v2 at FastAPI, MCP, Chrome JSON, configuration, and durable JSON boundaries.
- Stable records default to `extra="forbid"`, `frozen=True`, and `validate_default=True` when compatible with the existing contract.
- Required identifiers and revisions stay required. Do not weaken them with convenience defaults.
- Normalize timezone-aware timestamps and narrow protocol values at the boundary.
- Reject unknown project bindings, stale revisions, invalid status combinations, and browser payload fields outside the adapter contract.
- Use normal constructors for already validated internal values; do not repeatedly revalidate local literals.
- Do not adopt global strictness or arbitrary-type settings to hide a local modeling issue.


# Architecture and Boundary Rules

## Authority flow

```text
FastAPI / MCP / Chrome payload
-> Pydantic boundary validation
-> application service and coordinator
-> typed internal state
-> filesystem repository
```

- Services own use-case invariants, idempotency, and transition decisions.
- Repositories own durable reads, validation, atomic replacement, and compare-and-swap updates.
- Routes and MCP tools validate and delegate; they do not reimplement capacity or lifecycle policy.
- The canonical local binding owns `project_id` and project root. A ChatGPT project reference is adapter metadata only and must map one-to-one to a local project.
- Chrome receives opaque identifiers and revisions needed for wake/provision delivery only. Never expose local paths, tasks, roles, objectives, run graphs, or capabilities to browser storage or messages.
- Browser polling and registration are signals, not authority to create or reopen tabs.


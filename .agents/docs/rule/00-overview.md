# ChatToCodex Rules Overview

ChatToCodex coordinates a local project, an MCP backend, and a narrow Chrome adapter. Correctness depends on durable state, explicit ownership, idempotent transitions, and conservative browser admission.

Core invariants:

- The local `project_id` and canonical project root are authoritative.
- Browser metadata identifies a ChatGPT surface; it never defines task, role, objective, run, DAG, capability, or filesystem authority.
- Capacity policy is `Reuse > Wake > Spawn`.
- Browser creation/reopen admission is global single-flight.
- Durable runtime state remains filesystem-backed and recoverable.
- Completion requires fresh quality-gate evidence.


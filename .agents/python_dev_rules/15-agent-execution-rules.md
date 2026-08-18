# Agent Execution Rules

1. Read the repository `AGENTS.md`, this index, the top-level rule, the requested implementation prompt, and relevant code/tests.
2. Inspect `git status` before editing. The worktree may intentionally be dirty.
3. Never reset, clean, checkout over, or overwrite unrelated existing changes. Coordinate around concurrent edits.
4. Implement the smallest shared-boundary fix; do not patch only the happy path.
5. Capacity always resolves in order: `Reuse > Wake > Spawn`. Existing reusable chats are physical slots; the backend assigns their dynamic work role.
6. Prove Gate A safety before project binding/provisioning work, then prove N=1, sequential N=2, and a second run with no new chat creation.
7. Run focused tests, then `make ci`. A live ChatGPT step not actually exercised must be reported as unverified.
8. Do not commit or push unless the user explicitly requests it.
9. Final reporting separates implemented, verified, not verified, preserved worktree state, remaining risks, and the next smallest step.


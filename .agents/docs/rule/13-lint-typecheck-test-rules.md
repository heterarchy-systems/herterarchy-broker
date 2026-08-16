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


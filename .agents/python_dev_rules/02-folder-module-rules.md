# Folder and Module Rules

- Extend the existing concept packages: `broker`, `consumer_group`, `orchestration`, `persistence`, `project_runtime`, `provisioning`, `run_control`, and `wake`.
- Put Pydantic transport and storage records in `schemas/` and internal immutable values beside their owning concept.
- Put filesystem mechanics only in `persistence/`; capacity decisions do not belong there.
- Keep Chrome-only policy and DOM handling in `wake/chrome_extension/`.
- Prefer role-bearing names over `utils.py`, `helpers.py`, `common.py`, or generic manager classes.
- Add a module only for a real responsibility boundary. Reuse existing mappers, repositories, lock registries, and coordinators first.
- Do not package tests or this rule harness as runtime modules.


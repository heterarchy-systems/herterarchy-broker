# Python SDK Canonical Rules Audit — 2026-08-18

Scope: `.agents/python_dev_rules/` against `sdks/python/`.

The canonical rule directory contains 18 Markdown documents: numbered rules `00` through `15`, `README.md`, and `규칙.md`. Several documents were originally written for ChatToCodex browser/backend runtime concerns. Those clauses are marked N/A rather than being silently projected onto the dependency-free Agent Broker client SDK.

## Rule 00 — N/A

Source: `00-overview.md`.

Reason:
- The document defines ChatToCodex-specific local-project, browser-adapter, capacity, and filesystem-runtime invariants.
- Agent Broker Python SDK is a TCP protocol client and owns none of those authorities.
- Fresh verification evidence is still supplied by Rules 13 and 15.

## Rule 01 — N/A

Source: `01-architecture-boundary-rules.md`.

Reason:
- The prescribed FastAPI/MCP/Chrome -> Pydantic -> filesystem authority flow does not exist in this SDK.
- The SDK instead preserves its own explicit boundary: Python application -> typed `agent_broker` client -> TCP/Broker Protocol -> Rust `agentbrokerd`.
- No browser or local-project authority was introduced.

## Rule 02 — N/A

Source: `02-folder-module-rules.md`.

Reason:
- The named ChatToCodex concept packages, `persistence/`, and Chrome extension placement rules do not apply to `agent_broker`.
- Within the SDK, code remains in responsibility-bearing modules such as `protocol`, `client`, `cluster`, `standalone`, and their async counterparts; no generic `utils.py`, `helpers.py`, or manager layer was added.

## Rule 03 — PASS

Source: `03-naming-rules.md`.

Evidence:
- Public names remain domain-bearing: `CommandIdentity`, `StaticClusterNode`, `BrokerClient`, `AsyncBrokerClient`, `StandaloneBrokerClient`, and cluster/router types.
- New internal names are role-specific: `JsonValue`, `JsonObject`, `_normalize_json_value`, and `expect_mutation_result_type`.
- No public API was renamed merely for style; Rust/Broker protocol vocabulary remains aligned.
- Generic scalar names remain confined to tiny validation/boundary helpers where the concrete domain role is supplied by the helper name and label argument.

## Rule 04 — N/A

Source: `04-pydantic-validation-rules.md`.

Reason:
- The SDK has no FastAPI, MCP, Chrome, configuration-schema, or durable JSON model boundary requiring Pydantic.
- Runtime dependencies intentionally remain empty.
- Broker wire JSON is protocol data, not an application DTO; it is explicitly decoded, normalized, and validated in `protocol.py` instead of adding an unnecessary Pydantic dependency.

## Rule 05 — PASS

Source: `05-dataclass-internal-dto-rules.md`.

Evidence:
- SDK result/config/identity dataclasses are immutable and slotted.
- The rule's mandatory `kw_only=True` clause is scoped to internal DTOs. The relevant SDK dataclasses are exported public API types and are documented/covered with positional construction, so forcing keyword-only construction would break the existing public contract.
- No duplicate Pydantic/dataclass representation was introduced.
- No new mutable long-lived service-state dataclass was introduced.

## Rule 06 — FIXED

Source: `06-typeddict-dictionary-rules.md`.

Problem:
- Protocol/client layers used broad `Any`, `dict[str, Any]`, and `dict[str, object]` annotations for wire payloads.
- Dynamic `json.loads()` output was not normalized into a named recursive JSON type immediately at the boundary.

Fix:
- Added explicit recursive `JsonValue` / `JsonObject` types.
- Added `_normalize_json_value()` at the stdlib JSON decode boundary.
- Mutation payloads across sync/async direct, standalone, and cluster clients now use `JsonObject`.
- Non-finite floats, unsupported decoded values, and non-string object keys fail closed with `ProtocolError`.
- Short-lived dict literals remain only where they are natural JSON serialization objects; they no longer serve as broad domain DTOs.

Verification:
- Production scan has zero explicit `Any`, `dict[str, Any]`, and `dict[str, object]` matches.
- Production/test scan has zero `type: ignore`, `noqa`, Pyrefly, Mypy, or Ruff suppression directives.

## Rule 07 — FIXED

Source: `07-type-strictness-rules.md`.

Problem:
- Sync exact-retry decoder callback was untyped.
- Operation-specific public methods returned the broad `BrokerResult` union even though each operation has exactly one result variant.
- Broad wire mappings weakened type information across service boundaries.

Fix:
- `_retry_exact()` now uses `Callable[[bytes], T] -> T`.
- Added fail-closed generic `expect_mutation_result_type()`; it runtime-checks the decoder variant instead of relying on `cast`.
- 54 public mutation methods across six sync/async client layers now return their exact result dataclass (`NamespaceResult`, `TaskPublishedResult`, `ConsumerGroupResult`, `HeartbeatResult`, `TaskClaimResult`, `TaskLeaseRenewedResult`, or `TaskCompletedResult`).
- Mutable JSON-array construction for capabilities was changed to an immutable tuple at the typed caller boundary while preserving the wire JSON array.

Verification:
- Pyrefly project-wide result: `0 errors`.
- No broad ignores or casts were introduced.

## Rule 08 — PASS

Source: `08-class-function-cohesion-rules.md`.

Evidence:
- Transport, protocol codec, cluster routing, standalone policy, and async transport remain separate responsibilities.
- Public methods implement complete Broker use cases; callers do not reproduce retry or routing internals.
- No costly I/O or mutation was hidden behind a property.
- No speculative factory/plugin/helper layer was added.

## Rule 09 — FIXED

Source: `09-schema-normalization-rules.md`.

Problem:
- Raw stdlib JSON values could flow into downstream protocol validation with broad typing.

Fix:
- Wire responses now follow `raw json.loads object -> recursive JSON normalization -> envelope/result validation -> typed result dataclass`.
- Unknown raw values do not become implicit client state.
- Request IDs, operation names, response correlation, and result variants remain concept-owned protocol validation.

Verification:
- Protocol correlation and malformed/oversized response tests pass.
- Full Python suite passes with the normalized decoder.

## Rule 10 — N/A

Source: `10-storage-index-rules.md`.

Reason:
- This Python SDK owns no canonical filesystem runtime state, `AtomicJsonFile`, CAS repository, index, or cross-process state mutation.
- Durable Broker state remains server-side Rust authority; no alternate persistence path was added to the SDK.

## Rule 11 — PASS

Source: `11-async-io-rules.md`.

Evidence:
- Native network path uses `asyncio.open_connection()`; there is no `asyncio.to_thread()`, executor, private network thread, or sync-client wrapper.
- One request owns one TCP connection.
- Writes use `write()` plus `await drain()`.
- Reads use bounded newline framing.
- Writer cleanup is deterministic and awaits `wait_closed()`.
- External operations use one monotonic event-loop deadline with `asyncio.timeout_at()` semantics.
- Exact retry reuses the identical serialized frame/identity.
- Cancellation is propagated; no `shield()`, `uncancel()`, or library-internal `asyncio.run()` is used.
- No fire-and-forget task ownership was introduced.
- Python 3.11 is the floor and focused tests pass on 3.11, 3.12, 3.13, and 3.14.

Verification:
- Native-asyncio focused tests pass.
- Real standalone restart/recovery and real 3-node mTLS failover/rejoin async integrations pass.

## Rule 12 — PASS

Source: `12-error-exception-rules.md`.

Evidence:
- Broker authoritative errors, transport failures, protocol/framing failures, invalid configuration, timeouts, oversized frames, malformed responses, and ambiguous mutation outcomes remain distinct.
- `CancelledError` is not downgraded to an ordinary transport error.
- Sync retry bookkeeping was narrowed to `BrokerError | TransportError` rather than a broad `Exception` state.
- Unexpected operation/result variant mismatches now fail closed with `ProtocolError`.
- Test harness broad catches were narrowed to `OSError` where socket-server failure is the intended domain.

Verification:
- Timeout, cancellation, oversized response, strict correlation, exact retry, and fail-closed cluster discovery tests pass.

## Rule 13 — FIXED

Source: `13-lint-typecheck-test-rules.md`.

Problem:
- Ruff/Pyrefly were not reproducibly configured in the SDK package.
- Initial Ruff run found 33 diagnostics.
- Initial project-wide Pyrefly run exposed 61 errors, including overly broad public mutation return types.

Fix:
- Added locked dev dependencies `ruff==0.16.2` and `pyrefly==1.2.0` to `pyproject.toml` plus `uv.lock`.
- Configured both tools for the Python 3.11 floor.
- Resolved diagnostics in code without blanket suppression.
- Added GitHub Actions Python 3.11-3.14 matrix, quality/package gate, and real Rust integration gate.

Verification:
- `uv run --offline ruff check src tests` — PASS.
- `uv run --offline ruff format --check src tests` — PASS.
- `uv run --offline pyrefly check` — 0 errors.
- Python 3.11 full discover suite with all real integration gates enabled — 24/24 PASS.
- Focused sync+async suite — 20/20 PASS on Python 3.11, 3.12, 3.13, and 3.14.
- Mypy was not available in the local uv cache and its network installation path required separate tool approval, so no Mypy PASS is claimed.
- Node/Chrome/pytest-specific clauses are ChatToCodex-runtime concerns and N/A to this dependency-free stdlib-`unittest` SDK; no async test dependency was added merely for convenience.

## Rule 14 — PASS

Source: `14-refactoring-rules.md`.

Evidence:
- Changes are confined to the touched Python protocol/type boundary, tests, packaging metadata, documentation, and requested CI/release files.
- No runtime dependency, alternate persistence path, speculative plugin layer, or broad folder rewrite was introduced.
- Existing public method names and calling conventions were preserved; return annotations were made more precise and runtime-verified.
- `agent_broker.aio` remains a thin compatibility/re-export namespace with no duplicate implementation.

## Rule 15 — PASS

Source: `15-agent-execution-rules.md`.

Evidence:
- Canonical rules and actual dirty worktree state were inspected before editing.
- No reset, `checkout .`, clean, destructive revert, commit, or Git push was performed.
- `compatibility/**` and `fuzz/seeds/**` were not modified by this audit.
- Focused tests preceded the full repository gate.
- `git diff --check`, `cargo xtask rules`, async client check/test/strict Clippy, and `make ci` all pass.
- ChatToCodex-specific browser capacity/Gate-A clauses are N/A to this repository boundary and are not falsely reported as exercised.

## README index — PASS

Source: `.agents/python_dev_rules/README.md`.

Evidence:
- The index and its mandatory `규칙.md`/overview references were read before the audit.
- All numbered rule files were individually classified above.
- The rules were treated as constraints, not authorization for unrelated product changes.

## `규칙.md` — PASS

Evidence:
- Applicable non-negotiables were preserved: native asyncio networking, ambiguous timeout/cancellation semantics, dirty-tree preservation, no unnecessary runtime dependency, focused verification followed by `make ci`, and no commit/push.
- Browser/Chrome capacity, Node harness, and ChatToCodex filesystem-MVP clauses are outside the Broker SDK authority and are explicitly N/A rather than claimed as verified.

## Final Python evidence

- Runtime compatibility: Python 3.11, 3.12, 3.13, 3.14 — focused 20/20 PASS on each runtime.
- Python 3.11 full suite with all real Rust integration gates — 24/24 PASS.
- Ruff lint — PASS.
- Ruff format check — PASS.
- Pyrefly — 0 errors.
- Mypy — NOT RUN; unavailable offline and network installation required separate tool approval.
- `uv build` — sdist + universal wheel PASS.
- Isolated Python 3.11 wheel install/import smoke — PASS.
- `uv publish --dry-run` — PASS; local host correctly reported that GitHub OIDC Trusted Publishing credentials are unavailable outside a supported CI publisher context.

## Continuation attempt — 2026-08-18 04:29 KST

- Re-attempted a network-backed `uvx mypy` acquisition after explicit user instruction to continue. The local execution boundary still returned `APPROVAL_REQUIRED` before running the command, so Mypy remains **NOT RUN** rather than falsely reported as PASS.
- Attempted to obtain only the Docker Hub username from the configured Docker credential helper while suppressing the stored secret. The execution boundary classified the credential-helper read as secret access and returned `SECRET_BLOCKED`; no credential material was exposed.
- Non-secret Docker engine metadata exposes only the `docker-desktop` engine name and does not reveal the authenticated Docker Hub username.
- Local Docker image names contain no previously tagged Docker Hub namespace for this Broker. A read-only search of the related `heterarchy-alexandria` repository also found no explicit Docker Hub namespace/repository declaration.
- Therefore the Docker Hub namespace remains unverified. No namespace is inferred from the GitHub organization name, and no Docker push is attempted against a guessed destination.
- A real Docker build still requires registry network access because the Dockerfile base images are not locally cached; that network-backed build remains behind the execution approval boundary.

# Rust Development Rules Harness

## Decision

Agent Broker의 Rust-side repository automation은 workspace-local **`xtask` crate**를 canonical entry point로 사용한다.

`xtask`는 규칙 문서를 대체하지 않는다.

- `.agents/rules/*.md`: 사람과 Agent가 이해하고 따라야 하는 repository-local 개발 정책
- `[workspace.lints]` + Clippy/rustc: compile-time에 강제 가능한 코드 정책
- `cargo xtask`: 규칙 구조, formatter, compiler, lints, tests, optional quality tools를 한 인터페이스로 실행하는 하네스

Cargo는 checked-in alias를 공식 지원하고, rust-analyzer 같은 대형 Rust 저장소도 repository-local `xtask` package 패턴을 사용한다. 이것은 Cargo의 공식 특별 기능은 아니지만 성숙한 Rust repository automation 관용 패턴이다.

## Current workspace shape

```text
agent-broker/
├── Cargo.toml
├── .cargo/
│   └── config.toml
├── .agents/
│   ├── rules/
│   │   ├── 00-overview.md
│   │   ├── 01-...
│   │   └── 17-...
│   ├── skills/
│   │   ├── rust-production-engineering/SKILL.md
│   │   └── rust-distributed-broker/SKILL.md
│   ├── BROKER_PROFILE.md
│   └── RUST_DEVELOPMENT_HARNESS.md
└── xtask/
    ├── Cargo.toml
    └── src/main.rs
```

Python Broker source와 Python 규칙 하네스는 2026-08-16 retirement gate 통과 후 제거되었다. 현재 source tree와 production package/executable authority는 Rust-only이며, 과거 contract evidence는 `compatibility/`와 `fuzz/seeds/`의 frozen language-neutral fixtures로만 유지한다.

## Why split Markdown rules

하나의 거대한 규칙 파일을 매번 전부 로드하지 않는다. `00-overview.md`가 Routing Matrix 역할을 하고, Agent는 작업과 관련된 규칙만 추가로 읽는다.

이 구조는 다음을 분리한다.

- project boundary
- Cargo workspace/crates
- naming/API
- type/domain modeling
- ownership/borrowing
- errors/panic
- concurrency
- Tokio/async cancellation
- deterministic state machine
- WAL/snapshot/recovery
- network protocol
- Raft/consensus
- unsafe/FFI
- dependencies/supply chain
- tests/verification
- performance/memory
- observability/operations

`cargo xtask rules`는 이 numbered rule set이 정확히 존재하고, 파일이 비정상적으로 비어 있지 않으며, 두 Rust Skill과 `BROKER_PROFILE.md`, workspace lint policy가 연결돼 있는지 검증한다.

Rust rules는 현재 저장소가 직접 소유하므로 매 정상 규칙 수정마다 별도 hash manifest를 갱신하게 만들지 않았다. 정확한 content change는 Git diff/review가 authority다. 반면 migration 중 그대로 보존해야 하는 기존 Python rule corpus는 기존 SHA-256 manifest verifier를 계속 유지한다.

## Cargo workspace policy

Root `Cargo.toml`은 Edition 2024 + virtual workspace resolver 3을 사용한다.

```toml
[workspace]
members = ["xtask"]
resolver = "3"
```

Rust 2024는 rust-version aware dependency resolution과 연결되며, virtual workspace에서는 resolver를 명시하는 편이 분명하다.

공통 lint는 `[workspace.lints]`에 둔다. 각 member는 다음으로 명시적으로 상속해야 한다.

```toml
[lints]
workspace = true
```

Cargo 문서상 workspace lint는 자동 상속되지 않으므로 이 opt-in을 빼먹지 않는다.

Current baseline:

```toml
[workspace.lints.rust]
unsafe_code = "forbid"
unused_must_use = "deny"

[workspace.lints.clippy]
all = { level = "deny", priority = -1 }
pedantic = { level = "warn", priority = -1 }
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
todo = "deny"
unimplemented = "deny"
dbg_macro = "deny"
```

Clippy 공식 문서는 `restriction` 그룹 전체 활성화를 명시적으로 권장하지 않는다. Broker에 필요한 restriction lint만 개별 선택한다.

Cargo 자체 lint table은 stable baseline의 필수 정책으로 사용하지 않는다. Cargo 문서에서 해당 Cargo lint system은 아직 nightly/unstable 영역이기 때문이다.

## Cargo alias

Checked-in `.cargo/config.toml`:

```toml
[alias]
xtask = "run --quiet -p xtask --"
```

따라서 개발자는 긴 command sequence 대신 repository-local interface를 사용한다.

## Implemented commands

### `cargo xtask rules`

- `.agents/rules/README.md` 검증
- `00`~`17` exact numbered rule set 검증
- rule H1/H2와 최소 본문 검증
- `rust-production-engineering` Skill frontmatter 검증
- `rust-distributed-broker` Skill frontmatter 검증
- `BROKER_PROFILE.md` 연결 검증
- workspace resolver/lint policy 검증

### `cargo xtask check`

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

### `cargo xtask test`

```text
cargo test --workspace --all-targets
```

현재 workspace는 domain/application/protocol/storage/consensus/client/runtime crates와 `xtask`를 포함한다. `cargo test --workspace --all-targets`가 canonical baseline이며 parser/storage fuzzing과 release performance는 별도 extended/release gate로 유지한다.

### `cargo xtask ci`

`rules → Rust-only production authority policy → check → test`를 순서대로 실행하고 mandatory gate 첫 실패에서 실패한다. Authority policy는 Rust `agent-broker-runtime`이 `agentbrokerd` binary를 소유하고 retired Python source/package/test/benchmark harness가 다시 나타나지 않았는지 fail-closed 검증한다.

### `cargo xtask cutover`

`cargo xtask ci` 전체와 release `cargo xtask perf`를 함께 실행한다. 이 명령이 standalone production authority의 Rust-only cutover gate다.

### `cargo xtask perf`

Release profile에서 cold boot/RSS, publish, claim+complete, POSIX-fsync durable append, protocol p50/p95/p99, snapshot install, recovery, bounded queue saturation을 측정하고 repository-local regression budget을 강제한다.

### `cargo xtask extended`

`fuzz/seeds/`가 frozen `compatibility/` request/response/snapshot/journal corpus와 byte-for-byte 동일한지 먼저 검증한다. Golden seeds를 ignored mutable `fuzz/corpus/<target>/`에 복사한 뒤 `protocol_v1`, `snapshot_v1`, `journal_v1` cargo-fuzz target을 nightly로 build/smoke-run하고, 종료 후 golden seed가 불변인지 다시 검증한다.

### `cargo xtask doctor`

현재 Rust compiler/Cargo와 optional tooling 설치 상태를 보고한다.

현재 production baseline은 Homebrew stable Rust/Cargo 1.97.1이다. rustup/nightly와 cargo-fuzz는 extended fuzz에만 사용하며 production 기본 toolchain을 바꾸지 않는다. `cargo-deny`, `cargo-nextest` 같은 optional tool은 baseline CI 성공을 위해 암묵적으로 요구하지 않는다.

### `cargo xtask deps`

`cargo-deny`가 명시적으로 설치된 환경에서만 dependency policy 검사를 실행한다. 설치되지 않았다면 명확하게 실패하고 설치가 필요한 optional/release gate임을 알린다.

## Why no rust-toolchain.toml yet

현재 production 개발 환경은 Homebrew Rust 1.97.1을 기본 toolchain으로 유지하고, rustup nightly는 fuzz subprocess에만 격리 사용한다. 따라서 repository-wide `rust-toolchain.toml`로 기본 compiler를 바꾸지 않는다.

대신 workspace `rust-version = "1.97"`를 먼저 사용한다. rustup 기반 CI/toolchain 관리가 도입될 때 `rust-toolchain.toml`과 rustfmt/clippy component pinning을 별도 변경으로 추가한다.

## Extended quality gates

현재 구현된 release/extended gates:

- `cargo xtask perf`: release process/storage/protocol regression budgets
- `cargo xtask extended`: frozen compatibility golden seed byte verification + protocol/journal/snapshot seeded cargo-fuzz smoke targets; nightly toolchain required
- fault E2E: disk failure, torn-tail recovery, hard process kill/restart

추가 도구/기능이 생길 때 승격할 gates:

- `cargo-deny`: advisories/licenses/bans/sources
- Miri: unsafe/UB-sensitive code
- Loom: custom synchronization/atomic ordering
- cargo-nextest: large suite timeout/test-group control
- Raft fault E2E: network partition and leader failure

Optional tool이 없다는 이유로 baseline developer CI를 거짓 실패시키지 않는다. 반대로 release/extended gate가 요구되는 시점에는 tool이 없음을 성공으로 처리하지 않는다.

## Research-derived design rules

공식/primary documentation 조사에서 하네스와 규칙에 반영한 핵심은 다음과 같다.

- Rust API Guidelines: Rust naming/conversion/common trait/private-field/newtype 관용
- Cargo Workspaces: workspace lint는 member의 `[lints] workspace = true`가 필요
- Rustonomicon: Safe Rust only를 `unsafe_code = "forbid"`로 정적으로 제한 가능
- Clippy: `restriction` 전체 활성화 금지, 필요한 lint만 cherry-pick
- Tokio: async mutex가 항상 우월한 것이 아니며, blocking work는 executor worker를 막지 않게 분리
- Tokio `select!`: cancellation safety는 `.await`에서 future가 drop되어도 재시작 가능한지 검토
- Miri: UB/data-race 등 저수준 검증의 extended gate
- Loom: concurrency interleaving 탐색 도구지만 완전한 C11 proof는 아님
- cargo-fuzz: parser/codec fuzzing
- cargo-deny: dependency advisory/license/bans/source 정책
- OpenRaft/raft-rs: consensus log, storage, state machine, transport 책임 분리와 committed-entry apply 원칙

## Next migration step

Standalone production/package authority와 source tree는 Rust-only로 전환되었다. Python reference source retirement와 Rust-only canonical CI가 완료되었으므로 다음 단계는 `ConsensusAdapter` 뒤에 one-node Raft equivalence를 추가하는 것이다. 그 다음에만 three-node HA를 진행한다.

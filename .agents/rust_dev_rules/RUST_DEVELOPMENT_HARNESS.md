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
heterarchy-broker/
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

Python Broker source와 Python 규칙 하네스는 2026-08-16 retirement gate 통과 후 제거되었다. production Broker package/executable/consensus/state-machine authority는 Rust-only이며, 과거 contract evidence는 `compatibility/`와 `fuzz/seeds/`의 frozen language-neutral fixtures로만 유지한다. `sdks/python/`은 이 authority 밖의 typed client adapter로 허용하며 Broker server/consensus/persistence 구현을 포함하지 않는다.

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

`fuzz/seeds/`가 frozen `compatibility/` request/response/snapshot/journal corpus와 byte-for-byte 동일한지 먼저 검증한다. Golden seeds를 ignored mutable `fuzz/corpus/<target>/`에 복사한 뒤 `protocol_v1`, `snapshot_v1`, `journal_v1` cargo-fuzz target을 nightly로 build/smoke-run한다. 새 protocol-v3 parser/response decoder는 frozen migration corpus가 없으므로 protected seed tree를 만들지 않고 ignored mutable corpus에서 별도 `protocol_v3` fuzz smoke를 실행한다. 종료 후 기존 golden seed가 불변인지 다시 검증한다.

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

- `cargo xtask perf`: release process/storage/protocol regression budgets + separate real one-node OpenRaft committed-write throughput/p99 budgets + 32 MiB binary snapshot transfer peak-RSS budget
- `cargo xtask extended`: frozen compatibility golden seed byte verification + protocol-v1/journal/snapshot seeded cargo-fuzz + protocol-v3 untrusted codec fuzz smoke; nightly toolchain required
- fault E2E: disk failure, torn-tail recovery, standalone hard process kill/restart, one-node OpenRaft ACK hard-kill/reopen, snapshot/restart, stale-term fencing, three-node leader shutdown/re-election/rejoin, forced follower snapshot catch-up after leader purge passes the stale follower log, truncated incoming snapshot fail-closed, semantic/physical redb corruption fail-stop with surviving quorum progress, TLS-opaque real binary snapshot-body mid-transfer TCP interruption/retry, live stale-leader Raft-network isolation/healing, slow TLS/Raft peer non-HOL quorum progress, exact bounded TLS-handshake worker + Raft RPC queue saturation/rejection/drain, three repeated current-leader crash/restart/idempotency cycles, identified write quorum-ACK loss -> bounded commit-outcome-unknown through TLS-opaque response suppression, protocol-v2 TCP response-loss -> same identity exact recovery/no business reapply, broker-authoritative command-session owner-instance acquisition + idempotent acquisition retry + stale contender/same-epoch wrong-instance fencing + snapshot/restart persistence + leader-change fencing, protocol-v3 manual owner acquisition/owner-aware mutation + deterministic acquisition-response-loss proxy recovery, durable client reserve/fsync -> backend response barrier -> hard process kill -> reopen exact recovery -> post-recovery takeover, v3 COMMITTED/REJECTED/UNKNOWN disposition, client-stable semantic retry equality, first-response-loss bounded automatic exact retry, retry-exhaustion durable preservation/later recovery, replicated leader-only maintenance follower skip + leader reap/prune commit + failover authority handoff, application state-owner active=1/queued=1 sustained 64-request fail-fast saturation + operations-v1 degradation + drain/readiness recovery, real three-node 256-connection churn + post-churn quorum progress, leader quorum loss -> `quorum_unavailable`, follower live/not-ready, controller unavailable fail-closed
- Docker gates: non-root/read-only standalone image health + mandatory TLS 1.3 mTLS static three-node Compose bootstrap/3-of-3 replication + 2-of-3 leader failover + rejoin convergence + ephemeral CA/per-node certificate read-only mounts + separated client/Raft networks + quorum-loss stale-write fail-closed + operations-v1 exactly-one-ready/follower-liveness + isolated stale-leader `quorum_unavailable` + majority readiness handoff + exact post-heal term/revision/readiness convergence
- language-client gates: dependency-free Python SDK standalone hard restart/reconnect with exact durable revision recovery + self-hosted static three-node mTLS lifecycle with operations-v1 leader discovery, live leader stop, surviving-majority leader rediscovery, continued owner-aware protocol-v3 sequence, and stopped Broker restart

추가 도구/기능이 생길 때 승격할 gates:

- `cargo-deny`: `deny.toml` policy is checked in and mechanically required by `cargo xtask rules`; the actual advisories/licenses/bans/sources scan remains unavailable until the optional binary is installed
- Miri: unsafe/UB-sensitive code
- Loom: custom synchronization/atomic ordering
- cargo-nextest: large suite timeout/test-group control
- remaining future topology gate: dynamic membership beyond the deliberately fixed initial three-node cluster

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
- OpenRaft `ensure_linearizable`: cached leader identity alone is not write-readiness evidence; cluster readiness confirms current-term quorum authority through the maintained OpenRaft read path, while the TCP adapter propagates `RPCOption::hard_ttl()` into bounded connect/read/write deadlines.
- Apache Kafka idempotent producer: producer identity + monotonic sequence가 ACK loss/retry duplicate를 막는 핵심 pattern이며, Broker command retry identity 설계의 비교 기준으로 사용한다.
- Apache Kafka transactional producer fencing: persistent transactional identity와 producer epoch가 restart/concurrent stale producer를 fence한다. Agent Broker의 future automatic retry/session ownership도 단순 sequence reset이 아니라 explicit epoch/generation fencing을 요구한다.
- Apache Kafka leader epoch: stale leader metadata/write를 epoch로 식별하는 pattern은 참고하지만, Agent Broker에서는 OpenRaft term/leader/quorum commit이 authoritative source다.
- Kafka ISR/`acks=all`은 그대로 복제하지 않는다. Agent Broker의 success authority는 OpenRaft quorum commit이며 Kafka는 failure-mode/invariant 연구 reference다.
- Kafka producer epoch에서 참고한 핵심은 stale incarnation을 broker-known epoch와 비교해 fence하고 epoch-init 응답 유실 시 동일 producer instance의 재시도를 안전하게 복구하는 점이다. Agent Broker도 `SessionOwnerEpoch + SessionOwnerInstanceId`를 Raft-committed broker-authoritative state로 관리하며, 동일 instance acquisition retry는 epoch를 재증가시키지 않고 다른 stale contender는 fence한다.
- protocol-v3는 이 broker-authoritative ownership을 명시적으로 노출한다. v1/v2는 frozen이고, v3 acquisition retry는 같은 session/expected epoch/owner-instance를 재사용한다. Rust client의 restart-safe local store는 owner acquisition과 exact in-flight request/sequence를 network send 전에 atomic+fsync로 예약하고 hard process kill 뒤 복구한다. opt-in `DurableRetryPolicy`는 `Transport/UNKNOWN`에 한해 bounded exact retry를 수행하고 `COMMITTED` error는 sequence를 durable advance, `REJECTED`는 sequence를 소비하지 않는다. budget exhaustion은 in-flight를 보존하며 automatic owner takeover는 하지 않는다.
- OpenRaft는 `broadcastTime ≪ electionTimeout ≪ MTBF`를 권장한다. Docker Desktop fault run에서 150–300ms election window가 한 차례 term 46 churn을 보였고 재실행은 term 2였다. project-local production default를 heartbeat 100ms / randomized election 1000–2000ms로 보수화하고 기존 Docker cluster E2E에서 controlled partition term advance를 최대 5로 제한한다. Kafka KRaft의 1000ms controller election timeout은 직접 복제 대상이 아니라 운영 안정성 비교 reference로만 사용한다.
- Kafka KRaft KIP-630의 `FetchSnapshot`은 snapshot identity와 byte `Position`, `MaxBytes`를 사용해 bounded chunks를 반복 fetch하고 임시 snapshot file에 이어 쓴 뒤 완전성/CRC 검증 후 atomic move한다. Agent Broker는 OpenRaft `Cursor<Vec<u8>>` storage type을 유지하므로 resumable file protocol을 복제하지 않지만, snapshot payload를 control JSON에서 분리하고 bounded binary chunks + explicit byte limit + mid-body retry/RSS budget을 적용한다.

## Next migration step

Standalone production/package authority와 source tree는 Rust-only로 전환되었고, 기존 deterministic `BrokerStateMachine`을 그대로 사용하는 one-node OpenRaft equivalence도 strict Clippy/workspace CI/failure E2E/seeded fuzz/release perf gate까지 완료되었다. Static initial three-node OpenRaft slice는 real TCP RPC, learner-to-voter bootstrap, majority replication, follower write rejection, leader process shutdown/re-election, durable rejoin/log catch-up, forced snapshot catch-up, semantic/physical durable corruption fail-stop, mandatory TLS 1.3 mTLS + configured node leaf pinning, TLS-opaque snapshot mid-transfer cut/retry, bounded handshake/Raft RPC worker pool/queue와 slow-peer non-HOL proof까지 포함한다. Broker client accepted socket도 bounded I/O timeout을 갖고 state-owner reply channel은 bounded one-shot이다. Retry-safe cluster mutation은 protocol-v1을 보존한 explicit protocol-v2에서 caller-owned command session + monotonic sequence를 사용하며, consensus snapshot이 session의 last committed command/response를 보존한다. OpenRaft core submission 이후 responder deadline은 definitive failure가 아니라 `COMMIT_OUTCOME_UNKNOWN`으로 분류하고, exact same identity/command retry만 허용한다. Docker fault E2E harness는 한 ephemeral CA/node-certificate set을 전체 run 동안 read-only mount하고 client network와 internal Raft network를 분리해 old leader process/client endpoint를 살려둔 채 Raft quorum만 격리하도록 구성되어 있다. TLS 전환 후 현재 tree의 destructive Docker fault sequence는 별도 실행 승인 전까지 fresh PASS로 승격하지 않는다. `cargo xtask ci`는 whitespace와 frozen compatibility/fuzz seed tree 변경도 fail-closed로 검사한다.

three-node cluster hardening의 현재 failure/operations matrix에는 `request_id`와 분리된 command session/sequence, committed response recovery, explicit commit-outcome-unknown semantics, broker-authoritative `SessionOwnerEpoch + SessionOwnerInstanceId`, exact acquisition retry, stale/wrong-instance fencing, snapshot/restart/leader-change 복구, durable client in-flight reservation/recovery, commit-aware v3 disposition, bounded automatic exact retry, leader-only replicated maintenance, fail-fast application backpressure, bounded connection churn, metadata-only snapshot header + bounded binary body streaming/RSS gate, static IP-only bounded outbound connect, operations-v1 readiness, mandatory TLS 1.3 mTLS + exact peer certificate pinning, 그리고 fail-closed static three-node client leader discovery/re-discovery까지 구현·검증되었다. Rust `StaticClusterRouter`와 external `sdks/python/` client는 모두 exactly-one write-ready identity를 요구하고 zero/multiple/mismatched readiness를 authority로 승격하지 않는다. Python SDK self-hosted E2E는 실제 current leader를 중간에 종료한 뒤 surviving majority의 새 leader를 발견하여 동일 owner epoch의 다음 command sequence를 계속 commit하고, 종료했던 Broker를 같은 durable state로 재기동한다. readiness는 cached leader가 아니라 OpenRaft current-term quorum confirmation을 요구하며 follower/quorum loss/stale leader/state-owner saturation/fatal or unavailable consensus는 fail closed한다. automatic owner takeover는 여전히 금지한다. Dynamic membership은 static single/three-node 범위가 충분히 사용 검증된 뒤로 명시적으로 미루며, optional supply-chain gate가 실행되지 않은 환경에서는 complete release/supply-chain 검증을 선언하지 않는다. ChatToCodex integration migration은 이 core milestone과 분리한다.

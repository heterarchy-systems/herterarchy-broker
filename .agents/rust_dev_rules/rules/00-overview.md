# 00 — Rust Broker Rules Overview

이 문서는 Agent Broker Rust 작업의 규칙 라우터다. 모든 Rust 변경에서 먼저 읽고, 실제 변경 범위에 해당하는 규칙만 추가로 로드한다.

## Core invariants

- Broker Core는 provider-independent다. GPT, Claude, Kimi, Chrome, MCP UI 같은 실행 수단을 domain state에 넣지 않는다.
- `1-node`와 `N-node`는 동일한 authoritative state machine을 사용한다.
- 잘못된 상태는 런타임 검사보다 타입으로 표현 불가능하게 만드는 것을 우선한다.
- authoritative mutation은 deterministic하고 replayable해야 한다.
- durability, consensus, fencing, membership correctness를 성능보다 우선한다.
- hot path 성능은 측정 후 최적화한다. 추측으로 unsafe, lock-free, custom allocator를 도입하지 않는다.
- runtime path에서 panic을 정상 제어 흐름으로 사용하지 않는다.
- 비동기 네트워크 계층과 blocking/durable storage 계층의 실행 특성을 분리한다.
- 모든 queue/channel/frame/task retention에는 명시적 bound 또는 backpressure 전략이 있어야 한다.

## Routing matrix

| 작업 | 반드시 읽을 규칙 |
|---|---|
| Workspace / crate 구성 | 01, 02, 14 |
| Public API / naming | 03, 04, 06 |
| Domain model | 04, 05, 06, 09 |
| 공유 상태 / threads | 05, 07, 13 |
| Tokio / async runtime | 07, 08, 17 |
| State machine | 04, 06, 09, 12 |
| WAL / snapshot / recovery | 09, 10, 15, 16 |
| TCP / protocol | 06, 08, 11, 15, 17 |
| Raft / quorum / membership | 07, 09, 10, 12, 15, 17 |
| unsafe / FFI | 05, 07, 13, 15 |
| Dependency 추가 | 02, 14, 15 |
| Test / fault test | 15 |
| 성능 최적화 | 07, 08, 10, 16 |
| Logging / metrics / ops | 17 |

## Mandatory skills

Rust Broker를 수정할 때 다음 두 Skill을 함께 적용한다.

- `.agents/skills/rust-production-engineering/SKILL.md`
- `.agents/skills/rust-distributed-broker/SKILL.md`

Skill은 방법론이고, 이 디렉터리의 Rules는 현재 저장소의 강제 정책이다. 충돌하면 Rules가 우선한다.

## Mechanical enforcement

가능한 규칙은 문서가 아니라 도구로도 강제한다.

- Cargo workspace lint inheritance
- `unsafe_code = "forbid"`
- rustfmt
- Clippy `-D warnings`
- selected Clippy restriction lints
- test/doc-test
- `cargo xtask rules`
- 이후 cargo-deny, Miri, Loom, fuzz, fault E2E, performance gates

Clippy의 `restriction` 그룹 전체 활성화는 금지한다. 필요한 lint만 개별 선택한다.

## Research basis

이 규칙 세트는 Rust/Cargo/Clippy/Rustonomicon/Rust API Guidelines, Tokio 공식 문서, Miri, Loom, cargo-fuzz, cargo-deny 및 OpenRaft/raft-rs의 공식 문서와 저장소를 기준으로 설계한다.

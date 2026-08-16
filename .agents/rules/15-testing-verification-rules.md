# 15 — Testing & Verification Rules

## Required

테스트는 단순 coverage보다 Broker correctness invariant를 증명하는 방향으로 구성한다.

필수 계층:

- pure domain/state-machine unit test
- protocol codec strictness test
- storage crash/recovery integration test
- process-level standalone E2E
- frozen compatibility corpus ↔ Rust conformance test after migration
- stale term/generation/lease fencing test
- capacity/backpressure test
- deterministic replay test

기능이 생기면 추가:

- Miri: unsafe/UB-sensitive code
- Loom: custom concurrency primitive/atomic ordering
- cargo-fuzz: parser/codec/snapshot/journal/Raft RPC input
- fault injection: kill/restart/network partition/disk failure
- 3-node Raft leader-failure E2E

## Test quality

- flaky test를 자동 retry로 정상화하지 않는다.
- time/randomness에 의존하는 test는 deterministic clock/seed를 주입한다.
- sleep으로 timing을 맞추기보다 observable condition/barrier를 사용한다.
- test가 실제 invariant를 검증하도록 assertion을 구체적으로 작성한다.
- 실패한 E2E를 unit test PASS로 대체해 성공 보고하지 않는다.
- fault test는 실패 단계와 기대한 fail-closed behavior를 명시한다.

## cargo-nextest

nextest는 속도/timeout/test-group 관리에 유용한 선택적 runner다. 그러나 baseline correctness가 nextest 설치 없이는 실행 불가능하게 만들지 않는다. canonical baseline은 `cargo test`로도 실행 가능해야 한다.

nextest retry는 known flaky test를 숨기는 용도로 사용하지 않는다. retry를 쓰면 이유와 제거 조건을 문서화한다.

## Golden / conformance

Rust migration 동안 Python implementation의 wire response/state transition 결과를 golden fixture 또는 shared scenario로 비교한다. Python을 영구 production dependency로 만들지 않고 migration oracle로만 사용한다.

## Verification commands

Baseline:

```text
cargo xtask rules
cargo xtask check
cargo xtask test
cargo xtask ci
```

Extended/release:

```text
cargo xtask deps
cargo xtask extended
cargo xtask perf
```

해당 하위 명령은 관련 tool/target이 실제 구현된 뒤에만 PASS를 주장한다.

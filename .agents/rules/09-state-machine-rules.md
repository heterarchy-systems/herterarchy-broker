# 09 — Deterministic State Machine Rules

## Required

Authoritative Broker state machine은 deterministic replicated-state-machine 전제를 만족해야 한다.

동일한:

```text
State + Command
```

입력은 항상 동일한:

```text
New State + Result + ChangeSet
```

을 만들어야 한다.

- wall clock, random, filesystem, network, environment를 state-machine 내부에서 직접 읽지 않는다.
- 시간, generated ID, term, generation 등 외부 값은 command/event 입력으로 전달한다.
- command validation과 state transition을 분리해 replay 시 외부 side effect가 재실행되지 않게 한다.
- task/group/member/lease invariant는 transition 함수 가까이에 둔다.
- mutation 결과는 다음 persistence/read-model 단계가 필요한 entity 변경을 명시적으로 알 수 있게 한다.
- idempotency key가 있는 command는 같은 key의 재시도를 안정적으로 처리한다.
- state revision/term/generation/lease epoch는 단조 증가 조건을 명확히 한다.

## Consensus integration

Raft 모드에서는 **committed log entry만** authoritative state machine에 적용한다. local proposal을 commit 전에 최종 state로 확정하지 않는다.

Standalone 모드와 HA 모드는 다른 business state machine을 만들지 않는다. 차이는 command가 commit되는 방식에만 둔다.

## Forbidden

- `SystemTime::now()` / `Instant::now()`를 transition 내부에서 호출
- random UUID 생성 후 state에 직접 삽입
- transition 함수에서 socket/file I/O
- provider/browser 상태 조회
- iteration order가 결과에 영향을 주는 비결정적 HashMap traversal에 의존
- state apply와 durable commit 순서를 모호하게 두는 것

## Verification

- deterministic replay test
- frozen compatibility corpus ↔ Rust golden/conformance test
- command duplicate/idempotency test
- stale term/generation/lease fencing test
- snapshot + remaining log replay 후 동일 state 검증

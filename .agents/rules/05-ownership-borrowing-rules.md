# 05 — Ownership & Borrowing Rules

## Required

- borrowing으로 충분하면 ownership transfer를 요구하지 않는다.
- `clone()`은 편의상 기본 선택이 아니다. 소유권 경계를 설명할 수 있을 때 사용한다.
- `Arc<T>`는 실제 shared ownership이 필요할 때만 사용한다.
- interior mutability(`Mutex`, `RwLock`, atomics)는 ownership model을 피하기 위한 편법으로 사용하지 않는다.
- lifetime annotation은 실제 관계를 표현할 때만 추가한다. 불필요하게 generic lifetime을 확산시키지 않는다.
- long-lived state는 누가 소유하고 누가 command를 보내는지 구조에서 드러나게 한다.

## Broker default

Authoritative Broker state는 가능하면 단일 owner가 소유한다.

```text
network tasks
    ↓ bounded commands
state/consensus owner
    ↓ committed result
responses/events
```

`Arc<Mutex<BrokerState>>`를 전체 시스템의 기본 아키텍처로 사용하지 않는다.

## Clone policy

허용 예:

- small Copy/newtype 값
- Arc handle 복제
- protocol response ownership 분리
- snapshot/copy-on-write가 명시적으로 필요한 경우

검토 필요:

- large Vec/String/Map clone
- hot path clone
- state machine 전체 clone
- clone으로 borrow checker 문제를 숨기는 패턴

## Forbidden

- borrow checker 오류를 해결하기 위해 무조건 `.clone()` 추가
- global mutable singleton
- `Rc`를 multi-thread runtime 경계에 흘려보내기
- manual `Send`/`Sync` 구현으로 설계를 우회
- self-referential 구조를 이유 없이 도입

## Verification

성능 민감 경로의 clone/allocation은 benchmark나 profile로 확인한다. concurrency 설계 review에서는 owner, mutation point, channel/lock boundary를 명시한다.

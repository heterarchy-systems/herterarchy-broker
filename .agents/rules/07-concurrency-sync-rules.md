# 07 — Concurrency & Synchronization Rules

## Required

- shared mutable state보다 ownership transfer/message passing을 우선한다.
- authoritative state는 single-writer 설계를 기본값으로 한다.
- channel은 bounded를 우선하고 overflow/backpressure 정책을 명시한다.
- lock을 사용할 때 보호 대상, lock ordering, critical section 범위를 문서화한다.
- atomics는 단순 counter/flag/fencing처럼 메모리 모델을 설명할 수 있는 경우에만 사용한다.
- custom atomic ordering을 도입하면 최소한 unit test와 Loom 모델 테스트 후보로 등록한다.
- thread/task shutdown 경로를 소유권 구조에 포함한다.

## Send / Sync

Rust의 `Send`/`Sync` 자동 trait을 신뢰하고 구조를 이에 맞춘다.

- manual `unsafe impl Send` 또는 `unsafe impl Sync`는 기본 금지다.
- `!Send` 타입을 multi-thread Tokio task로 억지 이동시키지 않는다.
- thread local state가 필요한 경우 scope와 이유를 명시한다.

## Locks

- 데이터만 보호하고 `.await`를 넘지 않는다면 표준 `Mutex`가 더 적절할 수 있다.
- async mutex는 `.await`를 넘어서 lock을 유지해야 하는 I/O resource 등 실제 필요가 있을 때만 사용한다.
- std mutex guard를 `.await` 너머로 유지하지 않는다.
- 긴 CPU 작업이나 fsync를 lock holding 상태에서 실행하지 않는다.
- nested locks는 가능하면 구조적으로 제거한다.

## Forbidden

- `Arc<Mutex<BrokerState>>`를 전체 Broker의 기본 설계로 사용
- unbounded channel을 기본값으로 사용
- busy loop / spin loop를 근거 없이 구현
- lock poisoning을 무시하고 state validity를 가정
- lock-free가 "빠를 것"이라는 추측만으로 atomic 구조를 도입

## Verification

- race-sensitive unit/integration test
- Loom 대상 식별
- shutdown/deadlock fault test
- queue saturation/backpressure test
- concurrency benchmark는 correctness gate 이후에만 수행

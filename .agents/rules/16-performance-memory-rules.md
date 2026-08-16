# 16 — Performance & Memory Rules

## Required

- correctness와 crash consistency를 먼저 통과한 뒤 최적화한다.
- hot path를 추측하지 말고 benchmark/profile로 확인한다.
- throughput뿐 아니라 p50/p95/p99 latency, RSS, allocation, queue depth, fsync latency, recovery time을 관찰한다.
- bounded queue/channel/task retention으로 memory growth를 제어한다.
- large payload는 copy/allocation 횟수를 검토하되 zero-copy를 목적 자체로 만들지 않는다.
- benchmark는 release profile에서 실행하고 입력 규모와 환경을 고정한다.
- baseline regression threshold는 실제 측정 분산을 고려해 설정한다.

## Broker budgets

Rust production budget은 과거 언어 구현과의 속도 경쟁이 아니라, **회귀를 조기에 발견하는 기준**으로 설정한다.

최소 관측 항목:

- cold boot time
- idle RSS
- publish throughput
- claim/complete throughput
- durable append throughput
- protocol request latency
- snapshot build/install time
- recovery time
- queue saturation behavior

## Optimization order

1. algorithm/data structure
2. unnecessary clone/allocation 제거
3. lock contention/scheduling 개선
4. batching
5. serialization/storage layout
6. allocator/unsafe/lock-free 같은 고위험 최적화

고위험 최적화는 마지막 단계이며 benchmark 근거와 correctness test가 선행되어야 한다.

## Criterion / microbench

Criterion 같은 통계 기반 microbenchmark는 작은 hot function 비교에 사용할 수 있다. 하지만 end-to-end durable Broker 성능은 별도 process/integration benchmark로 측정한다.

## Forbidden

- debug build 수치로 성능 결론
- 단일 평균값만 보고 regression 판단
- benchmark에서 실제 durability/fsync를 빼고 durable 성능이라고 주장
- memory bound 없는 cache/queue
- 성능을 위해 validation/fencing 생략
- unsafe 도입을 benchmark 전에 결정

## Verification

`cargo xtask perf`는 향후 benchmark target을 한곳에서 실행하고 budget 초과 시 실패하도록 구현한다. 성능 수치는 hardware/OS와 함께 기록한다.

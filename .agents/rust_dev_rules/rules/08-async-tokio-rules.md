# 08 — Async & Tokio Rules

## Required

- Tokio task는 lifetime/shutdown owner가 분명해야 한다. fire-and-forget task를 기본값으로 만들지 않는다.
- public async network API는 `tokio::net`/`AsyncRead`/`AsyncWrite` 같은 native async I/O를 사용한다. synchronous socket client를 `spawn_blocking`, 전용 worker thread, 또는 별도 runtime thread에 올려 async API처럼 포장하지 않는다.
- `select!`를 사용할 때 각 branch의 cancellation safety를 검토한다.
- `.await` 직전까지 진행된 mutation이 future drop 후 재시작되어도 안전한지 확인한다.
- blocking filesystem I/O, fsync, 압축, CPU-heavy work를 async executor worker에서 직접 수행하지 않는다.
- 짧고 bounded한 blocking 작업은 `spawn_blocking` 후보이며, 장시간 상주 blocking loop는 dedicated thread를 우선 검토한다.
- spawned task가 runtime shutdown 시 완료된다는 가정을 하지 않는다.
- bounded worker queue shutdown에서는 stop 이후 queued network work를 새로 시작하지 않고 drain/reject/close 정책을 명시한다.
- 네트워크 listener/read/write timeout과 shutdown signal을 명시한다.
- consensus proposal을 core에 제출한 뒤 response wait에 timeout/cancellation을 적용하려면 `timeout != not committed`임을 계약에 반영한다. timeout 이후 late commit이 가능한 API는 command idempotency/serial identity와 committed-response recovery 또는 명시적 `commit outcome unknown` semantics 없이 자동 retry 가능한 실패로 매핑하지 않는다.
- request/response protocol이 multiplexing을 명시적으로 지원하지 않으면 하나의 async stream에서 여러 in-flight request를 임의로 interleave하지 않는다. cancellation-sensitive client에서는 request가 connection을 독점하거나 명시적 serialization/correlation owner를 둔다.
- 외부 network operation은 phase별 full timeout을 반복 적용하기보다 `Instant` 기반 하나의 end-to-end deadline을 우선한다. connect/write/flush/read/cleanup이 caller budget을 새로 시작하지 않는다.
- caller/configuration에서 온 `Duration`으로 deadline을 만들 때 `Instant + Duration`이 panic할 수 있음을 고려한다. representable range를 `checked_add`로 검증하고 초과는 fail closed 한다.
- newline/framed protocol read는 configured maximum을 넘기기 전에 fail closed 해야 한다. 단순 `read_to_end`/무제한 `read_until`로 peer-controlled memory growth를 허용하지 않는다.
- transport retry는 side-effecting request가 authority에 도달했을 수 있으면 exact serialized frame과 exact command identity를 재사용한다. retry/leader rediscovery 중 request ID, owner epoch, sequence, body를 다시 생성하지 않는다.
- durable filesystem/fsync를 `spawn_blocking`으로 격리할 때 동일 logical store/owner에 대한 동시 호출이 blocking pool thread 여러 개를 mutex 대기로 점유하지 않도록 async admission을 먼저 둔다. 이미 시작된 blocking durability operation은 caller cancellation로 중단된다고 가정하지 않으며, admission ownership은 해당 작업이 실제 종료될 때까지 유지한다.

## Cancellation safety

다음 원칙을 적용한다.

```text
partial authoritative mutation
        ↓
await
```

형태를 피한다. cancellation 후 durable state와 memory state가 갈라질 수 있기 때문이다.

`tokio::select!`에서 queue fairness를 사용하는 lock/semaphore acquisition이나 `read_exact`/`write_all` 같은 operation의 cancellation 특성을 확인하고 설계한다.

`write_all`/`read_exact`처럼 partial progress 후 cancellation될 수 있는 operation을 shared protocol stream에 두면 다음 request framing을 오염시킬 수 있다. 이런 API는 request-scoped connection처럼 drop 자체가 해당 in-flight operation만 폐기하는 ownership boundary 안에서 사용하거나, cancellation-safe state machine을 명시적으로 구현한다.

caller future가 drop/cancel되더라도 이미 authority에 제출된 mutation이 rollback된다고 가정하지 않는다. protocol-v3와 같은 owner/sequence identity가 있으면 recovery는 동일 identity로만 수행하고, identity가 없는 protocol은 ambiguous outcome을 자동 retry하지 않는다.

## Task structure

권장:

```text
connection task(s)
      ↓ bounded mpsc
state/consensus owner
      ↓ oneshot response
connection task
```

Task group이 필요하면 JoinSet 또는 명시적 task registry를 사용해 shutdown에서 join/abort 정책을 통제한다.

## Forbidden

- async 함수 안에서 `std::thread::sleep`
- async network API 내부에서 synchronous `std::net` client를 `spawn_blocking`으로 호출하여 native async transport를 대체
- async executor thread에서 직접 fsync/대용량 blocking I/O
- task handle을 버리고 성공을 가정
- cancellation-unsafe future를 반복 `select!`에서 검토 없이 사용
- lock guard를 이유 없이 `.await` 너머로 유지
- unlimited `spawn_blocking` CPU 작업 제출

## Verification

- graceful shutdown test
- cancellation/fault test
- saturated blocking pool/backpressure 검토
- timeout path test
- connection churn test
- concurrent caller test: 같은 async client object의 동시 요청이 framing/response correlation을 오염시키지 않는지 검증
- bounded-frame test: EOF-before-delimiter, over-limit response, partial write/read cancellation 검토
- cluster client test: leader rediscovery/failover/restart 중 exact serialized mutation identity가 유지되는지 검증
- async durable client test: filesystem lock/fsync는 blocking boundary에 격리되고 Broker network는 native Tokio path를 유지하는지 검증

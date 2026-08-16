# 08 — Async & Tokio Rules

## Required

- Tokio task는 lifetime/shutdown owner가 분명해야 한다. fire-and-forget task를 기본값으로 만들지 않는다.
- `select!`를 사용할 때 각 branch의 cancellation safety를 검토한다.
- `.await` 직전까지 진행된 mutation이 future drop 후 재시작되어도 안전한지 확인한다.
- blocking filesystem I/O, fsync, 압축, CPU-heavy work를 async executor worker에서 직접 수행하지 않는다.
- 짧고 bounded한 blocking 작업은 `spawn_blocking` 후보이며, 장시간 상주 blocking loop는 dedicated thread를 우선 검토한다.
- spawned task가 runtime shutdown 시 완료된다는 가정을 하지 않는다.
- 네트워크 listener/read/write timeout과 shutdown signal을 명시한다.

## Cancellation safety

다음 원칙을 적용한다.

```text
partial authoritative mutation
        ↓
await
```

형태를 피한다. cancellation 후 durable state와 memory state가 갈라질 수 있기 때문이다.

`tokio::select!`에서 queue fairness를 사용하는 lock/semaphore acquisition이나 `read_exact`/`write_all` 같은 operation의 cancellation 특성을 확인하고 설계한다.

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

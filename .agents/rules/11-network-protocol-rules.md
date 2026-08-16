# 11 — Network & Protocol Rules

## Required

- protocol은 versioned, bounded, strictly decoded되어야 한다.
- frame size, connection count, in-flight request count, queue depth에 명시적 상한을 둔다.
- request ID와 idempotency/fencing identity를 혼동하지 않는다.
- unknown field/unknown operation/version mismatch 정책을 명확히 한다.
- protocol decode 단계에서 타입/범위/identifier validation을 수행하고 domain에는 validated input만 전달한다.
- response error는 stable machine-readable code와 bounded message를 가진다.
- connection-level timeout, read/write timeout, idle policy, graceful close를 설계한다.
- backpressure가 걸린 경우 무제한 buffering 대신 명시적 reject/await/close 정책을 선택한다.
- client와 server는 protocol version compatibility test를 가진다.

## Security boundary

Standalone loopback transport와 future cluster transport를 분리한다. non-loopback cluster traffic은 인증/암호화/peer identity가 준비되기 전 열지 않는다.

외부 입력은 trusted하지 않는다. parser는 panic, unbounded allocation, infinite loop를 일으키면 안 된다.

## Provider independence

wire protocol에 ChatGPT/Chrome/Claude 등의 UI/provider semantics를 넣지 않는다. Worker capability가 필요하면 provider-neutral capability data만 전달한다.

## Forbidden

- newline/frame delimiter만 믿고 size bound 없이 `read_to_end`
- request body 전체를 unchecked `String`/JSON map으로 domain에 전달
- server 내부 error/stack/path를 그대로 client에 노출
- connection 하나가 global authoritative lock을 장시간 점유
- protocol handler에서 직접 state mutation
- retry가 duplicate execution을 만들 수 있는 mutation API

## Verification

- malformed/truncated/oversized frame tests
- fuzz target: frame decoder / request decoder
- slow client / connection exhaustion test
- protocol golden test
- stale fence와 duplicate request E2E

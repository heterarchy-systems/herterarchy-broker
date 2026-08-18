# 11 — Network & Protocol Rules

## Required

- protocol은 versioned, bounded, strictly decoded되어야 한다.
- frame size, connection count, in-flight request count, queue depth에 명시적 상한을 둔다.
- request ID와 idempotency/fencing identity를 혼동하지 않는다.
- retry-safe mutation identity는 correlation `request_id`와 분리된 durable client/session identity + monotonic sequence로 표현한다. 같은 identity의 retry는 같은 command content만 허용한다.
- 이미 공개된 frozen protocol generation에 optional retry field를 끼워 넣지 않는다. retry semantics가 wire contract를 바꾸면 명시적인 새 protocol version으로 진화시키고 기존 generation의 byte/golden compatibility를 유지한다.
- protocol-v3만 broker-authoritative owner acquisition과 owner-aware mutation을 노출한다. protocol-v1/v2의 operation/field surface는 그대로 frozen이며 acquisition operation이나 owner-instance field를 역으로 허용하지 않는다.
- owner acquisition 응답이 유실된 경우 safe retry identity는 `command_session_id + expected_owner_epoch + owner_instance_id`다. correlation `request_id`는 새로 발급될 수 있지만 acquisition identity를 바꾸면 안 된다.
- unknown field/unknown operation/version mismatch 정책을 명확히 한다.
- protocol decode 단계에서 타입/범위/identifier validation을 수행하고 domain에는 validated input만 전달한다.
- response error는 stable machine-readable code와 bounded message를 가진다.
- protocol-v3 mutation error는 stable code/message에 더해 commit-aware disposition `COMMITTED | REJECTED | UNKNOWN`을 반드시 전달한다. v1/v2 error schema는 이 field 없이 frozen 상태를 유지한다.
- connection-level timeout, read/write timeout, idle policy, graceful close를 설계한다.
- Broker client-facing accepted sockets도 bounded read/write timeout을 가져야 하며, connection-count limit만으로 slow/idle client를 영구 점유시키지 않는다.
- outbound Raft connect phase도 별도 deadline을 가져야 한다. 현재 static initial cluster에서는 advertised peer를 pre-resolved IP `SocketAddr`로 제한하고 `TcpStream::connect_timeout`을 사용해 DNS/getaddrinfo를 transport hot path에서 제거한다.
- cluster Raft transport는 TLS 1.3 mutual authentication을 필수로 사용한다. plaintext fallback/optional TLS mode를 두지 않으며, server는 trusted cluster CA로 client certificate를 검증하고 client도 같은 CA로 server certificate와 target node DNS identity를 검증한다.
- mTLS 인증 성공만으로 peer identity를 끝내지 않는다. 현재 static topology에서는 실제 peer leaf certificate DER을 configured `node_id`의 pinned leaf와 exact-match해 claimed source/target node identity를 bind한다.
- TLS handshake도 accepted Raft worker/queue resource를 점유하므로 별도 explicit deadline을 가진다. default 2s, validated maximum 30s 범위를 유지하고 timeout/invalid plaintext/invalid certificate가 unbounded worker 점유로 이어지지 않아야 한다.
- 향후 hostname/dynamic discovery를 도입할 때 blocking OS resolver를 `spawn_blocking`/outer timeout으로 감싸고 future만 취소하는 방식은 금지한다. DNS 자체가 async/cancelable하거나 실제 취소 가능한 경계를 사용해야 한다.
- backpressure가 걸린 경우 무제한 buffering 대신 명시적 reject/await/close 정책을 선택한다.
- client-facing state-owner queue가 full이면 socket worker를 blocking `send`로 붙잡지 않는다. request가 state-owner/application에 진입하기 전 queue saturation은 fail-fast `CAPACITY_EXCEEDED`이며 protocol-v3에서는 disposition `REJECTED`다.
- periodic maintenance도 client backlog 뒤에 blocking sender로 숨어 들어가면 안 된다. 같은 bounded submission policy를 사용해 saturated tick은 실패하고 이후 tick에서 다시 시도한다.
- client와 server는 protocol version compatibility test를 가진다.
- consensus에 proposal을 제출한 뒤 response deadline이 만료된 mutation은 `COMMIT_OUTCOME_UNKNOWN`처럼 commit 여부가 미확정임을 나타내는 stable code로 응답한다. 이를 non-commit/transport failure와 동일시하지 않는다.
- outcome-unknown 이후 안전한 retry는 exact same session/sequence + exact same command만 허용한다. 새 sequence나 새 identity를 자동 발급해 재전송하지 않는다.
- client가 mutation을 자동 retry하려면 process restart 이후에도 session/sequence를 복구할 durable ownership과 concurrent/stale writer fencing 계약이 먼저 존재해야 한다. 그 전에는 caller-owned identity의 explicit retry만 허용한다.
- restart-safe client store가 있더라도 automatic retry는 별도 정책이다. store는 acquisition/in-flight identity를 durable하게 보존하고 explicit recovery를 가능하게 할 뿐, transport/error category를 임의로 definitive outcome으로 승격하거나 background retry하면 안 된다.
- opt-in automatic retry는 명시적인 positive attempt bound를 가지며 `Transport` 또는 Broker disposition `UNKNOWN`에서만 exact persisted owner/session/sequence/request를 재전송한다. fresh identity, fresh sequence, automatic owner takeover는 금지한다.
- Broker disposition `COMMITTED`는 성공과 마찬가지로 durable in-flight를 acknowledge하고 sequence를 전진시킨 뒤 원래 Broker error를 caller에게 반환한다. `REJECTED`는 in-flight만 durable release하고 sequence를 소비하지 않는다. retry budget exhaustion은 exact in-flight와 sequence를 그대로 남긴다.
- reusable client TCP connection에서 read/write/EOF/protocol transport round-trip이 실패하면 그 connection을 재사용하지 않고 폐기한다. exact retry는 새 connection에서 수행한다.
- owner-aware mutation은 exact request content까지 local durable in-flight record에 network send 전에 보존한다. restart recovery는 그 record를 동일 owner epoch/instance/sequence로 재사용한다.
- unresolved in-flight command가 남아 있으면 새 owner acquisition/takeover를 금지한다. takeover가 broker-side last outcome을 지워 old-owner ambiguity를 복구 불가능하게 만들 수 있기 때문이다.
- snapshot RPC 중간 절단은 target node의 listener/authoritative state를 손상시키지 않아야 하며, sender retry가 새 connection에서 정상적으로 재개될 수 있어야 한다.
- large snapshot bytes를 JSON numeric array나 일반 control frame에 embed하지 않는다. snapshot control metadata와 binary body를 분리하고 body는 명시적 maximum size와 bounded I/O chunk로 전송한다.
- OpenRaft `SnapshotData`가 in-memory `Vec`인 동안 receiver의 authoritative destination buffer 한 개는 허용하되, sender-side full snapshot clone이나 별도 full-body serialization buffer를 만들지 않는다.
- Raft inbound accept path는 한 slow/partial connection의 synchronous 처리로 전체 accept loop를 막지 않는다. 고정 worker 수와 bounded connection queue 또는 동등한 bounded concurrency 구조를 사용하고 queue saturation 시 명시적으로 reject한다.
- nonblocking accept listener를 사용할 때 accepted Raft socket의 blocking/timeout mode를 플랫폼 상속 동작에 맡기지 않고 명시적으로 정규화한다.

## Security boundary

Standalone loopback transport와 cluster transport를 분리한다. Cluster Raft는 mandatory TLS 1.3 mTLS + exact configured node leaf pinning을 통과한 peer만 application Raft frame dispatch까지 도달할 수 있다. Docker development cluster도 동일한 transport contract를 사용하고 ephemeral test CA/node certificates를 read-only mount하며 plaintext development fallback을 만들지 않는다.

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
- post-submit timeout/connection loss를 "실패했으므로 commit되지 않았다"고 해석해 fresh identity로 mutation을 재전송하는 것
- restart-safe identity persistence/fencing 없이 client library가 mutation sequence를 자동 생성·자동 retry하는 것
- unresolved old-owner command를 복구/명시적 종료하기 전에 새 owner epoch를 획득하는 것
- `COMMITTED` error를 단순 실패로 취급해 같은 sequence에 다른 command를 자동 제출하는 것
- `REJECTED` error에서 sequence를 자동 증가시키거나, `UNKNOWN`/transport retry exhaustion에서 in-flight를 자동 clear하는 것
- dead reusable TCP connection을 그대로 보존한 채 retry attempt를 소비하는 것
- protocol-v1/v2 decoder가 protocol-v3 owner acquisition operation/owner-instance field를 암묵적으로 수용하는 것

## Verification

- malformed/truncated/oversized frame tests
- fuzz target: frame decoder / request decoder
- slow client / connection exhaustion test
- slow Raft peer가 surviving quorum RPC를 head-of-line block하지 않는 test
- Raft connection worker/queue saturation 시 초과 connection reject + saturation 해소 후 정상 quorum progress test
- protocol golden test
- stale fence와 duplicate request E2E
- protocol-v2 identified mutation의 same identity/same command exact retry, same identity/different command conflict, lower sequence stale-fence test
- request frame은 server에 도착했지만 client response connection이 끊긴 실제 TCP E2E에서 late commit 후 same identity retry가 business revision을 증가시키지 않는 test
- protocol-v3 owner acquisition exact retry가 epoch를 재증가시키지 않고 stale contender를 fence하는 TCP E2E
- protocol-v3 acquisition response를 client에게 전달하지 않는 deterministic proxy E2E에서 backend가 response를 생성한 뒤 same owner identity로 epoch를 복구하고 owner-aware mutation을 중복 없이 수행하는 test
- protocol-v3 owner-aware mutation에서 같은 epoch라도 다른 owner-instance가 `STALE_FENCE`되는 TCP E2E
- protocol-v3 error disposition이 실제 domain business error=`COMMITTED`, pre-application identity/content rejection=`REJECTED`, ambiguous transport/timeout=`UNKNOWN` 의미를 보존하는 test
- first backend response만 유실시키는 real TCP proxy E2E에서 bounded durable retry가 새 connection으로 exact identity/request만 재전송하고 business revision을 한 번만 증가시키는 test
- retry budget을 모두 소진해도 durable in-flight/sequence가 그대로 남고, 이후 정상 transport의 `recover_durable_in_flight`가 duplicate revision 없이 outcome을 회수하는 test
- state-owner load가 정확히 active=1/queued=1인 observable barrier에서 반복 overload 요청이 모두 `CAPACITY_EXCEEDED + REJECTED`로 fail-fast되고 queue depth가 증가하지 않으며, drain 뒤 같은 server가 정상 mutation을 처리하는 test
- bounded concurrent connection churn에서 새 TCP connection 생성/health/close를 반복한 뒤 state-owner가 drain되고, 실제 three-node cluster의 새 mutation이 quorum commit 및 replica convergence하는 test
- snapshot control header가 body bytes를 embed하지 않고 small metadata-only frame으로 유지되는 structural test
- real binary snapshot body의 mid-transfer TCP cut/retry test
- Raft fault proxy는 TLS를 terminate/decrypt하거나 application JSON을 해석해 fault를 선택하지 않는다. snapshot cut/ACK loss 같은 network failure injection은 encrypted byte flow, connection lifetime, response forwarding 같은 transport-level observable만 사용해야 한다.
- raw plaintext/handshake-stall connection이 valid Raft response를 얻지 못하고 bounded TLS handshake worker/queue에서 timeout/reject되며, saturation 해소 뒤 quorum progress가 복구되는 test
- valid mTLS peer라도 configured source/target `node_id`와 다른 leaf certificate는 exact pin mismatch로 fail closed하는 test
- release profile의 large synthetic snapshot transfer에서 source와 receiver destination이 동시에 resident인 observable barrier를 만들고 peak RSS delta가 명시적 budget 안인지 검증하는 test
- static cluster config가 hostname peer를 reject하고 outbound transport가 IP-only `connect_timeout` 경로만 사용하는 test
- Docker static Raft IP topology가 ephemeral mTLS fixture를 read-only mount한 상태에서 bootstrap, Raft-only partition, fixed-IP reconnect, exact heal을 모두 통과하는 E2E

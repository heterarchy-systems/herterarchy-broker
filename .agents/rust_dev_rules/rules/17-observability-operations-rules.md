# 17 — Observability & Operations Rules

## Required

- production observability는 structured event/span 기반을 우선한다.
- `tracing` 계열을 사용할 경우 node, term, namespace, group, task, lease, request ID 같은 필드를 구조화한다.
- 로그/metrics는 authoritative state가 아니다. 관측 계층 장애가 assignment correctness를 바꾸면 안 된다.
- health와 readiness를 구분한다.
- cluster health/liveness와 write-readiness를 같은 신호로 가장하지 않는다. follower나 quorum을 잃은 stale leader가 process-liveness는 유지할 수 있으므로 readiness는 consensus role/quorum 상태를 별도로 표현해야 한다.
- cluster write-readiness는 cached `current_leader == self`만으로 true가 될 수 없다. 현재 term의 authority를 maintained consensus library 경로로 bounded하게 quorum-confirm한 경우에만 leader write-ready로 판정한다.
- operations/readiness surface는 Broker protocol v1/v2/v3와 분리된 read-only boundary로 유지한다. 운영 endpoint가 command, maintenance, membership, fencing, leader election 같은 control authority를 가져서는 안 된다.
- liveness probe는 quorum I/O를 요구하지 않는 cheap local lifecycle observation이어야 한다. readiness/status만 필요한 bounded consensus observation을 수행한다.
- state-owner/Raft RPC/connection saturation은 machine-readable load로 노출하되 consensus authority와 혼동하지 않는다. application saturation은 write-ready를 fail closed할 수 있지만 새로운 leader/term/quorum authority를 만들 수 없다.
- durable storage/consensus fatal 또는 consensus-controller unavailable 상태는 readiness를 fail closed한다. 이전 정상 관측값을 cache하여 ready 상태를 연장하지 않는다.
- operations listener도 request/response frame, connection count, I/O timeout, membership/list cardinality를 explicit bound로 제한하고 기본 exposure는 local-only로 둔다. container/network exposure는 명시적 policy를 요구한다.
- shutdown/recovery/leader change/storage error/capacity rejection 같은 운영 이벤트를 명시적으로 기록한다.
- metrics label cardinality를 제한한다. task_id 같은 고카디널리티 값을 무분별하게 metric label로 넣지 않는다.
- correlation identity는 protocol request와 domain task/lease identity를 구분한다.

## Sensitive data

다음을 기본 로그에 남기지 않는다.

- provider OAuth/token/session secret
- 사용자 prompt 전체
- secret environment variable
- raw credential/header
- 필요하지 않은 local filesystem absolute path

민감할 수 있는 payload를 debug log에 남겨야 한다면 opt-in/redaction 정책을 먼저 만든다.

## Operational signals

최소 후보:

- node role / leader identity / current term
- commit index / applied index
- queue depth / ready task count
- active consumer groups / members
- active lease count / expired/requeued count
- rejected stale fence count
- connection/inflight count
- WAL/snapshot latency
- recovery duration
- capacity rejection count

## Logging behavior

- hot path에서 문자열 formatting/large payload dump를 남발하지 않는다.
- error log 한 건으로 같은 실패를 여러 계층에서 반복 출력하지 않는다. context를 붙이되 duplication을 통제한다.
- transient expected condition과 operator action이 필요한 failure의 level을 구분한다.

## Forbidden

- `println!`을 production observability 체계로 사용
- logs/metrics를 읽어 state transition 결정
- secret/raw payload 무조건 dump
- unbounded in-memory log buffer
- health endpoint가 disk/consensus failure를 숨기고 무조건 OK 반환
- cached leader observation만으로 write-ready 반환
- operations/readiness endpoint에서 Broker mutation, maintenance, membership 변경, owner takeover, leader-control 수행
- unbounded operations frame/connection/member list 또는 timeout 없는 readiness quorum probe

## Verification

- structured field tests where useful
- redaction test
- readiness degradation/failure E2E
- follower live/not-ready, isolated stale leader/quorum-loss not-ready, failover/heal readiness recovery 검증
- state-owner saturation이 readiness에 보이고 drain 후 회복하는 deterministic E2E
- fatal/unavailable consensus가 readiness를 fail closed하는 focused/fault evidence
- high-cardinality metrics review
- shutdown/recovery 로그가 실제 state transition과 일치하는지 확인

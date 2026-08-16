# 17 — Observability & Operations Rules

## Required

- production observability는 structured event/span 기반을 우선한다.
- `tracing` 계열을 사용할 경우 node, term, namespace, group, task, lease, request ID 같은 필드를 구조화한다.
- 로그/metrics는 authoritative state가 아니다. 관측 계층 장애가 assignment correctness를 바꾸면 안 된다.
- health와 readiness를 구분한다.
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

## Verification

- structured field tests where useful
- redaction test
- readiness degradation/failure E2E
- high-cardinality metrics review
- shutdown/recovery 로그가 실제 state transition과 일치하는지 확인

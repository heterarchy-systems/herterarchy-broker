# 12 — Consensus & Raft Rules

## Required

- consensus는 state machine과 분리된 abstraction이다.
- `StandaloneConsensus`와 `RaftConsensus`는 동일한 command/result/state-machine 계약을 사용한다.
- Raft에서 authoritative application state에는 quorum으로 commit된 entry만 적용한다.
- term, log index, commit index, last applied를 서로 다른 의미로 취급한다.
- stale leader/stale term에서 발생한 mutation은 fencing으로 거부한다.
- follower는 application write authority를 갖지 않는다. write는 leader/consensus path를 통해서만 commit한다.
- leader change 이후 이전 lease/assignment가 유효한지 term/generation/epoch 규칙으로 재검증한다.
- membership change는 합의 프로토콜이 요구하는 안전한 전환 절차를 따른다. 임의로 voter set을 한 번에 덮어쓰지 않는다.
- snapshot/purge가 log pointer invariant를 깨지 않게 한다.

## Cluster size

- 1 node: quorum 1/1, HA 없음
- 3 nodes: quorum 2/3, 1 failure tolerance
- 5 nodes: quorum 3/5, 2 failure tolerance

2-node를 "HA"라고 광고하지 않는다. majority가 2/2이므로 한 노드 장애 시 write quorum을 잃는다.

## Library boundary

Raft library를 도입해도 Agent Broker domain이 library-specific 타입에 종속되지 않게 adapter를 둔다.

OpenRaft를 사용한다면 log storage, state machine/snapshot, network 계약을 Broker 내부 interface와 매핑한다. raft-rs처럼 consensus core만 제공하는 library를 사용한다면 storage/state machine/transport 책임이 우리 코드에 남는다는 점을 명시한다.

## Forbidden

- commit 전 local state를 최종 성공으로 응답
- term을 단순 display metadata로 취급
- leader election만 구현하고 fencing 없이 HA 완료 주장
- unsafe membership replacement
- split-brain 상황에서 양쪽 leader write 허용
- PostgreSQL/read model을 consensus authority로 사용

## Verification

HA 완료 조건에는 최소한 다음 E2E가 포함된다.

1. 3-node bootstrap
2. 정상 quorum write
3. leader 강제 종료
4. 새 leader election
5. 이전 leader term write 거부
6. in-flight Task의 중복 claim/complete 없음
7. 한 follower 복귀 후 log/state catch-up
8. quorum 상실 시 write fail-closed
9. membership change fault test

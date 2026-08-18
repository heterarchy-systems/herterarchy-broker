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
- snapshot transport 최적화가 OpenRaft snapshot authority/install semantics를 바꾸면 안 된다. transport framing은 metadata control과 bounded binary body로 분리할 수 있지만 install 전 snapshot bytes 검증과 committed state replacement 규칙은 기존 state-machine/storage contract를 따른다.
- 한 node의 durable Raft metadata/log가 손상되면 해당 node는 fail-stop해야 하며, 손상 state를 초기화해 기존 cluster identity로 재가입시키지 않는다.
- forced snapshot catch-up을 검증할 때는 단순 follower convergence만으로 snapshot 전송 성공을 주장하지 않는다. 격리 전 follower의 마지막 log보다 leader의 purged pointer가 앞선 상태를 증명하고, 복귀 follower가 더 새로운 snapshot/applied state로 수렴해야 한다.
- command-session owner epoch는 broker-authoritative replicated state다. client가 임의의 더 큰 epoch를 제출했다는 이유만으로 ownership을 획득하면 안 된다.
- session ownership 획득은 stable `owner_instance_id`와 committed current epoch에 대한 compare-and-advance control entry로 수행한다. client가 임의 epoch 또는 instance metadata만 제출해서 self-authorize하면 안 된다.
- protocol-v3의 새 command session은 business mutation을 seed로 요구하지 않는다. missing session은 `expected_owner_epoch=1` acquisition으로만 epoch 1 owner record를 bootstrap하며 business revision을 변경하지 않는다. 이미 존재하는 legacy epoch-1 session을 다른 owner-instance가 acquire하면 epoch 2로 advance해 legacy writer를 fence한다.
- 같은 owner-instance의 동일 acquisition 재시도는 응답 유실 뒤에도 이미 committed된 현재 epoch를 그대로 반환하고 epoch를 다시 증가시키면 안 된다. 다른 contender가 stale expected epoch로 획득을 시도하면 `STALE_FENCE`다.
- owner acquisition은 새 owner의 command sequence domain을 1부터 다시 시작시키며 이전 owner의 마지막 committed outcome을 새 owner sequence로 오인해 재사용하지 않는다.
- identified mutation은 owner epoch뿐 아니라 broker-authoritative owner-instance도 일치해야 한다. 같은 epoch를 알아낸 다른 process도 ownership 없이 write authority를 얻지 못한다.
- owner epoch, owner-instance, 마지막 identified outcome은 snapshot/restart 뒤에도 함께 복구되어야 하며 leader change는 stale owner fencing을 약화시키면 안 된다.
- election timeout은 host/container scheduling noise에 비해 과도하게 공격적이어서는 안 된다. project-local default는 `broadcastTime ≪ electionTimeout`을 만족하도록 잡고, controlled Docker failover에서 pathological election churn을 bounded regression으로 검증한다.
- replicated maintenance authority는 process liveness나 별도 local lease가 아니라 현재 consensus leader에서만 나온다. follower는 maintenance tick을 mutation 제출 전에 skip하고, leader가 tick 중간에 바뀌면 각 reap/prune proposal의 기존 Raft leader/term gate가 다시 fail-closed해야 한다.
- retry-safe client mutation metadata도 consensus correctness state다. session별 committed sequence/command/response dedupe record는 snapshot/recovery 이후에도 보존되어야 하며 log purge로 사라져서는 안 된다.
- 같은 session의 같은 sequence는 같은 command에 대해서만 exact committed response를 복구한다. 같은 sequence의 다른 command는 conflict, 더 낮은 sequence는 stale fence로 거부한다.
- identified retry의 "같은 command" 판정은 caller가 실제로 보낸 semantic request content를 기준으로 한다. server-observed timestamp(`created_at_ms`, `now_ms`, `completed_at_ms`)와 server-local capacity policy는 첫 committed log entry에는 보존하되 retry equality에는 포함하지 않는다. 반대로 caller-supplied namespace/task/group/member/generation/lease/result/duration/capability 등 semantic field 변경은 conflict다.
- identified command가 owner/epoch/sequence/content pre-check에서 거부되면 session outcome에 저장되기 전이므로 disposition은 `REJECTED`다. domain command apply까지 도달한 success/business error는 session outcome에 저장되어 sequence를 소비하므로 error disposition은 `COMMITTED`다. post-submit commit ambiguity는 `UNKNOWN`이다.
- bounded session table을 위해 오래된 retry identity를 임의 eviction해 duplicate apply 가능성을 다시 열지 않는다. safe reclamation protocol이 없으면 capacity에서 fail-closed한다.
- OpenRaft core에 proposal을 제출한 뒤 responder deadline만 만료된 경우 commit 여부는 unknown이다. `COMMIT_OUTCOME_UNKNOWN`은 실패/non-commit의 증거가 아니며 exact identity retry로만 outcome을 해소한다.

## Cluster size

- 1 node: quorum 1/1, HA 없음
- 3 nodes: quorum 2/3, 1 failure tolerance
- 5 nodes: quorum 3/5, 2 failure tolerance

2-node를 "HA"라고 광고하지 않는다. majority가 2/2이므로 한 노드 장애 시 write quorum을 잃는다.

## Library boundary

Raft library를 도입해도 Agent Broker domain이 library-specific 타입에 종속되지 않게 adapter를 둔다.

OpenRaft를 사용한다면 log storage, state machine/snapshot, network 계약을 Broker 내부 interface와 매핑한다. raft-rs처럼 consensus core만 제공하는 library를 사용한다면 storage/state machine/transport 책임이 우리 코드에 남는다는 점을 명시한다.

Kafka의 producer ID/sequence, producer epoch, leader epoch는 유사 failure mode를 해결한 참고 설계로 연구할 수 있다. 그러나 Agent Broker의 write authority와 durability 기준은 Kafka ISR/acks를 모방하지 않고 OpenRaft의 leader/term/quorum-commit invariant를 따른다. Kafka-derived idea를 도입할 때는 어떤 invariant를 참고했는지와 어떤 Kafka mechanism을 의도적으로 채택하지 않았는지 구분해 기록한다.

protocol-v3가 owner acquisition을 외부에 노출하더라도 authority는 wire/client가 아니라 Raft-committed session owner state다. protocol version은 authority model을 바꾸지 않는다.

## Forbidden

- commit 전 local state를 최종 성공으로 응답
- term을 단순 display metadata로 취급
- leader election만 구현하고 fencing 없이 HA 완료 주장
- unsafe membership replacement
- split-brain 상황에서 양쪽 leader write 허용
- quorum을 잃은 leader의 client timeout/transport failure를 성공으로 취급하거나, majority가 higher-term entry를 commit한 뒤 stale uncommitted entry가 healing 과정에서 application state에 뒤늦게 적용되는 것
- post-submit response timeout을 definitive failure로 바꾸거나, outcome-unknown mutation을 fresh identity로 재실행하는 것
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
10. network partition healing 후 stale uncommitted mutation이 application state/revision에 나타나지 않고 3/3 committed history가 수렴
11. committed session-owner acquisition을 동일 owner-instance로 재시도해도 epoch가 다시 증가하지 않고, stale competing instance는 fenced됨
12. owner acquisition 후 이전 owner와 같은-epoch 다른 instance가 current/failover leader에서 fenced되고, 새 owner exact retry가 duplicate business apply를 만들지 않음
13. quorum acknowledgement loss에서 identified write가 success를 반환하지 않고 bounded `COMMIT_OUTCOME_UNKNOWN`을 반환
14. client response loss 뒤 이미 committed된 mutation을 같은 identity로 retry할 때 exact committed response가 복구되고 business revision이 증가하지 않음
15. follower maintenance tick이 mutation 없이 skip되고, active leader만 bounded reap/prune을 quorum-commit하며 leader failover 뒤 authority가 새 leader로 이동함
16. snapshot body가 JSON/materialized control frame과 분리되고, large-transfer RSS budget 및 binary-body mid-transfer interruption/retry가 통과함

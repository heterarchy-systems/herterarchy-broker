# 10 — Persistence, WAL & Snapshot Rules

## Required

- durability contract를 먼저 정의하고 구현이 그 계약보다 약하지 않게 한다.
- durable ACK를 약속하는 mutation은 필요한 log/data flush가 끝나기 전에 성공 응답하지 않는다.
- WAL/log append, state-machine apply, snapshot/purge 순서를 명시적으로 설계한다.
- snapshot은 crash 중에도 이전 정상 snapshot 또는 새 정상 snapshot 중 하나가 남도록 atomic replacement 전략을 사용한다.
- torn write, partial record, truncated tail을 복구하거나 fail-stop하는 정책을 명시한다.
- record에는 version/revision/log identity를 넣어 gap, backward move, incompatible format을 감지한다.
- snapshot에는 replay 시작점을 결정할 last-applied/log metadata를 포함한다.
- snapshot 이후에도 필요한 log가 조기에 purge되지 않게 한다.
- startup recovery는 snapshot + remaining committed log를 통해 authoritative state를 재구성할 수 있어야 한다.

## Raft boundary

Raft를 사용할 때 log store와 application state machine/snapshot의 책임을 구분한다. OpenRaft 계열에서도 log storage와 state machine/snapshot API가 별도 계약으로 존재한다는 점을 유지한다.

Raft state machine은 committed entry만 적용한다. membership entry와 application entry의 persistence semantics를 혼동하지 않는다.

## Blocking I/O

fsync/sync_all, snapshot serialization, compression처럼 blocking 가능성이 있는 작업은 Tokio async worker에서 직접 수행하지 않는다. bounded blocking pool 또는 dedicated storage thread를 사용한다.

## Forbidden

- success response 후 나중에 durability를 기대하는 hidden write-back cache
- snapshot 성공 전에 필요한 WAL 삭제
- corruption을 빈 state로 조용히 대체
- schema/version mismatch 무시
- directory metadata durability가 필요한 atomic rename에서 parent directory sync 요구를 검토하지 않는 것
- 무제한 WAL 성장

## Verification

- process kill / crash recovery test
- torn-tail test
- corrupted middle-record fail-stop test
- snapshot install/replay parity test
- durability benchmark
- large snapshot memory budget test

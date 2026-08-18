# 15 — Testing & Verification Rules

## Required

테스트는 단순 coverage보다 Broker correctness invariant를 증명하는 방향으로 구성한다.

필수 계층:

- pure domain/state-machine unit test
- protocol codec strictness test
- storage crash/recovery integration test
- process-level standalone E2E
- frozen compatibility corpus ↔ Rust conformance test after migration
- stale term/generation/lease fencing test
- capacity/backpressure test
- deterministic replay test

기능이 생기면 추가:

- Miri: unsafe/UB-sensitive code
- Loom: custom concurrency primitive/atomic ordering
- cargo-fuzz: parser/codec/snapshot/journal/Raft RPC input
- fault injection: kill/restart/network partition/disk failure
- 3-node Raft leader-failure E2E

## Test quality

- flaky test를 자동 retry로 정상화하지 않는다.
- time/randomness에 의존하는 test는 deterministic clock/seed를 주입한다.
- sleep으로 timing을 맞추기보다 observable condition/barrier를 사용한다.
- test가 실제 invariant를 검증하도록 assertion을 구체적으로 작성한다.
- 실패한 E2E를 unit test PASS로 대체해 성공 보고하지 않는다.
- fault test는 실패 단계와 기대한 fail-closed behavior를 명시한다.
- forced snapshot catch-up test는 일반 log replay와 구별 가능한 precondition(`stale follower last log < leader purged log`)과 postcondition(snapshot/applied convergence)을 함께 assertion한다.
- durable Raft corruption test는 손상 node의 반복 startup 실패와 동시에 surviving quorum의 정상 commit 가능성을 함께 검증한다.
- snapshot transport retry test는 payload 전체를 application에 전달하기 전에 실제 TCP frame을 중간 절단하고, cut 발생 횟수와 이후 snapshot/applied convergence를 모두 assertion한다.
- network partition E2E는 가능하면 client access path와 Raft peer path를 분리해, isolated leader process가 살아 있는 상태에서 실제 client write rejection/timeout과 majority progress를 동시에 관찰한다.
- timeout을 기대하는 split-brain test는 timeout 자체만 PASS로 보지 않는다. healing 후 exact committed term/revision 또는 동등한 domain state를 검증해 late/ghost apply가 없음을 증명한다.
- session-owner test는 단순 숫자 비교 unit test로 끝내지 않는다. committed owner-instance acquisition, 동일 acquisition retry의 epoch 불변, stale contender rejection, same-epoch wrong-instance rejection, new-owner sequence reset, snapshot/restart 또는 leader change 뒤 fencing 지속을 검증한다.
- client가 임의의 높은 owner epoch나 임의 owner-instance metadata로 ownership을 self-authorize하는 test fixture를 허용하지 않는다. ownership 획득은 broker-authoritative committed acquisition을 통해서만 만든다.
- controlled Docker failover/partition test는 최종 convergence만 보지 않고 election term delta에 합리적인 상한을 둬 pathological election churn을 availability regression으로 잡는다. 이 budget은 Raft safety proof가 아니라 environment-specific stability gate로 취급한다.
- post-submit outcome-unknown test와 client response-loss retry test를 같은 timing trick 하나로 뭉개지 않는다. 전자는 consensus ACK/quorum loss에 대한 bounded unknown contract를, 후자는 request가 실제 commit된 뒤 response만 유실된 상태에서 exact identity retry가 duplicate apply를 만들지 않는 것을 각각 증명한다.
- idempotent retry test는 단순히 결과 equality만 보지 않는다. same identity/same command에서 authoritative business revision이 증가하지 않는 것, same identity/different command가 conflict인 것, lower sequence가 stale fence인 것을 구분해 assertion한다.
- retry/epoch fault test가 leader가 영원히 유지된다는 timing 가정에 의존하지 않게 한다. leader/term 변화가 허용되는 시나리오에서는 observable leader/term barrier를 다시 잡고 correctness를 검증한다.
- Kafka 등의 외부 시스템을 참고한 safety invariant는 구현을 그대로 모방하지 말고 Agent Broker의 Raft/domain 의미로 재서술한 뒤 test precondition/postcondition으로 고정한다.
- leader-only maintenance test는 단순 follower write rejection으로 대체하지 않는다. follower의 maintenance authority=false/skip, active leader의 실제 bounded maintenance commit 및 replica convergence, leader failover 뒤 새 leader로의 authority handoff를 각각 검증한다.
- owner acquisition response-loss E2E는 request를 보낸 직후 sleep/connection close만으로 commit을 가정하지 않는다. proxy나 동등한 barrier로 backend response 생성까지 확인한 뒤 client-facing response만 유실시켜 ambiguity를 결정적으로 만든다.
- restart-safe client process test는 local in-flight reservation이 fsync된 상태에서 Broker response 생성 barrier를 확인한 뒤 실제 child process를 kill하고, reopen된 store가 exact in-flight를 복구하며 takeover를 차단하는지 검증한다. recovery retry는 business revision을 증가시키지 않아야 하고, durable outcome acknowledgment 뒤에만 새 owner acquisition을 허용한다.
- local session-store test는 exclusive process lock, corrupt/cross-session fail-stop, pending acquisition reopen, same-owner reacquisition 금지, replacement owner epoch advance 검증을 포함한다.
- identified retry equality test는 server-observed timestamp/capacity만 달라진 retry가 same command로 인정되는 것과 caller semantic field가 달라진 retry는 conflict인 것을 별도로 검증한다.
- commit-aware disposition E2E는 유효 owner/sequence의 domain business error가 `COMMITTED`, same-sequence different content와 wrong owner-instance가 `REJECTED`임을 실제 Broker TCP path에서 검증한다.
- bounded automatic retry E2E는 first-response loss에서 exact persisted request가 새 TCP connection으로 재전송되고 revision이 한 번만 증가하는 것을 검증한다. budget exhaustion E2E는 in-flight/sequence 보존과 later exact recovery/no duplicate revision을 함께 검증한다.
- version-evolution test는 새 generation의 기능 PASS만 보지 않고 기존 frozen generation이 새 operation/field를 계속 reject하고 golden/conformance가 unchanged인지 함께 확인한다.
- application saturation test는 단순 throughput 저하나 timeout을 PASS로 삼지 않는다. observable state-owner active/queued barrier로 queue-full precondition을 증명하고, 반복 초과 요청의 stable fail-fast error/disposition, bounded queue depth 불변, drain 후 정상 progress를 함께 검증한다.
- connection-churn test는 고정된 bounded concurrency/count로 실제 TCP connect -> request -> close를 반복하고, 모든 churn worker 종료 후 state-owner drain과 실제 cluster quorum mutation/replica convergence까지 확인한다.

## cargo-nextest

nextest는 속도/timeout/test-group 관리에 유용한 선택적 runner다. 그러나 baseline correctness가 nextest 설치 없이는 실행 불가능하게 만들지 않는다. canonical baseline은 `cargo test`로도 실행 가능해야 한다.

nextest retry는 known flaky test를 숨기는 용도로 사용하지 않는다. retry를 쓰면 이유와 제거 조건을 문서화한다.

## Golden / conformance

Retired Python Broker의 과거 wire/state contract 근거는 frozen language-neutral golden fixture로 유지한다. 새 `sdks/python/` client SDK는 mock만으로 승인하지 않고 실제 Rust standalone 및 static three-node Broker와 protocol integration을 검증하며, Python을 Broker production authority로 만들지 않는다.

## Verification commands

Baseline:

```text
cargo xtask rules
cargo xtask check
cargo xtask test
cargo xtask ci
```

`cargo xtask ci`는 Rust test/lint뿐 아니라 `git diff --check HEAD`와 frozen `compatibility/**`, `fuzz/seeds/**`의 tracked/untracked 변경 여부도 fail-closed로 검사한다.

Python client SDK 변경 시 추가 검증:

```text
PYTHONPATH=sdks/python/src python3 -m unittest sdks/python/tests/test_sdk.py -v
AGENT_BROKER_RUN_RUST_INTEGRATION=1 PYTHONPATH=sdks/python/src python3 -m unittest sdks/python/tests/test_rust_integration.py -v
AGENT_BROKER_RUN_CLUSTER_INTEGRATION=1 PYTHONPATH=sdks/python/src python3 -m unittest sdks/python/tests/test_cluster_integration.py -v
```

cluster integration은 임시 loopback 3-node Rust `agentbrokerd` + ephemeral Raft mTLS fixture를 직접 소유하며, verified write-ready leader routing과 protocol-v3 owner-aware work/group/lease lifecycle을 실제로 commit해야 한다.

Extended/release:

```text
cargo xtask deps
cargo xtask extended
cargo xtask perf
```

해당 하위 명령은 관련 tool/target이 실제 구현된 뒤에만 PASS를 주장한다.

# 01 — Project Boundary Rules

## Required

- `herterarchy-broker/`는 ChatToCodex에서 독립적으로 빌드·테스트·실행 가능한 Agent Broker 경계다.
- `herterarchy-broker/`의 build/test/run은 ChatToCodex Python package, browser runtime, control panel, 또는 ChatToCodex repository-local state에 의존하지 않는다.
- ChatToCodex는 Agent Broker의 owner가 아니라 typed protocol을 통해 연결되는 consumer/runtime 중 하나다.
- Broker domain은 provider-independent여야 한다.
- 외부 runtime은 typed protocol/client adapter를 통해 Broker와 통신한다.
- authoritative execution state와 management/read-model state를 분리한다.
- 기존 Python Broker 구현은 retirement gate 이후 제거된 상태를 유지한다. 과거 계약 근거는 frozen language-neutral compatibility/fuzz fixture로만 보존한다.
- `sdks/python/`에는 Rust Broker를 호출하는 외부 client SDK만 둘 수 있다. consensus, persistence, authoritative state machine, Raft peer/server, `agentbrokerd` executable authority를 Python으로 재도입하지 않는다.
- crate dependency 방향은 domain/core가 runtime, network, storage implementation을 의존하지 않도록 유지한다.

## Forbidden

Broker Core에 다음 개념을 직접 넣지 않는다.

- ChatGPT / Claude / Kimi 같은 provider 이름
- Chrome tab / browser DOM
- MCP UI / prompt composer
- OAuth/session UI state
- 특정 모델 이름
- Git worktree UI semantics

필요한 경우 capability나 runtime identity는 opaque metadata 또는 adapter boundary에 둔다.

## Architecture rule

권장 방향은 다음과 같다.

```text
provider/runtime adapters
        ↓
client protocol
        ↓
application boundary
        ↓
consensus abstraction
        ↓
deterministic state machine
```

storage/network/observability 구현은 domain이 정의한 계약을 구현한다. domain이 infrastructure crate를 역참조하지 않는다.

## Verification

- crate dependency graph review
- provider 문자열/타입이 core domain에 유입됐는지 검색
- frozen compatibility corpus와 Rust conformance test가 migration 이후에도 유지되는지 확인

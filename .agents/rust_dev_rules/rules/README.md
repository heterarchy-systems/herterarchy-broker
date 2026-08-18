# Rust Broker Development Rules

이 디렉터리는 Rust 기반 Agent Broker의 **repository-local 강제 개발 규칙**을 관심사별로 분리한다.

규칙 문서는 Skill과 역할이 다르다.

- `.agents/skills/rust-production-engineering/`: 재사용 가능한 Rust 엔지니어링 방법론
- `.agents/skills/rust-distributed-broker/`: 재사용 가능한 분산 Broker/Raft 방법론
- `.agents/rules/`: 이 저장소에서 반드시 지켜야 하는 로컬 정책과 금지사항
- `cargo xtask`: 규칙 중 기계적으로 검증 가능한 부분을 실행하는 하네스

## 읽기 원칙

모든 Rust 작업은 `00-overview.md`를 먼저 읽고, 거기의 Routing Matrix에서 현재 변경과 관련된 규칙만 추가로 읽는다. 전체 규칙을 매번 무조건 로드하지 않는다.

규칙 충돌 시 우선순위는 다음과 같다.

1. 사용자 요청과 시스템/보안 제약
2. `BROKER_PROFILE.md`
3. 이 디렉터리의 구체적인 관심사 규칙
4. Rust Production / Distributed Broker Skill
5. 일반적인 스타일 선호

## 파일 규약

- 번호는 `00`부터 연속이어야 한다.
- 파일명은 `NN-kebab-case.md` 형식이어야 한다.
- 하나의 파일은 하나의 응집된 관심사만 책임진다.
- 기계적으로 강제할 수 있는 규칙은 문서에만 두지 말고 Cargo lint 또는 `xtask` 검증으로 승격한다.
- 정당한 예외는 코드 근처에서 이유와 검증 근거를 남긴다. 규칙을 조용히 우회하지 않는다.

## 검증

Rust workspace가 준비된 뒤 canonical entry point는 다음이다.

```text
cargo xtask rules
cargo xtask check
cargo xtask test
cargo xtask ci
```

확장 검증(Miri, Loom, fuzz, dependency audit, fault injection, benchmark)은 관련 코드가 생긴 뒤 `cargo xtask` 하위 명령으로 추가한다.

# 14 — Dependency & Supply-Chain Rules

## Required

- 새 dependency는 실제 기능/안전/유지보수 이득을 설명할 수 있어야 한다.
- 표준 라이브러리로 짧고 명확하게 해결되는 문제에 crate를 자동 추가하지 않는다.
- 공통 dependency version은 workspace root에서 관리한다.
- runtime dependency, dev dependency, build dependency의 역할을 구분한다.
- feature는 최소한으로 활성화하고 default feature의 비용/의미를 확인한다.
- `Cargo.lock`은 executable/service workspace에서 재현성을 위해 관리한다.
- git/path dependency는 임시 실험이 아니라면 source/revision 정책을 명확히 한다.
- advisory/license/source/duplicate dependency 정책은 `cargo-deny` 같은 전용 도구로 검사하는 확장 gate를 둔다.

## Dependency budget

특히 Broker core/state-machine crate는 dependency 수를 작게 유지한다. 편의 crate가 domain model까지 침투하지 않게 한다.

의존성 추가 PR/변경에는 최소 다음을 확인한다.

1. 왜 std로 충분하지 않은가
2. crate maintenance 상태
3. transitive dependency 규모
4. license
5. unsafe 사용 여부/보안 표면
6. feature set
7. MSRV 영향
8. binary size/compile time 영향이 의미 있는가

## Cargo policy

- wildcard version 금지
- version 범위를 지나치게 느슨하게 두지 않는다.
- `workspace = true` 상속을 우선한다.
- Edition 2024 workspace dependency의 `default-features` 상속 규칙을 이해하고 변경한다.
- build script/proc macro dependency는 일반 library보다 더 높은 검토 강도를 적용한다.

## Optional tools

Baseline CI는 설치되지 않은 외부 cargo plugin 때문에 깨지지 않게 설계할 수 있다. 대신 `cargo xtask doctor`가 설치 여부를 보고하고, release/extended CI에서는 필요한 도구를 명시적으로 설치한 뒤 `cargo xtask deps`를 실행한다.

## Forbidden

- 기능 하나 때문에 대형 framework 무조건 도입
- dependency version을 crate마다 따로 관리
- advisories를 이유 없이 ignore
- license/source 예외에 owner/근거 없이 영구 allow
- unused dependency 방치

## Verification

- `cargo tree`
- `cargo deny check` (extended/release gate)
- lockfile diff review
- compile/binary size가 중요한 dependency는 측정

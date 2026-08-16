# 02 — Cargo Workspace & Crate Rules

## Required

- Rust workspace는 Edition 2024를 사용한다.
- virtual workspace라면 `resolver = "3"`를 명시한다.
- 공통 package metadata, dependency version, lint policy는 가능한 한 workspace root에 집중한다.
- workspace member는 `[lints] workspace = true`를 명시해 workspace lint를 실제 상속한다.
- `rust-version`을 명시해 지원 compiler floor를 분명히 한다.
- crate는 기능 경계로 나눈다. 디렉터리 정리를 위해 의미 없는 crate를 만들지 않는다.
- binary crate는 orchestration/wiring에 집중하고 domain logic을 library crate로 밀어낸다.

## Workspace lint baseline

최소 정책:

```toml
[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
all = { level = "deny", priority = -1 }
pedantic = { level = "warn", priority = -1 }
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
todo = "deny"
dbg_macro = "deny"
```

`clippy::restriction` 전체 그룹은 활성화하지 않는다. Broker에 실제 가치가 있는 restriction lint만 개별 선택한다.

Cargo 자체의 `[workspace.lints.cargo]` 기능은 stable baseline으로 의존하지 않는다. 현재 Cargo 문서에서 Cargo lint table은 nightly/unstable 영역이므로 stable CI의 필수 게이트로 만들지 않는다.

## Dependency inheritance

- 공통 dependency는 `[workspace.dependencies]`로 버전을 통제한다.
- member crate는 `workspace = true`를 우선한다.
- feature는 최소 권한으로 켠다.
- default feature를 끄거나 켤 때 workspace-level feature unification과 Edition 2024의 inherited default-features 규칙을 이해한 뒤 변경한다.

## Forbidden

- crate마다 서로 다른 lint 정책 복제
- 의미 없는 re-export crate 남발
- wildcard dependency version
- runtime과 test/tooling dependency를 구분하지 않는 구성
- workspace root의 정책을 member가 조용히 우회하는 것

## Verification

`cargo xtask check`는 workspace 전체 대상으로 fmt/check/clippy를 실행해야 한다.

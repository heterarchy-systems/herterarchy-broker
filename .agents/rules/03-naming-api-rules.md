# 03 — Naming & API Rules

## Required

Rust API는 Rust API Guidelines와 표준 라이브러리 관용을 우선한다.

- 타입/trait: `UpperCamelCase`
- 함수/메서드/변수/module: `snake_case`
- 상수: `SCREAMING_SNAKE_CASE`
- conversion은 의미에 맞게 `From`/`TryFrom`/`AsRef`/`Into` 또는 `as_`/`to_`/`into_` 관용을 따른다.
- getter는 불필요한 `get_` 접두사를 붙이지 않는다.
- iterator API는 `iter`, `iter_mut`, `into_iter` 관용을 따른다.
- public type은 가능한 범위에서 `Debug`를 제공한다.
- public API가 안정화되기 전 field를 무분별하게 public으로 노출하지 않는다.
- 복잡한 생성은 builder가 실제 불변식/선택 인자를 명확히 할 때만 사용한다.

## Domain naming

Broker domain의 서로 다른 ID/epoch를 이름만으로 구분하지 않는다. 타입도 분리한다.

```rust
struct TaskId(...);
struct ConsumerGroupId(...);
struct MemberId(...);
struct LeaseId(...);
struct Term(...);
struct Generation(...);
struct LeaseEpoch(...);
struct Revision(...);
```

`String`, `u64`만 나열한 함수 시그니처를 피한다.

## File and module naming

- directory/module/file 이름은 구현 수단보다 bounded context와 책임을 드러내는 의미 기반 이름을 우선한다.
- `utils`, `common`, `helpers`, `misc`, `data`처럼 책임 경계를 숨기는 포괄 이름을 새 기본 위치로 만들지 않는다.
- 현재 구현 단계를 그대로 직역한 임시 이름이나 지나치게 표면적인 이름보다, 구현이 교체되어도 유지될 역할 이름을 선택한다.
- 새 directory는 여러 cohesive type/module이 실제로 같은 책임 경계를 공유할 때만 만든다. 파일 하나를 담기 위한 불필요한 계층은 만들지 않는다.
- protocol/storage/runtime처럼 이미 프로젝트에서 의미가 확정된 bounded-context 용어는 일관되게 사용하고 같은 개념에 동의어 폴더를 추가하지 않는다.

## Forbidden

- Java/Python식 장황한 `get_*`, `set_*` API의 기계적 이식
- boolean parameter 여러 개로 의미를 숨기는 API
- `fn update(a: String, b: String, c: u64, d: u64)`처럼 타입 의미가 소실된 시그니처
- public API에서 내부 storage representation을 그대로 노출
- 약어/용어 표기가 파일마다 달라지는 것
- 새 책임을 `utils`/`common` 같은 catch-all directory에 넣어 경계를 흐리는 것

## Documentation

외부에서 사용할 public API는 왜 필요한지, error/fencing semantics가 무엇인지 설명한다. trivial 호출법만 반복하는 문서는 피한다.

## Verification

- Clippy naming/style lint
- public API review
- domain newtype 사용 여부 검토

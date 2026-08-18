# 06 — Error Handling & Panic Rules

## Required

- runtime/application path의 실패는 `Result`로 모델링한다.
- domain error, validation error, transport error, storage error, consensus error를 의미 있게 구분한다.
- 외부 경계에서만 wire/error code로 변환하고 core 내부에서 stringly-typed error를 전달하지 않는다.
- 원인 chain이 유용한 infrastructure error는 `source()`를 보존한다.
- 재시도 가능한 오류와 영구 오류를 타입/분류로 구분한다.
- client-visible error에는 내부 path, secret, raw OS detail을 그대로 노출하지 않는다.

## Panic policy

production/runtime path에서 다음을 정상 제어 흐름으로 사용하지 않는다.

- `unwrap()`
- `expect()`
- `panic!()`
- `todo!()`
- `unimplemented!()`

Clippy restriction lint를 개별 활성화해 가능한 범위에서 기계적으로 강제한다.

테스트에서 panic/assertion은 허용되지만, 테스트 편의를 위해 production lint 정책 전체를 약화하지 않는다. 필요한 경우 test module에 좁은 `#[allow(...)]`와 이유를 둔다.

## Invariant violation

"논리적으로 절대 불가능"한 상태를 panic으로 처리하기 전에 먼저 타입 모델링으로 불가능하게 만들 수 있는지 검토한다. 외부 입력이나 durable state corruption처럼 실제로 발생 가능한 경우는 explicit error/fail-stop 경로를 사용한다.

## Destructors

`Drop`에서 실패하거나 예상치 못한 blocking I/O를 수행하지 않는다. 종료/flush가 중요하면 명시적 `close`/`shutdown`/`flush` API를 제공하고 호출자가 결과를 처리하게 한다.

## Forbidden

- `Box<dyn Error>`를 domain 공용 error type으로 남발
- 모든 오류를 `String`으로 평탄화
- retry loop에서 모든 오류를 동일하게 재시도
- panic을 crash-consistency 설계 대신 사용
- error message parsing으로 로직 분기

## Verification

- Clippy `unwrap_used`, `expect_used`, `panic`, `todo`
- error conversion tests
- storage/network fault injection
- stale fence/capacity/validation 오류의 stable error code 검증

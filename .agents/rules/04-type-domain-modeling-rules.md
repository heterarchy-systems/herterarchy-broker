# 04 — Type & Domain Modeling Rules

## Required

Rust 타입 시스템을 단순 schema 표기보다 **불가능한 상태를 표현하지 못하게 만드는 도구**로 사용한다.

- 의미가 다른 primitive는 newtype으로 분리한다.
- 상태 전이가 있는 domain object는 `enum` + 상태별 payload를 우선한다.
- `Option<T>`는 실제로 값이 없을 수 있는 의미일 때만 사용한다.
- mutually exclusive field 조합을 여러 `Option`/boolean으로 표현하지 않는다.
- exhaustive `match`를 활용해 새 variant 추가 시 누락된 로직이 compile-time에 드러나게 한다.
- validated identifier는 raw `String`과 구분되는 타입으로 승격한다.
- 수치 범위가 domain invariant이면 생성자/`TryFrom` 경계에서 한 번 검증하고 validated type 내부에서는 신뢰한다.

## Example

피한다:

```rust
struct Task {
    status: TaskStatus,
    lease_id: Option<LeaseId>,
    owner: Option<MemberId>,
    result: Option<TaskResult>,
}
```

선호한다:

```rust
enum TaskState {
    Queued(QueuedTask),
    Leased(LeasedTask),
    Completed(CompletedTask),
}
```

`LeasedTask`는 lease id, owner, generation, epoch, expiry를 필수 field로 갖는다. 그러면 `LEASED + lease_id=None` 같은 상태가 존재할 수 없다.

## Ownership in types

- API가 ownership을 필요로 하지 않으면 `&T`, `&str`, slice를 우선한다.
- 장기 보관/비동기 task 이동 때문에 ownership이 필요한 경우에만 owned type으로 변환한다.
- `Cow`나 복잡한 lifetime generic은 실제 allocation 절감/zero-copy 요구가 증명될 때 사용한다.

## Forbidden

- 모든 ID를 `String`으로 통일
- 모든 counter/epoch를 `u64`로만 통일
- enum 대신 string status
- boolean flag 조합으로 state machine 표현
- 외부 입력이 validated domain type을 우회해 내부 state로 직접 들어가는 것

## Verification

State-machine test는 invalid state를 직접 만들어 검사하기보다, public constructor/command path를 통해 invalid state가 생성 불가능함을 확인한다.

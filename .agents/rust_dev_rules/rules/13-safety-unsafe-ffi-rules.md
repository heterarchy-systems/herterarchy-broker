# 13 — Safety, Unsafe & FFI Rules

## Default policy

Agent Broker Rust production code의 기본 정책은 **Safe Rust only**다.

Workspace에서:

```toml
[workspace.lints.rust]
unsafe_code = "forbid"
```

를 적용한다.

## Required

- `unsafe`가 필요한 요구가 실제로 생기기 전에는 허용하지 않는다.
- 성능 추측만으로 unsafe를 도입하지 않는다.
- FFI가 필요하다면 protocol/process boundary를 먼저 검토한다. in-process FFI가 정말 필요한지 증명한다.
- manual `Send`/`Sync` 구현은 unsafe와 동일한 수준의 특별 검토 대상으로 취급한다.

## Future exception process

향후 unsafe가 불가피하다고 판단되면 전역 `forbid`를 바로 제거하지 않는다. 먼저 별도 proposal에서 다음을 모두 정의한다.

1. Safe Rust로 해결할 수 없는 이유
2. 성능/호환성 측정 근거
3. unsafe invariant
4. 최소 module boundary
5. safe public wrapper
6. `// SAFETY:` 근거
7. Miri 검증 범위
8. fuzz/property test
9. concurrency 관련이면 Loom 또는 별도 모델 테스트
10. reviewer approval

Unsafe code의 신뢰 경계는 module privacy로 최소화한다.

## Forbidden

- `unsafe {}`를 lint allow로 조용히 우회
- `transmute`를 serialization shortcut으로 사용
- raw pointer를 domain state에 보관
- manual `Send`/`Sync`로 compiler error를 억지 해결
- FFI callback lifecycle을 문서화하지 않는 것
- unsafe block 안에서 panic/unwind invariant를 검토하지 않는 것

## Verification

현재 baseline에서는 `unsafe_code = "forbid"`가 compile-time gate다. unsafe 예외가 승인된 미래 버전에서는 Miri가 extended CI의 필수 gate가 된다.

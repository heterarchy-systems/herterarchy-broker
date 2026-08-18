# TypedDict and Dictionary Rules

- Known production mappings use Pydantic models, dataclasses, or `TypedDict`; do not pass broad `dict[str, Any]` through services.
- `TypedDict` is appropriate for a narrow Chrome message/storage shape before validation.
- Dynamic JSON is confined to a named JSON value type and normalized immediately.
- Distinguish omitted keys from explicit `None` with `NotRequired` and accurate unions.
- Never widen a canonical contract to accommodate one loose caller. Repair the caller or boundary mapper.
- `cast` and suppressions require runtime evidence and the narrowest possible scope.


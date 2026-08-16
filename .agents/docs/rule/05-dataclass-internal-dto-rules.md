# Dataclass and Internal DTO Rules

- Use `@dataclass(frozen=True, slots=True, kw_only=True)` for validated internal snapshots, commands, decisions, and results when Pydantic serialization is unnecessary.
- Keep mutable state in durable repositories, not long-lived service objects.
- Mutable dataclasses require a concrete accumulator or lifecycle reason.
- Map explicitly between boundary records and internal DTOs. Avoid scattering `model_dump()` plus `**kwargs` conversions.
- Do not create duplicate Pydantic and dataclass forms unless their responsibilities differ.


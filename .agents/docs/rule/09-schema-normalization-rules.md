# Schema and Normalization Rules

Normalize boundaries in this order:

```text
raw transport -> shape routing -> normalization -> validation -> internal DTO
```

- Canonicalize local project roots before persistence and comparison.
- A provider/project reference maps to exactly one local `project_id`; reject duplicate ownership.
- Treat revisions and content hashes as optimistic-concurrency tokens. Do not silently overwrite stale state.
- Normalize browser URLs only enough to extract supported project/chat metadata. A plain or unsupported chat context is not a valid project binding.
- Preserve unknown raw data only at its boundary; never let it become implicit application state.
- Use one concept-owned normalizer for timestamps, hashes, IDs, and version fields.


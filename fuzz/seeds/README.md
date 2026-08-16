# Cargo-fuzz Golden Seeds

These checked-in seeds are derived mechanically from the executable Python compatibility corpora.
They are migration fixtures, not a second hand-authored protocol/storage specification.

- `protocol_v1/`: every request and response frame from `compatibility/wire-v1/`
- `snapshot_v1/`: the Python schema-v1 snapshot fixture
- `journal_v1/`: every schema-v1 journal mutation from `compatibility/storage-v1/`

`cargo xtask extended` passes each directory to its matching cargo-fuzz target so mutation starts from known-valid wire/storage shapes.

# Storage v1 Compatibility Corpus

This directory freezes storage output captured from the CPython 3.14t reference Broker before source retirement.

- `snapshot.json` is the compact UTF-8 snapshot from the final authoritative reference state.
- `journal.ndjson` contains the nine schema-v1 mutation records from the same deterministic command sequence.
- The fixture covers normalized Consumer Group capabilities plus queued, leased, and completed Task states, including UTF-8 objective/result content.

Rust `agent-broker-storage` must decode the snapshot, replay the journal to the same logical checkpoint, and preserve the frozen schema-v1 byte contract where conformance tests require it.

Python source retirement completed on 2026-08-16. These files are language-neutral migration evidence and do not require a live Python Broker. Filesystem crash-safety is proven independently by Rust repository/process tests covering atomic snapshot replacement, fsync ordering, torn-tail repair, corruption fail-stop behavior, and hard-process restart recovery.

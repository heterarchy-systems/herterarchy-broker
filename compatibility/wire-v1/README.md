# Wire v1 compatibility corpus

This directory freezes executable wire output captured from the CPython 3.14t reference Broker before source retirement.

- `request_frames.ndjson` contains the frozen protocol-v1 request frames.
- `response_frames.ndjson` contains the frozen protocol-v1 success/error response frames.
- Rust protocol conformance tests decode and re-emit these frames byte-for-byte.

Python source retirement completed on 2026-08-16. These files are language-neutral migration evidence and do not require a live Python Broker.

Do not hand-edit individual JSON fields merely to make a Rust test pass. Protocol changes must be intentional, versioned, and reviewed for compatibility impact; a new compatibility corpus must be created rather than rewriting historical v1 evidence in place.

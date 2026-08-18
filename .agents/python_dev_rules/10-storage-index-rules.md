# Durable State and Concurrency Rules

- The MVP canonical runtime state is filesystem JSON under the configured execution root.
- Use the existing `AtomicJsonFile` path: validate, write a temporary file, flush as required, and atomically replace.
- State updates use compare-and-swap revision checks where concurrent writers can conflict. Never implement read-modify-write as separate unlocked operations.
- Use `EntityLockRegistry` and filesystem locks for cross-process serialization. All processes that mutate the same entity must derive the same lock key and lock directory.
- Define one lock order and keep it. Never acquire a broader lock while holding a narrower state lock if another path can reverse that order.
- Repository updater callbacks are pure state transforms. Do not perform browser, network, subprocess, wake-registration, or other external side effects inside them.
- Validate durable state before side effects and revalidate the expected revision afterward when completing a saga.
- Bound terminal request, admission, replay-defense, and browser history. Retain live/in-flight records and only the newest completed records needed for safety.
- Do not add a database or alternate persistence path as a shortcut for this MVP.


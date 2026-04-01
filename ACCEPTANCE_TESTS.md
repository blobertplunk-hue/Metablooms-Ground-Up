# Acceptance Tests

Required behaviors:

- Replay from `events.jsonl` occurs before execution.
- Exactly one bounded stage executed per run.
- Appended event must validate against `EVENT_SCHEMA.json`.
- `runtime_state.json` is derived from replay only.
- Receipt is written.
- Writes are limited to canonical root.
- Execution is blocked on schema failure.
- Execution is blocked on replay hash mismatch.
- Execution is blocked on idempotency collision.
- Mutating steps require compensation metadata.

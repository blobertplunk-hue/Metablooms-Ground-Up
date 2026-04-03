# Execution Gate Spec

All gates MUST pass before appending an event:

1. `schema_valid`: all existing events and candidate event validate against `EVENT_SCHEMA.json`.
2. `replay_valid`: deterministic replay and state-hash validation succeeds.
3. `idempotency_valid`: candidate `idempotency_key` is unique in `events.jsonl`.
4. `compensation_valid`: mutating stages must include compensation metadata.
5. `bounded_stage_available`: exactly one bounded stage is executed per invocation.

If any gate fails, block execution and write no new event.

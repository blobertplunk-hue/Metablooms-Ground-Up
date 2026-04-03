# Replay Rules

- Replay source: `events.jsonl`.
- Replay must be deterministic.
- Determinism check: compare `state_hash_before` and `state_hash_after` on every `STAGE_EXECUTED` event.
- Hash algorithm: `sha256` over canonical JSON (`sort_keys=true`, compact separators).
- Any mismatch is a hard failure and blocks execution.

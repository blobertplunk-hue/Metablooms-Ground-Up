# Master Workflow V2

1. Load all control files.
2. Replay and validate events deterministically.
3. Enforce execution gates before append/commit.
4. Execute exactly one bounded stage.
5. Append one schema-valid event.
6. Recompute and write derived `runtime_state.json`.
7. Write receipt.

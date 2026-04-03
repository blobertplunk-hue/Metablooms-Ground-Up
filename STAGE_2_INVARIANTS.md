# Stage 2 Turn Execution Engine — Enforced Invariants

## Guaranteed invariants

1. **Event identity integrity**
   - Duplicate `event_id` values in historical `events.jsonl` are rejected.

2. **Replay completeness**
   - Historical replay must consume events exactly once and in historical order.
   - Execution fails if replay indicates skipped, reordered, or double-applied events.

3. **Deterministic replay (state + output)**
   - `state_hash_before` / `state_hash_after` are verified for executed events.
   - `payload.output_hash` is required for executed events and must match canonicalized output.
   - Repeated executions for the same stage must produce consistent canonical output.

4. **Idempotency enforcement**
   - Candidate execution `idempotency_key` must be unique across historical events.

5. **Atomic write guarantees (fail-closed)**
   - Writes use temp-file + fsync + rename + directory fsync.
   - On write failure, rollback restores prior persisted state.

6. **Canonical-root isolation**
   - Mutated targets are normalized/resolved and must stay inside `CURRENT_ROOT.json`.
   - Path traversal / root escape attempts are rejected.

7. **Semantic marker enforcement for control files**
   - `AGENTS.md`, `MASTER_WORKFLOW_V2.md`, `REPLAY_RULES.md`, `EXECUTION_GATE_SPEC.md`, and `ACCEPTANCE_TESTS.md` are checked for required markers/rules.
   - Missing required markers/rules blocks execution.

8. **Acceptance gate fail-closed behavior**
   - Acceptance gate requires evaluable acceptance markers.
   - If acceptance evaluation cannot run, execution fails closed.

## Not guaranteed

- Full natural-language interpretation of control docs beyond required marker/rule checks.
- Cross-file logical consistency beyond explicitly enforced markers and runtime gate checks.
- External side-effect rollback outside files controlled by this engine.

## Marker-enforced vs advisory

### Marker-enforced
- `AGENTS.md` (required operational markers)
- `MASTER_WORKFLOW_V2.md` (required workflow markers)
- `REPLAY_RULES.md` (required replay markers)
- `EXECUTION_GATE_SPEC.md` (required gate rule markers)
- `ACCEPTANCE_TESTS.md` (required acceptance markers)

### Advisory
- Any additional prose in the above files that is not represented by an enforced marker/rule.

# Stage 18 — Recovery and Lock Engine

Consumes validation-layer outputs (`failure_class`, `retry_class`, proof registry links) to decide retry/continuation.

## Rules

1. HARD_FAILURE / NON_RETRYABLE
   - Exact task retry is blocked by default.
   - Manual continuation requires valid Process Lock Override token.

2. SOFT_FAILURE / RETRYABLE
   - Allow one retry for the exact task with adjusted parameters.
   - Second retry attempt is blocked.

3. Auditability
   - Every decision is appended to `RECOVERY_AUDIT_LOG.jsonl` atomically.
   - Decision includes linked proof registry reference.

4. Proof linkage
   - Decision records the latest proof registry entry for the task (if present).

5. Fail-closed
   - Unknown classes default to blocked.

# Governed Execution Entrypoints

- **Default governed entrypoint:** `execute_with_recovery(...)` in `src/turn_execution_engine.py`.
- **Internal-only lower-level path:** `_execute_once_internal(...)` is retained only for isolated engine tests.
- `execute_once(...)` is intentionally guarded and fail-closed to prevent bypassing recovery enforcement in normal orchestration.

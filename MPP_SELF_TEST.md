# MPP Self-Test Hook

This repository includes a minimal self-governance path to ensure MPP changes are validated by MPP rules.

## Trigger

When MPP-related files change (`turn_execution_engine`, `validation_layer`, `recovery_lock_engine`, and matching tests), runtime must run:

- `tests/test_validation_layer.py`
- `tests/test_recovery_lock_engine.py`

## Receipt

The self-test writes `MPP_SELF_TEST_RECEIPT.json` with:

- command executed
- required test list
- return code
- PASS/FAIL result
- captured stdout/stderr (truncated)
- invariant checks:
  - schemas load successfully
  - synthetic recovery decision path behaves as expected

## Fail-closed

If self-test command fails, execution is blocked with an engine error and no success path is allowed.

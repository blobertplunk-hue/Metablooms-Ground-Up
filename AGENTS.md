# Agent Instructions

Scope: repository root.

- Stage 2 engine must load and enforce control files:
  - `MASTER_WORKFLOW_V2.md`
  - `EVENT_SCHEMA.json`
  - `REPLAY_RULES.md`
  - `EXECUTION_GATE_SPEC.md`
  - `ACCEPTANCE_TESTS.md`
  - `CURRENT_ROOT.json`
- Runtime mutations are limited to canonical root from `CURRENT_ROOT.json`.
- `runtime_state.json` is derived-only and must be replayed from `events.jsonl`.

## Enforcement Contract (Codex)

Before completion, Codex must run and report:
1. `pre-commit run --all-files`
2. `pytest`
3. `python -m scripts.mpp_guard --mode=ci`
4. `python -m scripts.mpp_self_test`

Codex must not claim success if any gate fails.

# MPP Stage Research Packet (SEE)

## Scope
This packet maps existing repository enforcement surfaces to the full MPP Stage 1–10 runtime plan.

## Existing constraint map
- Deterministic replay/hash logic: `src/replay_utils.py`, `src/invariants.py`.
- Append-only proof chain + snapshot binding: `src/validation_layer.py`, `src/invariants.py`, `scripts/verify_proof_chain.py`.
- Fail-closed runtime execution gates: `src/turn_execution_engine.py`.
- Invariant registry single authority and coverage parity: `src/invariants.py`, `INVARIANT_COVERAGE_MAP.md`, `IMMUTABLE_CONFIG.json`.
- Independent verifier boundary: `scripts/verify_proof_chain.py` + TCB declaration.

## Minimal TCB (must remain minimal)
- `src.invariants`
- `src.replay_utils`
- `src.validation_layer`
- `scripts.verify_proof_chain`

MPP stage enforcement is implemented in runtime and checked through invariant context, but verifier TCB remains independent from runtime stage execution.

## Runtime-independent components
- Export bundle generation and manifest/signature outputs.
- Independent verifier execution and report generation.
- Canonical hashing definitions in invariants/replay utilities.

## Determinism-critical components
- Canonical JSON serialization and event hashing.
- Replay-derived runtime state reconstruction from `events.jsonl`.
- Proof snapshot hashing and append-only proof linkage.
- Stage artifact validation must be pure/deterministic and file-content based.

## Repository-artifact-enforced (not prompt-enforced)
- Stage policy + stage artifact schema.
- Stage pipeline descriptor (ordered, required, auditable).
- Stage artifact outputs under `mpp_artifacts/`.
- Coverage map parity including MPP compliance invariant.
- Runtime fail-closed checks for missing/malformed/insufficient artifacts.

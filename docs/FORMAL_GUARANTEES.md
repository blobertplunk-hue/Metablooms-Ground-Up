# Formal Guarantees

## 1) Determinism Guarantee
- **Assumptions:** identical canonical event stream; same invariant registry major version.
- **Enforcement:** `canonical_events_hash`, replay-derived `state_hash`, proof/output hash checks, deterministic export manifest.
- **Fail-closed path:** verifier/guard/runtime invariant failure aborts execution or returns FAIL.

## 2) Tamper-Evidence Guarantee
- **Assumptions:** proof entries persisted in append-only JSONL.
- **Enforcement:** `prev_hash -> current_hash` chain, entry hash recomputation, snapshot hash validation, duplicate trace rejection.
- **Fail-closed path:** `ProofChainInvariant` / `ProofRegistrySnapshotInvariant` failure.

## 2b) Release Artifact Integrity Guarantee
- **Assumptions:** export bundles include `manifest.json`; optional `release_signature.json` may be present.
- **Enforcement:** verifier recomputes manifest file hashes and validates signature over bundle hash.
- **Fail-closed path:** manifest/signature mismatch forces verifier FAIL.

## 3) Replay Correctness Guarantee
- **Assumptions:** `events.jsonl` is source of truth.
- **Enforcement:** replay-based state derivation, state hash binding (`state_hash_after`), event order/sequence integrity.
- **Fail-closed path:** replay/state mismatch rejects validation and governed execution.

## 4) Invariant Completeness Guarantee
- **Assumptions:** coverage map is canonical and immutable during run.
- **Enforcement:** runtime loads `INVARIANT_COVERAGE_MAP.md` and requires exact parity with `INVARIANT_REGISTRY`.
- **Fail-closed path:** coverage mismatch blocks execution.

## 5) Version Compatibility Guarantee
- **Assumptions:** invariant versions follow semantic-major compatibility.
- **Enforcement:** proof entries include `invariant_registry_version`; verifier compatibility checks and migration shim for legacy entries missing explicit version.
- **Fail-closed path:** incompatible major version returns verification FAIL.

## Minimal Trusted Computing Base (TCB)
The minimum TCB for offline verification is:
- `src/invariants.py`
- `src/replay_utils.py`
- `src/validation_layer.py` (proof-registry load/snapshot helpers)
- `scripts/verify_proof_chain.py`

Runtime execution engines are not part of verifier TCB.

## Operational Auditability
- Verifier writes `audit_log.jsonl` entries for each verification run.
- Audit log is intentionally excluded from canonical manifest hashing to preserve deterministic payload hashes.

## Full MPP Reasoning Enforcement
- Stages 1–10 are enforced by runtime artifact checks (`src/mpp_stage_pipeline.py`) before implementation execution.
- Missing or malformed required stage artifacts fail closed.
- MPP compliance is surfaced through `MPPComplianceInvariant` in the invariant registry.

## Reasoning Integrity Guarantee
- Canonical Stage 1/2/3/4/5/7/10 artifacts are hashed as `mpp_hash` and bound into each proof chain entry hash.
- Any mutation to reasoning artifacts, missing artifact, or malformed artifact causes verifier/runtime invariant failure.

## Decision Provenance Guarantee (BTS)
- Canonical BTS trace is hashed as `bts_hash` and proof-bound with `mpp_hash` + execution hashes.
- BTS integrity/completeness/justification/consistency invariants must pass for provenance to be valid.

## Decision Optimality Guarantee
- Canonical optimality trace is hashed as `optimality_hash` and proof-bound with execution + reasoning hashes.
- Optimality invariants enforce all-option scoring, quantified tradeoffs, option salience, and bounded regret.

## Implementation-Claim Integrity Guarantees

- Feature-level claims must be supported by substantive repository deltas classified by deterministic diff inspection.
- Claim-to-diff consistency is enforced fail-closed by `ImplementationRealityInvariant` and `ClaimConsistencyInvariant`.
- Formatting-only/docs-only/tests-only/no-op diffs cannot validly pass as feature implementation claims.

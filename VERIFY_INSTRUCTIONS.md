# Verification Instructions

This document describes external verification for exported MPP artifacts.

## Threat model

The verifier is designed to detect:
- Proof-registry tampering (hash mismatch, broken `prev_hash` linkage, duplicate `trace_id`).
- Event/proof binding mismatches (`events_hash` mismatch).
- Replay/state divergence (`state_hash_after` mismatch vs replayed state hash).
- Invariant registry version incompatibility.
- Canonicalization bypass attempts that change semantic content.
- Registry drift (coverage/version or manifest mismatch when artifacts are exported).
- Release-signature mismatch when `release_signature.json` is present.

The verifier does **not** claim protection against:
- Host compromise before artifacts are generated.
- Private-key/signing trust models (none are used in this repository).
- Kernel/runtime compromise while verification is running.
- External clock/time attestation.

## Inputs

Required:
- `events.jsonl`
- `PROOF_REGISTRY.jsonl`

Optional but recommended:
- `runtime_state.json`
- `INVARIANT_COVERAGE_MAP.md`

## Commands

Standalone verification:

```bash
python -m scripts.verify_proof_chain --root .
```

Write structured JSON report:

```bash
python -m scripts.verify_proof_chain --root . --report verification_report.json

# Implementation-reality gate (claim-to-diff consistency)
python -m scripts.implementation_reality_gate --root . --base HEAD~1 --head HEAD --claim IMPLEMENTATION_CLAIM.json
```

Replay + verification:

```bash
python -m scripts.replay --root . --verify
```

Create deterministic export bundle:

```bash
python -m scripts.export_bundle --root . --output ./export
```

Continuous audit mode:

```bash
python -m scripts.verify_proof_chain --root . --watch --format text
```

## Guarantees

When verification reports `PASS`, the following hold:
- **Tamper-evidence**: proof entries are chain-linked and hash-validated.
- **Determinism**: canonical event hashing and replayed state hashing are consistent.
- **Replay integrity**: replay-derived state hash matches proof-bound `state_hash_after`.
- **Version binding**: proof entries are compatible with the current invariant registry version.
- **Supply-chain integrity**: manifest and release signature (if present) are verified.

## Limitations

- Verification assumes canonical JSON artifacts were captured from a trusted filesystem snapshot.
- Freshness/real-time attestations are out of scope; verification is artifact-based.
- Performance bounds are enforced by tests/CI guardrails, not by the verifier runtime itself.
- HMAC signature validation requires `RELEASE_SIGNING_KEY` when `algorithm` is `hmac-sha256`.

## MPP Stage Artifacts
- Runtime requires Stage 1–10 artifacts under `mpp_artifacts/<task_id>/` when a stage payload sets `params.mpp_required=true`.
- Stage 9 must declare `validation_surface=existing_invariant_registry` and no duplicate validation paths.

- Recompute canonical MPP artifacts hash (`mpp_hash`) from Stage 1/2/3/4/5/7/10 artifacts and compare to proof entry.
- Any mismatch, missing artifact, or malformed MPP artifact is a verification FAIL (reasoning integrity).

- Recompute canonical BTS trace hash (`bts_hash`) from `bts_artifacts/<task_id>/bts_trace.json` and compare to proof entry.
- Verify BTS completeness/justification/consistency invariants (exploration, rejection reasons, decision linkage).

- Recompute canonical optimality trace hash (`optimality_hash`) and enforce argmax + bounded regret invariants.
- Enforce implementation reality and claim consistency (`ImplementationRealityInvariant`, `ClaimConsistencyInvariant`) for feature-claim changesets.

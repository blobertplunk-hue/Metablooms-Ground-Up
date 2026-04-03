# Implementation Reality Research Packet

## Repository-specific placement
- PR messaging is currently external to repository code (`make_pr` tool invocation), so repository-side enforcement must run *before* PR payload creation.
- Existing diff inspection is not centralized today; this change introduces deterministic inspection in `src/review/diff_classifier.py`.
- Existing invariant/proof/BTS hooks live in:
  - invariant registry: `src/invariants.py`
  - runtime enforcement boundary: `src/turn_execution_engine.py`
  - independent verifier: `scripts/verify_proof_chain.py`
  - export/audit surface: `scripts/export_bundle.py`

## What counts as substantive implementation in this repo
- Semantic runtime/verifier logic changes in `src/*.py` / `scripts/*.py`.
- Schema/contract changes that tighten acceptance for new artifacts/entries.
- New enforcement invariants or fail-closed rule additions.
- New tests proving newly enforced behavior.

## Legitimate non-feature changes
- formatting/lint wrapping only
- docs-only updates
- tests-only coverage changes
- generated receipts/derived artifacts (`GUARD_RECEIPT.json`, timestamps in `MPP_SELF_TEST_RECEIPT.json`)

## Enforced objective
Feature claims must map to semantic diff evidence and changed surfaces; otherwise the gate fails closed.

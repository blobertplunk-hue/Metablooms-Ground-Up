# Validation Layer Spec

Aligned to MASTER_MPP_SCHEMA_v1:
- Stage 11 VALIDATION_ENGINE
- Stage 12 TRACE_VALIDATION
- Stage 13 COUNTERFACTUAL_TESTING

Required persisted artifacts:
- VALIDATION_RECEIPT.json
- TRACE_VALIDATION_RECEIPT.json
- EXECUTION_PROOF.json
- COUNTERFACTUAL_TEST_REPORT.json
- PROOF_REGISTRY.jsonl (append-only)

Fail-closed: missing proof, missing mutation, silent success, or indistinguishable counterfactual => FAIL.

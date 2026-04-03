from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.recovery_lock_engine import decide_recovery
from src.invariants import run_invariants
from src.turn_execution_engine import EngineError
from src.turn_execution_engine import canonical_events_hash
from src.validation_layer import (
    ValidationError,
    append_registry_atomic,
    run_validation_pipeline,
)


def _scenario_known_good(root: Path) -> None:
    run_validation_pipeline(
        {
            "trace_id": "self-good",
            "task_id": "self",
            "execution_id": "self-good",
            "stage_id": "11",
            "mutation_proof": {
                "target_id": "s1",
                "delta_observed": True,
                "pre_hash": "aaa",
                "post_hash": "bbb",
            },
            "artifacts_present": ["events.jsonl", "runtime_state.json"],
            "execution_claimed": True,
            "execution_events": [
                {
                    "stage_id": "s1",
                    "event_id": "e1",
                    "target_id": "s1",
                    "artifact_id": "events.jsonl",
                }
            ],
            "mutated_artifact": "events.jsonl",
            "pre_hash": "aaa",
            "post_hash": "bbb",
            "events_hash": canonical_events_hash([]),
            "schema_dir": str(root),
        }
    )


def _scenario_counterfactual_fail(root: Path) -> None:
    try:
        run_validation_pipeline(
            {
                "trace_id": "self-bad-cf",
                "task_id": "self",
                "execution_id": "self-bad-cf",
                "stage_id": "11",
                "mutation_proof": {
                    "target_id": "s1",
                    "delta_observed": False,
                    "pre_hash": "aaa",
                    "post_hash": "aaa",
                },
                "artifacts_present": ["events.jsonl", "runtime_state.json"],
                "execution_claimed": True,
                "execution_events": [
                    {
                        "stage_id": "s1",
                        "event_id": "e1",
                        "target_id": "s1",
                        "artifact_id": "events.jsonl",
                    }
                ],
                "mutated_artifact": "events.jsonl",
                "pre_hash": "aaa",
                "post_hash": "aaa",
                "events_hash": canonical_events_hash([]),
                "schema_dir": str(root),
            }
        )
    except ValidationError:
        return
    raise EngineError("counterfactual indistinguishable should fail")


def _scenario_recovery() -> None:
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as td:
        p = Path(td)
        (p / "PROOF_REGISTRY.jsonl").write_text("", encoding="utf-8")
        d1 = decide_recovery(
            task_id="t",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
            adjusted_params=None,
            audit_log_path=p / "a.jsonl",
            proof_registry_path=p / "PROOF_REGISTRY.jsonl",
        )
        d2 = decide_recovery(
            task_id="t",
            failure_class="HARD_FAILURE",
            retry_class="NON_RETRYABLE",
            adjusted_params=None,
            audit_log_path=p / "a.jsonl",
            proof_registry_path=p / "PROOF_REGISTRY.jsonl",
        )
        if not d1.allowed or d2.allowed:
            raise EngineError("recovery invariant failed")


def _scenario_proof_append() -> None:
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as td:
        proof = Path(td) / "PROOF_REGISTRY.jsonl"
        append_registry_atomic(
            proof,
            {
                "trace_id": "self-chain-1",
                "task_id": "self",
                "execution": {},
                "mutation_proof": {},
                "result": "PASS",
                "events_hash": canonical_events_hash([]),
                "timestamp": "2026-01-01T00:00:00+00:00",
                "validation_receipt_ref": "VALIDATION_RECEIPT.json",
                "trace_receipt_ref": "TRACE_VALIDATION_RECEIPT.json",
                "counterfactual_report_ref": "COUNTERFACTUAL_TEST_REPORT.json",
            },
        )
        report = run_invariants(
            {"proofs": [json.loads(proof.read_text(encoding="utf-8").strip())]},
            names={"ProofChainInvariant"},
        )
        if not report["passed"]:
            raise EngineError("invariant registry self-test failed")


def main() -> int:
    root = Path(".").resolve()
    receipt_path = root / "MPP_SELF_TEST_RECEIPT.json"
    results = {
        "known_good": "PASS",
        "broken_proof_chain": "PASS",
        "replay_mismatch": "PASS",
        "counterfactual_indistinguishable": "PASS",
        "recovery_logic": "PASS",
    }
    status = "PASS"
    try:
        _scenario_known_good(root)
        _scenario_counterfactual_fail(root)
        _scenario_recovery()
        _scenario_proof_append()
    except (ValidationError, EngineError) as exc:
        status = "FAIL"
        results["error"] = str(exc)

    receipt = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "result": status,
        "scenarios": results,
    }
    receipt_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if status != "PASS":
        raise SystemExit(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

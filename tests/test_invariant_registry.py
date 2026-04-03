from __future__ import annotations

import json
from pathlib import Path

from src.invariants import FunctionInvariant, run_all_invariants
from src.validation_layer import (
    run_validation_pipeline,
    validate_counterfactual,
    validate_mutation_proof,
    validate_trace_consistency,
)


def _base_context() -> dict:
    return {
        "trace_id": "t1",
        "task_id": "task",
        "execution_id": "exec-1",
        "stage_id": "11",
        "mutation_proof": {
            "target_id": "s1",
            "delta_observed": True,
            "pre_hash": "a",
            "post_hash": "b",
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
        "pre_hash": "a",
        "post_hash": "b",
        "events_hash": "h",
    }


def test_invariant_registry_execution() -> None:
    registry = [
        FunctionInvariant("MutationProofInvariant", validate_mutation_proof),
        FunctionInvariant("TraceConsistencyInvariant", validate_trace_consistency),
        FunctionInvariant("CounterfactualInvariant", validate_counterfactual),
    ]
    results = run_all_invariants(_base_context(), registry)
    assert len(results) == 3
    assert [r.name for r in results] == [
        "MutationProofInvariant",
        "TraceConsistencyInvariant",
        "CounterfactualInvariant",
    ]
    assert not any(r.fail for r in results)


def test_partial_invariant_failure_aggregation() -> None:
    broken = _base_context()
    broken["mutation_proof"]["delta_observed"] = False
    registry = [
        FunctionInvariant("MutationProofInvariant", validate_mutation_proof),
        FunctionInvariant("TraceConsistencyInvariant", validate_trace_consistency),
    ]
    results = run_all_invariants(broken, registry)
    assert results[0].fail
    assert not results[1].fail
    assert any(f["rule"] == "delta_mismatch" for f in results[0].failures)


def test_execution_validation_separation(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    events_path.write_text(json.dumps({"k": "v"}) + "\n", encoding="utf-8")
    before = events_path.read_text(encoding="utf-8")

    schema_dir = Path(".")
    context = _base_context()
    context["schema_dir"] = str(schema_dir)
    run_validation_pipeline(context)

    after = events_path.read_text(encoding="utf-8")
    assert before == after

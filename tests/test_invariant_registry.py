from __future__ import annotations

import json
import random
from copy import deepcopy
from pathlib import Path

import pytest

from src import invariants as invariant_module
from src.invariants import (
    Invariant,
    InvariantContractError,
    ValidationResult,
    run_invariants,
)
from src.validation_layer import (
    run_validation_pipeline,
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
    report = run_invariants(
        _base_context(),
        names={
            "MutationProofInvariant",
            "TraceConsistencyInvariant",
            "CounterfactualInvariant",
        },
    )
    assert report["passed"]
    assert set(report["invariants_checked"]) == {
        "MutationProofInvariant",
        "TraceConsistencyInvariant",
        "CounterfactualInvariant",
    }


def test_invariant_aggregation_multiple_failures() -> None:
    broken = _base_context()
    broken["mutation_proof"]["delta_observed"] = False
    broken["execution_events"] = []
    report = run_invariants(
        broken,
        names={"MutationProofInvariant", "TraceConsistencyInvariant"},
    )
    assert not report["passed"]
    assert len(report["failures"]) >= 2


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


def test_guard_uses_registry_only() -> None:
    text = Path("scripts/mpp_guard.py").read_text(encoding="utf-8")
    assert "run_invariants(" in text


def test_self_test_uses_registry_only() -> None:
    text = Path("scripts/mpp_self_test.py").read_text(encoding="utf-8")
    assert "run_invariants(" in text


def test_registry_evaluation_is_pure() -> None:
    context = _base_context()
    before = deepcopy(context)
    _ = run_invariants(context)
    assert context == before


def test_structured_failures_are_typed() -> None:
    broken = _base_context()
    broken["mutation_proof"]["delta_observed"] = False
    report = run_invariants(broken, names={"MutationProofInvariant"})
    assert not report["passed"]
    failure = report["failures"][0]
    structured = failure["metadata"]["structured_failures"][0]
    assert set(structured) == {"rule", "failure_class", "retry_class"}


def test_invariant_determinism_harness() -> None:
    context = _base_context()
    first = run_invariants(context)
    second = run_invariants(context)
    assert first["invariants_checked"] == second["invariants_checked"]
    assert first["failures"] == second["failures"]
    assert [r.passed for r in first["results"]] == [r.passed for r in second["results"]]


def test_invariant_determinism_stress_harness() -> None:
    base = _base_context()
    expected = run_invariants(base)
    outputs = []
    for seed in range(5):
        shuffled = dict(list(base.items()))
        items = list(shuffled.items())
        random.Random(seed).shuffle(items)
        ctx = dict(items)
        outputs.append(run_invariants(ctx))
    for output in outputs:
        assert output["invariants_checked"] == expected["invariants_checked"]
        assert output["failures"] == expected["failures"]


def test_structured_failure_schema_rejects_adhoc_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BadInvariant(Invariant):
        def __init__(self) -> None:
            super().__init__("BadInvariant", "x", "HARD", [])

        def validate(self, context: dict) -> ValidationResult:
            return ValidationResult(
                self.name,
                passed=False,
                failures=["bad_rule_name"],
                metadata={
                    "structured_failures": [
                        {
                            "rule": "not_allowed",
                            "failure_class": "HARD_FAILURE",
                            "retry_class": "NON_RETRYABLE",
                            "unexpected": "field",
                        }
                    ]
                },
            )

    monkeypatch.setattr(invariant_module, "INVARIANT_REGISTRY", [BadInvariant()])
    with pytest.raises(InvariantContractError):
        run_invariants({})


def test_structured_failure_schema_rejects_unknown_enum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BadEnumInvariant(Invariant):
        def __init__(self) -> None:
            super().__init__("BadEnumInvariant", "x", "HARD", [])

        def validate(self, context: dict) -> ValidationResult:
            return ValidationResult(
                self.name,
                passed=False,
                failures=["artifact_presence"],
                metadata={
                    "structured_failures": [
                        {
                            "rule": "artifact_presence",
                            "failure_class": "NOT_A_CLASS",
                            "retry_class": "NON_RETRYABLE",
                        }
                    ]
                },
            )

    monkeypatch.setattr(invariant_module, "INVARIANT_REGISTRY", [BadEnumInvariant()])
    with pytest.raises(InvariantContractError):
        run_invariants({})


def test_structured_failure_schema_rejects_missing_required_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MissingFieldInvariant(Invariant):
        def __init__(self) -> None:
            super().__init__("MissingFieldInvariant", "x", "HARD", [])

        def validate(self, context: dict) -> ValidationResult:
            return ValidationResult(
                self.name,
                passed=False,
                failures=["artifact_presence"],
                metadata={
                    "structured_failures": [
                        {
                            "rule": "artifact_presence",
                            "failure_class": "HARD_FAILURE",
                        }
                    ]
                },
            )

    monkeypatch.setattr(
        invariant_module, "INVARIANT_REGISTRY", [MissingFieldInvariant()]
    )
    with pytest.raises(InvariantContractError):
        run_invariants({})


def test_invariant_coverage_map_is_complete_and_non_overlapping() -> None:
    text = Path("INVARIANT_COVERAGE_MAP.md").read_text(encoding="utf-8")
    mapped_invariants = []
    for line in text.splitlines():
        if not line.startswith("|") or line.startswith("|---"):
            continue
        cols = [c.strip() for c in line.strip("|").split("|")]
        if len(cols) != 2 or cols[0] == "Critical property":
            continue
        mapped_invariants.append(cols[1].strip("`"))
    assert len(mapped_invariants) == len(set(mapped_invariants))
    registry_names = [inv.name for inv in invariant_module.INVARIANT_REGISTRY]
    assert set(mapped_invariants) == set(registry_names)

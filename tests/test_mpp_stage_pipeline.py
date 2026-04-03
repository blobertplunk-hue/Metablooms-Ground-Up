import json
from pathlib import Path

import pytest

from scripts.verify_proof_chain import TCB_MODULES
from src.turn_execution_engine import EngineError, _execute_once_internal

from tests.test_turn_execution_engine import _paths, _write_controls


def _write_artifact(
    root: Path, task_id: str, stage: int, name: str, content: dict
) -> None:
    target = root / "mpp_artifacts" / task_id
    target.mkdir(parents=True, exist_ok=True)
    payload = {"stage": stage, "task_id": task_id, "content": content}
    (target / f"stage_{stage:02d}_{name}.json").write_text(
        json.dumps(payload, sort_keys=True), encoding="utf-8"
    )


def _write_all_valid_artifacts(root: Path, task_id: str = "task-mpp") -> None:
    _write_artifact(
        root,
        task_id,
        1,
        "see_gate",
        {
            "task_framing": "framing",
            "constraints": ["c1"],
            "sources": ["repo"],
            "failure_modes": ["f1"],
            "design_implications": ["d1"],
        },
    )
    _write_artifact(
        root,
        task_id,
        2,
        "problem_formalization",
        {
            "inputs": ["i"],
            "outputs": ["o"],
            "constraints": ["c"],
            "invariants_affected": ["MutationProofInvariant"],
            "acceptance_criteria": ["a"],
        },
    )
    options = [
        {
            "option_id": f"o{i}",
            "approach_key": f"k{i}",
            "primary_mechanism": f"m{i}",
            "touched_modules": [f"src/m{i}.py"],
        }
        for i in range(1, 6)
    ]
    bts_dir = root / "bts_artifacts" / task_id
    bts_dir.mkdir(parents=True, exist_ok=True)
    (bts_dir / "bts_trace.json").write_text(
        json.dumps(
            {
                "options": options,
                "evaluation_scores": {f"o{i}": float(10 - i) for i in range(1, 6)},
                "rejected_options": [
                    {
                        "option_id": "o2",
                        "reason": "higher risk",
                        "criteria_links": ["operational_risk"],
                    },
                    {
                        "option_id": "o3",
                        "reason": "complex",
                        "criteria_links": ["complexity"],
                    },
                    {
                        "option_id": "o4",
                        "reason": "lower score",
                        "criteria_links": ["correctness"],
                    },
                    {
                        "option_id": "o5",
                        "reason": "lower score",
                        "criteria_links": ["correctness"],
                    },
                ],
                "decision_criteria": [
                    "correctness",
                    "determinism_compatibility",
                    "invariant_compatibility",
                ],
                "chosen_option": "o1",
                "decision_confidence": 0.82,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    optimality_dir = root / "optimality_artifacts" / task_id
    optimality_dir.mkdir(parents=True, exist_ok=True)
    (optimality_dir / "optimality_trace.json").write_text(
        json.dumps(
            {
                "chosen_option": "o1",
                "options": [
                    {
                        **opt,
                        "alternatives_score": 0.8,
                        "justification_score": 0.7,
                        "constraint_satisfaction": 1.0,
                        "tradeoff": {
                            "cost": float(i),
                            "benefit": float(10 - i),
                            "risk": 0.1 * i,
                            "uncertainty": 0.05 * i,
                        },
                        "counterfactual": {
                            "intervention": f"use-{opt['option_id']}",
                            "predicted_outcome": "ok",
                            "confidence": 0.8 - (i * 0.05),
                        },
                        "primary_mechanism": f"mech-{i}",
                    }
                    for i, opt in enumerate(options, start=1)
                ],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    _write_artifact(root, task_id, 3, "multi_option_generation", {"options": options})
    _write_artifact(
        root,
        task_id,
        4,
        "evaluation_matrix",
        {
            "matrix": [
                {
                    "option_id": option["option_id"],
                    "scores": {
                        "correctness": 5,
                        "determinism_compatibility": 5,
                        "invariant_compatibility": 5,
                        "complexity": 3,
                        "extensibility": 4,
                        "operational_risk": 2,
                        "verification_friendliness": 5,
                    },
                    "rationale": "ok",
                }
                for option in options
            ]
        },
    )
    _write_artifact(
        root,
        task_id,
        5,
        "decision_record",
        {
            "chosen_option_id": "o1",
            "rejected_option_ids": ["o2", "o3", "o4", "o5"],
            "tradeoffs": ["t1"],
            "risks": ["r1"],
            "linked_artifacts": ["stage_04_evaluation_matrix.json"],
        },
    )
    _write_artifact(
        root,
        task_id,
        6,
        "missing_middle_detector",
        {
            "checked_stage_refs": [2, 3, 4, 5],
            "unbound_transitions": [],
            "unresolved_assumptions": [],
        },
    )
    _write_artifact(
        root,
        task_id,
        7,
        "implementation_plan",
        {
            "dependency_graph": {"nodes": ["a"], "edges": []},
            "touched_modules": ["src/turn_execution_engine.py"],
            "invariants_impacted": ["MPPComplianceInvariant"],
            "tests_required": ["tests/test_mpp_stage_pipeline.py"],
            "rollback_recovery": {"strategy": "rollback"},
        },
    )
    _write_artifact(
        root,
        task_id,
        8,
        "implementation",
        {
            "linked_plan_artifact": "stage_07_implementation_plan.json",
            "executed_steps": ["step1"],
            "touched_files": ["src/turn_execution_engine.py"],
        },
    )
    _write_artifact(
        root,
        task_id,
        9,
        "validation",
        {
            "validation_surface": "existing_invariant_registry",
            "invariant_registry_used": True,
            "validation_artifacts": ["VALIDATION_RECEIPT.json"],
            "duplicate_validation_paths": [],
        },
    )
    _write_artifact(
        root,
        task_id,
        10,
        "refinement_loop",
        {
            "comparison_against_rejected_options": "improved",
            "refinement_passes": 1,
            "stop_reason": "bounded",
        },
    )


def _write_events_with_mpp(root: Path, task_id: str = "task-mpp") -> None:
    events = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:00:00+00:00",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {
                "stage_id": "s1",
                "bounded": True,
                "mutates": False,
                "params": {
                    "mpp_required": True,
                    "mpp_task_id": task_id,
                    "mpp_policy": {
                        "task_id": task_id,
                        "requires_see": True,
                        "requires_multi_option": True,
                        "requires_refinement": True,
                    },
                },
            },
        }
    ]
    (root / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )


def test_mpp_missing_see_artifact_fails_closed(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_all_valid_artifacts(tmp_path)
    _write_events_with_mpp(tmp_path)
    (tmp_path / "mpp_artifacts" / "task-mpp" / "stage_01_see_gate.json").unlink()
    with pytest.raises(EngineError, match="MPP stage enforcement failed"):
        _execute_once_internal(_paths(tmp_path))


def test_mpp_malformed_see_artifact_fails_closed(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_all_valid_artifacts(tmp_path)
    _write_events_with_mpp(tmp_path)
    (tmp_path / "mpp_artifacts" / "task-mpp" / "stage_01_see_gate.json").write_text(
        "{", encoding="utf-8"
    )
    with pytest.raises(EngineError, match="Malformed JSON artifact"):
        _execute_once_internal(_paths(tmp_path))


def test_mpp_fewer_than_five_options_fails_closed(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_all_valid_artifacts(tmp_path)
    _write_events_with_mpp(tmp_path)
    _write_artifact(
        tmp_path,
        "task-mpp",
        3,
        "multi_option_generation",
        {
            "options": [
                {
                    "option_id": f"o{i}",
                    "approach_key": "same",
                    "primary_mechanism": "same",
                    "touched_modules": ["src/same.py"],
                }
                for i in range(1, 5)
            ]
        },
    )
    with pytest.raises(EngineError, match="fewer than 5 meaningfully distinct"):
        _execute_once_internal(_paths(tmp_path))


def test_mpp_missing_decision_record_fails_closed(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_all_valid_artifacts(tmp_path)
    _write_events_with_mpp(tmp_path)
    (tmp_path / "mpp_artifacts" / "task-mpp" / "stage_05_decision_record.json").unlink()
    with pytest.raises(EngineError, match="Missing required MPP stage artifact"):
        _execute_once_internal(_paths(tmp_path))


def test_mpp_missing_middle_detected_fails_closed(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_all_valid_artifacts(tmp_path)
    _write_events_with_mpp(tmp_path)
    _write_artifact(
        tmp_path,
        "task-mpp",
        6,
        "missing_middle_detector",
        {
            "checked_stage_refs": [2, 3],
            "unbound_transitions": ["stage4->stage5"],
            "unresolved_assumptions": [],
        },
    )
    with pytest.raises(EngineError, match="missing-middle"):
        _execute_once_internal(_paths(tmp_path))


def test_mpp_implementation_without_plan_fails_closed(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_all_valid_artifacts(tmp_path)
    _write_events_with_mpp(tmp_path)
    _write_artifact(
        tmp_path,
        "task-mpp",
        8,
        "implementation",
        {
            "linked_plan_artifact": "wrong_plan.json",
            "executed_steps": ["step1"],
            "touched_files": ["src/turn_execution_engine.py"],
        },
    )
    with pytest.raises(EngineError, match="implementation not linked"):
        _execute_once_internal(_paths(tmp_path))


def test_mpp_refinement_required_missing_fails_closed(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_all_valid_artifacts(tmp_path)
    _write_events_with_mpp(tmp_path)
    (tmp_path / "mpp_artifacts" / "task-mpp" / "stage_10_refinement_loop.json").unlink()
    with pytest.raises(EngineError, match="Missing required MPP stage artifact"):
        _execute_once_internal(_paths(tmp_path))


def test_verifier_independence_tcb_excludes_runtime_stage_engine() -> None:
    assert "src.turn_execution_engine" not in TCB_MODULES
    assert "src.mpp_stage_pipeline" not in TCB_MODULES


def test_mpp_determinism_remains_across_repeated_runs(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_all_valid_artifacts(tmp_path)
    _write_events_with_mpp(tmp_path)

    first = _execute_once_internal(_paths(tmp_path))
    assert first["executed_stage_id"] == "s1"
    # second run is noop but must remain deterministic and fail-safe
    second = _execute_once_internal(_paths(tmp_path))
    assert second["executed_stage_id"] is None


def test_mpp_validation_stage_rejects_duplicate_shadow_paths(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_all_valid_artifacts(tmp_path)
    _write_events_with_mpp(tmp_path)
    _write_artifact(
        tmp_path,
        "task-mpp",
        9,
        "validation",
        {
            "validation_surface": "existing_invariant_registry",
            "invariant_registry_used": True,
            "validation_artifacts": ["VALIDATION_RECEIPT.json"],
            "duplicate_validation_paths": ["custom_validator.py"],
        },
    )
    with pytest.raises(EngineError, match="duplicate validation paths"):
        _execute_once_internal(_paths(tmp_path))


def test_runtime_refuses_proof_write_when_mpp_hash_cannot_be_computed(
    tmp_path: Path,
) -> None:
    _write_controls(tmp_path)
    _write_all_valid_artifacts(tmp_path)
    _write_events_with_mpp(tmp_path)
    (tmp_path / "mpp_artifacts" / "task-mpp" / "stage_01_see_gate.json").unlink()
    with pytest.raises(
        EngineError, match="MPP stage enforcement failed|MPP hash calculation failed"
    ):
        _execute_once_internal(_paths(tmp_path))


def test_runtime_refuses_when_bts_artifact_missing(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_all_valid_artifacts(tmp_path)
    _write_events_with_mpp(tmp_path)
    (tmp_path / "bts_artifacts" / "task-mpp" / "bts_trace.json").unlink()
    with pytest.raises(EngineError, match="BTS hash calculation failed"):
        _execute_once_internal(_paths(tmp_path))


def test_runtime_refuses_when_optimality_artifact_missing(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_all_valid_artifacts(tmp_path)
    _write_events_with_mpp(tmp_path)
    (tmp_path / "optimality_artifacts" / "task-mpp" / "optimality_trace.json").unlink()
    with pytest.raises(EngineError, match="Optimality hash calculation failed"):
        _execute_once_internal(_paths(tmp_path))

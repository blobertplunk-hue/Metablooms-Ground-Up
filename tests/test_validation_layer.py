import json
from pathlib import Path

import pytest

from src.validation_layer import (
    ValidationError,
    append_registry_atomic,
    classify_failure,
    load_registry_entries_resilient,
    run_validation_pipeline,
)


def _base_context() -> dict:
    schema_dir = Path(__file__).resolve().parents[1]
    return {
        "trace_id": "t1",
        "task_id": "task1",
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
        "schema_dir": str(schema_dir),
    }


def test_validation_fails_when_mutation_proof_missing() -> None:
    ctx = _base_context()
    ctx["mutation_proof"] = None
    with pytest.raises(ValidationError):
        run_validation_pipeline(ctx)


def test_validation_fails_when_delta_observed_false() -> None:
    ctx = _base_context()
    ctx["mutation_proof"]["delta_observed"] = False
    with pytest.raises(ValidationError):
        run_validation_pipeline(ctx)


def test_trace_validation_fails_without_execution_artifacts() -> None:
    ctx = _base_context()
    ctx["execution_events"] = []
    with pytest.raises(ValidationError):
        run_validation_pipeline(ctx)


def test_counterfactual_fails_when_indistinguishable() -> None:
    ctx = _base_context()
    ctx["post_hash"] = ctx["pre_hash"]
    with pytest.raises(ValidationError):
        run_validation_pipeline(ctx)


def test_proof_registry_append_only_entries(tmp_path: Path) -> None:
    path = tmp_path / "PROOF_REGISTRY.jsonl"
    append_registry_atomic(
        path,
        {
            "trace_id": "t1",
            "task_id": "x",
            "execution": {},
            "mutation_proof": {},
            "result": "PASS",
        },
    )
    append_registry_atomic(
        path,
        {
            "trace_id": "t2",
            "task_id": "x",
            "execution": {},
            "mutation_proof": {},
            "result": "PASS",
        },
    )
    lines = [
        json.loads(x)
        for x in path.read_text(encoding="utf-8").splitlines()
        if x.strip()
    ]
    assert [line["trace_id"] for line in lines] == ["t1", "t2"]


def test_validation_fails_closed_on_incomplete_validation() -> None:
    ctx = _base_context()
    ctx["artifacts_present"] = []
    with pytest.raises(ValidationError):
        run_validation_pipeline(ctx)


def test_no_success_when_ran_but_changed_nothing() -> None:
    ctx = _base_context()
    ctx["mutation_proof"]["delta_observed"] = False
    with pytest.raises(ValidationError):
        run_validation_pipeline(ctx)


def test_cross_artifact_mismatch_detected() -> None:
    ctx = _base_context()
    ctx["execution_events"] = [
        {
            "stage_id": "different",
            "event_id": "e1",
            "target_id": "different",
            "artifact_id": "wrong.json",
        }
    ]
    with pytest.raises(ValidationError):
        run_validation_pipeline(ctx)


def test_delta_true_but_hashes_unchanged_fails() -> None:
    ctx = _base_context()
    ctx["mutation_proof"]["delta_observed"] = True
    ctx["mutation_proof"]["pre_hash"] = "same"
    ctx["mutation_proof"]["post_hash"] = "same"
    ctx["pre_hash"] = "same"
    ctx["post_hash"] = "same"
    with pytest.raises(ValidationError, match="delta_mismatch|no_silent_success"):
        run_validation_pipeline(ctx)


def test_mutation_hashes_disagree_with_top_level_fails() -> None:
    ctx = _base_context()
    ctx["mutation_proof"]["pre_hash"] = "x1"
    ctx["mutation_proof"]["post_hash"] = "x2"
    ctx["pre_hash"] = "a"
    ctx["post_hash"] = "b"
    with pytest.raises(ValidationError, match="pre_hash_mismatch|post_hash_mismatch"):
        run_validation_pipeline(ctx)


def test_counterfactual_requires_explicit_distinguishing_signal() -> None:
    ctx = _base_context()
    ctx["pre_hash"] = "z"
    ctx["post_hash"] = "z"
    ctx["mutation_proof"]["pre_hash"] = "z"
    ctx["mutation_proof"]["post_hash"] = "z"
    ctx["mutation_proof"]["delta_observed"] = False
    with pytest.raises(
        ValidationError, match="counterfactual_indistinguishable|no_silent_success"
    ):
        run_validation_pipeline(ctx)


def test_proof_registry_atomic_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "PROOF_REGISTRY.jsonl"
    original_open = Path.open

    def fail_open(self: Path, mode: str = "r", *args, **kwargs):  # type: ignore[override]
        if self == path and "a" in mode:
            raise OSError("io failure")
        return original_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_open)
    with pytest.raises(OSError):
        append_registry_atomic(
            path,
            {
                "trace_id": "t1",
                "task_id": "x",
                "execution": {},
                "mutation_proof": {},
                "result": "PASS",
            },
        )
    assert not path.exists()


def test_generated_artifact_schema_violation_fails() -> None:
    ctx = _base_context()
    # Corrupt schema: require impossible field.
    schema_dir = Path(ctx["schema_dir"])
    backup = (schema_dir / "VALIDATION_RECEIPT_SCHEMA.json").read_text(encoding="utf-8")
    (schema_dir / "VALIDATION_RECEIPT_SCHEMA.json").write_text(
        '{"required":["impossible"]}', encoding="utf-8"
    )
    try:
        with pytest.raises(
            ValidationError, match="VALIDATION_RECEIPT schema validation failed"
        ):
            run_validation_pipeline(ctx)
    finally:
        (schema_dir / "VALIDATION_RECEIPT_SCHEMA.json").write_text(
            backup, encoding="utf-8"
        )


def test_proof_registry_duplicate_trace_id_fails(tmp_path: Path) -> None:
    path = tmp_path / "PROOF_REGISTRY.jsonl"
    append_registry_atomic(
        path,
        {
            "trace_id": "t1",
            "task_id": "x",
            "execution": {},
            "mutation_proof": {},
            "result": "PASS",
        },
    )
    with pytest.raises(ValidationError, match="duplicate trace_id"):
        append_registry_atomic(
            path,
            {
                "trace_id": "t1",
                "task_id": "x",
                "execution": {},
                "mutation_proof": {},
                "result": "PASS",
            },
        )


def test_proof_registry_stable_hash_ignores_extra_fields(tmp_path: Path) -> None:
    path = tmp_path / "PROOF_REGISTRY.jsonl"
    append_registry_atomic(
        path,
        {
            "trace_id": "t1",
            "task_id": "task-a",
            "execution": {},
            "mutation_proof": {"x": 1},
            "result": "PASS",
            "extra": 1,
        },
    )
    rows = [
        json.loads(x)
        for x in path.read_text(encoding="utf-8").splitlines()
        if x.strip()
    ]
    rows[0]["extra"] = 999
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    append_registry_atomic(
        path,
        {
            "trace_id": "t2",
            "task_id": "task-a",
            "execution": {},
            "mutation_proof": {"x": 2},
            "result": "PASS",
        },
    )


def test_proof_registry_broken_prev_hash_fails(tmp_path: Path) -> None:
    path = tmp_path / "PROOF_REGISTRY.jsonl"
    append_registry_atomic(
        path,
        {
            "trace_id": "t1",
            "task_id": "x",
            "execution": {},
            "mutation_proof": {},
            "result": "PASS",
        },
    )
    append_registry_atomic(
        path,
        {
            "trace_id": "t2",
            "task_id": "x",
            "execution": {},
            "mutation_proof": {},
            "result": "PASS",
        },
    )
    rows = [
        json.loads(x)
        for x in path.read_text(encoding="utf-8").splitlines()
        if x.strip()
    ]
    rows[1]["prev_hash"] = "broken"
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="broken chain"):
        append_registry_atomic(
            path,
            {
                "trace_id": "t3",
                "task_id": "x",
                "execution": {},
                "mutation_proof": {},
                "result": "PASS",
            },
        )


def test_proof_registry_tail_recovery_truncates_invalid_line(tmp_path: Path) -> None:
    path = tmp_path / "PROOF_REGISTRY.jsonl"
    append_registry_atomic(
        path,
        {
            "trace_id": "t1",
            "task_id": "x",
            "execution": {},
            "mutation_proof": {},
            "result": "PASS",
        },
    )
    with path.open("ab") as fh:
        fh.write(b'{"trace_id":"bad"')
    entries = load_registry_entries_resilient(path)
    assert len(entries) == 1
    text = path.read_text(encoding="utf-8")
    assert text.count("\n") == 1


def test_failure_classification_soft_and_hard() -> None:
    assert classify_failure("timeout while writing") == ("SOFT_FAILURE", "RETRYABLE")
    assert classify_failure("schema mismatch") == ("HARD_FAILURE", "NON_RETRYABLE")

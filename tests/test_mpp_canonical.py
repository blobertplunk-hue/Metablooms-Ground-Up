import json
from pathlib import Path

import pytest

from src.mpp.mpp_canonical import MPPCanonicalError, canonical_mpp_hash


def _write_artifact(base: Path, stage: int, name: str, content: dict) -> None:
    payload = {
        "stage": stage,
        "task_id": "task-a",
        "content": content,
        "timestamp": "2026-01-01T00:00:00+00:00",
    }
    (base / f"stage_{stage:02d}_{name}.json").write_text(
        json.dumps(payload, sort_keys=True), encoding="utf-8"
    )


def _seed(base: Path) -> None:
    base.mkdir(parents=True, exist_ok=True)
    _write_artifact(base, 1, "see_gate", {"a": 1})
    _write_artifact(base, 2, "problem_formalization", {"b": 2})
    _write_artifact(base, 3, "multi_option_generation", {"options": [1, 2, 3, 4, 5]})
    _write_artifact(base, 4, "evaluation_matrix", {"m": 4})
    _write_artifact(base, 5, "decision_record", {"d": 5})
    _write_artifact(base, 7, "implementation_plan", {"p": 7})
    _write_artifact(base, 10, "refinement_loop", {"r": 10})


def test_canonical_hash_deterministic_same_inputs(tmp_path: Path) -> None:
    base = tmp_path / "mpp_artifacts" / "task-a"
    _seed(base)
    h1 = canonical_mpp_hash(tmp_path, "task-a")
    h2 = canonical_mpp_hash(tmp_path, "task-a")
    assert h1 == h2


def test_canonical_hash_normalizes_key_order(tmp_path: Path) -> None:
    base = tmp_path / "mpp_artifacts" / "task-a"
    _seed(base)
    path = base / "stage_01_see_gate.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["content"] = {"z": 1, "a": 2}
    path.write_text(json.dumps(payload), encoding="utf-8")
    h1 = canonical_mpp_hash(tmp_path, "task-a")
    payload["content"] = {"a": 2, "z": 1}
    path.write_text(json.dumps(payload), encoding="utf-8")
    h2 = canonical_mpp_hash(tmp_path, "task-a")
    assert h1 == h2


def test_canonical_hash_changes_on_mutation(tmp_path: Path) -> None:
    base = tmp_path / "mpp_artifacts" / "task-a"
    _seed(base)
    before = canonical_mpp_hash(tmp_path, "task-a")
    path = base / "stage_05_decision_record.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["content"]["d"] = 999
    path.write_text(json.dumps(payload), encoding="utf-8")
    after = canonical_mpp_hash(tmp_path, "task-a")
    assert before != after


def test_missing_required_artifact_fails_closed(tmp_path: Path) -> None:
    base = tmp_path / "mpp_artifacts" / "task-a"
    _seed(base)
    (base / "stage_01_see_gate.json").unlink()
    with pytest.raises(MPPCanonicalError, match="Missing required MPP artifact"):
        canonical_mpp_hash(tmp_path, "task-a")

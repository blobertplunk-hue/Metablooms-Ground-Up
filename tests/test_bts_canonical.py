import json
from pathlib import Path

import pytest

from src.bts.bts_canonical import BTSCanonicalError, canonical_bts_hash


def _write_trace(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "options": [
                    {
                        "option_id": f"o{i}",
                        "approach_key": f"k{i}",
                        "touched_modules": [f"m{i}"],
                    }
                    for i in range(1, 6)
                ],
                "evaluation_scores": {f"o{i}": float(10 - i) for i in range(1, 6)},
                "rejected_options": [
                    {
                        "option_id": "o2",
                        "reason": "risk",
                        "criteria_links": ["operational_risk"],
                    },
                    {
                        "option_id": "o3",
                        "reason": "cost",
                        "criteria_links": ["complexity"],
                    },
                ],
                "decision_criteria": ["correctness", "complexity"],
                "chosen_option": "o1",
                "decision_confidence": 0.9,
                "timestamp": "2026-01-01T00:00:00+00:00",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_bts_hash_deterministic(tmp_path: Path) -> None:
    trace = tmp_path / "bts_artifacts" / "task-a" / "bts_trace.json"
    _write_trace(trace)
    assert canonical_bts_hash(tmp_path, "task-a") == canonical_bts_hash(
        tmp_path, "task-a"
    )


def test_bts_hash_changes_on_score_mutation(tmp_path: Path) -> None:
    trace = tmp_path / "bts_artifacts" / "task-a" / "bts_trace.json"
    _write_trace(trace)
    before = canonical_bts_hash(tmp_path, "task-a")
    payload = json.loads(trace.read_text(encoding="utf-8"))
    payload["evaluation_scores"]["o1"] = 1.0
    trace.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    after = canonical_bts_hash(tmp_path, "task-a")
    assert before != after


def test_bts_fails_on_missing_rejected_options(tmp_path: Path) -> None:
    trace = tmp_path / "bts_artifacts" / "task-a" / "bts_trace.json"
    _write_trace(trace)
    payload = json.loads(trace.read_text(encoding="utf-8"))
    payload["rejected_options"] = []
    trace.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    with pytest.raises(BTSCanonicalError, match="justification"):
        canonical_bts_hash(tmp_path, "task-a")


def test_bts_fails_on_insufficient_diversity(tmp_path: Path) -> None:
    trace = tmp_path / "bts_artifacts" / "task-a" / "bts_trace.json"
    _write_trace(trace)
    payload = json.loads(trace.read_text(encoding="utf-8"))
    payload["options"] = [
        {"option_id": f"o{i}", "approach_key": "same", "touched_modules": ["m"]}
        for i in range(1, 6)
    ]
    trace.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    with pytest.raises(BTSCanonicalError, match="insufficient option diversity"):
        canonical_bts_hash(tmp_path, "task-a")


def test_bts_fails_on_malformed_implementation_reality_metadata(tmp_path: Path) -> None:
    trace = tmp_path / "bts_artifacts" / "task-a" / "bts_trace.json"
    _write_trace(trace)
    payload = json.loads(trace.read_text(encoding="utf-8"))
    payload["implementation_reality"] = {"claimed_capability": "runtime"}
    trace.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    with pytest.raises(BTSCanonicalError, match="implementation reality metadata"):
        canonical_bts_hash(tmp_path, "task-a")

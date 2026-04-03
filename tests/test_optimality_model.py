import json
from pathlib import Path

import pytest

from src.optimality.optimality_model import (
    OptimalityError,
    canonical_optimality_hash,
)


def _write_trace(path: Path, *, chosen: str = "o1") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "chosen_option": chosen,
                "options": [
                    {
                        "option_id": f"o{i}",
                        "approach_key": f"k{i}",
                        "primary_mechanism": f"m{i}",
                        "touched_modules": [f"m{i}"],
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
                            "intervention": f"use-o{i}",
                            "predicted_outcome": "ok",
                            "confidence": 0.8 - (i * 0.05),
                        },
                    }
                    for i in range(1, 6)
                ],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_optimality_hash_deterministic(tmp_path: Path) -> None:
    p = tmp_path / "optimality_artifacts" / "task-a" / "optimality_trace.json"
    _write_trace(p)
    assert canonical_optimality_hash(tmp_path, "task-a") == canonical_optimality_hash(
        tmp_path, "task-a"
    )


def test_suboptimal_choice_fails_closed(tmp_path: Path) -> None:
    p = tmp_path / "optimality_artifacts" / "task-a" / "optimality_trace.json"
    _write_trace(p, chosen="o5")
    with pytest.raises(OptimalityError, match="not optimal"):
        canonical_optimality_hash(tmp_path, "task-a")


def test_missing_tradeoff_metric_fails_closed(tmp_path: Path) -> None:
    p = tmp_path / "optimality_artifacts" / "task-a" / "optimality_trace.json"
    _write_trace(p)
    payload = json.loads(p.read_text(encoding="utf-8"))
    payload["options"][0]["tradeoff"].pop("risk")
    p.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    with pytest.raises(OptimalityError, match="required optimality metrics"):
        canonical_optimality_hash(tmp_path, "task-a")

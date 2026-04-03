from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from src.replay_utils import canonical_json

OPTIMALITY_MODEL_VERSION = "1.0.0"
WEIGHTS = {
    "alternatives_score": 0.2,
    "tradeoff_score": 0.2,
    "counterfactual_score": 0.15,
    "justification_score": 0.15,
    "constraint_satisfaction": 0.2,
    "risk_penalty": 0.1,
}
REGRET_THRESHOLD = 0.0


class OptimalityError(RuntimeError):
    pass


def _load_trace(root: Path, task_id: str) -> dict[str, Any]:
    path = root / "optimality_artifacts" / task_id / "optimality_trace.json"
    if not path.exists():
        raise OptimalityError(f"Missing optimality artifact: {path.relative_to(root)}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise OptimalityError("Malformed optimality artifact") from exc
    if not isinstance(payload, dict):
        raise OptimalityError("Malformed optimality artifact object")
    return payload


def _score_option(option: dict[str, Any]) -> float:
    try:
        tradeoff = option["tradeoff"]
        counterfactual = option["counterfactual"]
        for key in ["cost", "benefit", "risk", "uncertainty"]:
            if key not in tradeoff:
                raise OptimalityError("Missing required optimality metrics")
        for key in ["intervention", "predicted_outcome", "confidence"]:
            if key not in counterfactual:
                raise OptimalityError("Missing required optimality metrics")
        score = (
            WEIGHTS["alternatives_score"] * float(option.get("alternatives_score", 0.0))
            + WEIGHTS["tradeoff_score"]
            * float(tradeoff.get("benefit", 0.0) - tradeoff.get("cost", 0.0))
            + WEIGHTS["counterfactual_score"]
            * float(counterfactual.get("confidence", 0.0))
            + WEIGHTS["justification_score"]
            * float(option.get("justification_score", 0.0))
            + WEIGHTS["constraint_satisfaction"]
            * float(option.get("constraint_satisfaction", 0.0))
            - WEIGHTS["risk_penalty"]
            * float(tradeoff.get("risk", 0.0) + tradeoff.get("uncertainty", 0.0))
        )
    except Exception as exc:
        raise OptimalityError("Missing required optimality metrics") from exc
    return round(score, 8)


def canonical_optimality_payload(root: Path, task_id: str) -> dict[str, Any]:
    payload = _load_trace(root, task_id)
    options = payload.get("options")
    chosen = str(payload.get("chosen_option", ""))
    if not isinstance(options, list) or len(options) < 5:
        raise OptimalityError("Optimality requires scoring all candidate options")
    score_matrix: dict[str, float] = {}
    signatures = set()
    for option in options:
        if not isinstance(option, dict) or "option_id" not in option:
            raise OptimalityError("Malformed option in optimality trace")
        oid = str(option["option_id"])
        score_matrix[oid] = _score_option(option)
        cf = option.get("counterfactual", {})
        if not all(
            k in cf for k in ["intervention", "predicted_outcome", "confidence"]
        ):
            raise OptimalityError("Missing counterfactual modeling fields")
        signatures.add(
            (
                str(option.get("approach_key", "")),
                tuple(sorted(str(m) for m in option.get("touched_modules", []))),
            )
        )
    if len(signatures) < 5:
        raise OptimalityError("Insufficient option diversity for salience")
    if chosen not in score_matrix:
        raise OptimalityError("Chosen option missing from score matrix")
    best_option = sorted(score_matrix.items(), key=lambda item: (-item[1], item[0]))[0][
        0
    ]
    regret = round(max(score_matrix.values()) - score_matrix[chosen], 8)
    if chosen != best_option:
        raise OptimalityError("Chosen option is not optimal")
    if regret > REGRET_THRESHOLD:
        raise OptimalityError("Regret above threshold")
    return {
        "model_version": OPTIMALITY_MODEL_VERSION,
        "task_id": task_id,
        "weights": WEIGHTS,
        "score_matrix": dict(sorted(score_matrix.items())),
        "chosen_option": chosen,
        "best_option": best_option,
        "regret": regret,
    }


def canonical_optimality_hash(root: Path, task_id: str) -> str:
    payload = canonical_optimality_payload(root, task_id)
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()

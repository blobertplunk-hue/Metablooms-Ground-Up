from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from jsonschema import validate

from src.replay_utils import canonical_json

NON_SEMANTIC_FIELDS = frozenset({"ts", "timestamp", "runtime_id", "execution_id"})
MIN_OPTIONS = 5


class BTSCanonicalError(RuntimeError):
    pass


def _strip_non_semantic(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_non_semantic(item)
            for key, item in sorted(value.items())
            if key not in NON_SEMANTIC_FIELDS
        }
    if isinstance(value, list):
        return [_strip_non_semantic(item) for item in value]
    return value


def _load_trace(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise BTSCanonicalError(f"Missing BTS artifact: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise BTSCanonicalError("Malformed BTS artifact JSON") from exc
    if not isinstance(payload, dict):
        raise BTSCanonicalError("Malformed BTS artifact object")
    return payload


def _option_signatures(
    options: list[dict[str, Any]],
) -> set[tuple[str, str, tuple[str, ...]]]:
    return {
        (
            str(item.get("approach_key", "")).strip().lower(),
            str(item.get("primary_mechanism", "")).strip().lower(),
            tuple(sorted(str(module) for module in item.get("touched_modules", []))),
        )
        for item in options
    }


def canonical_bts_payload(root: Path, task_id: str) -> dict[str, Any]:
    trace = _strip_non_semantic(
        _load_trace(root / "bts_artifacts" / task_id / "bts_trace.json")
    )
    schema_path = root / "BTS_TRACE_SCHEMA.json"
    if schema_path.exists():
        validate(
            instance=trace, schema=json.loads(schema_path.read_text(encoding="utf-8"))
        )
    for field in [
        "options",
        "evaluation_scores",
        "rejected_options",
        "decision_criteria",
        "chosen_option",
    ]:
        if field not in trace:
            raise BTSCanonicalError(f"BTS artifact missing required field: {field}")
    options = trace["options"]
    if not isinstance(options, list) or len(options) < MIN_OPTIONS:
        raise BTSCanonicalError("BTS completeness failure: fewer than 5 options")
    signatures = _option_signatures(
        [item for item in options if isinstance(item, dict)]
    )
    if len(signatures) < MIN_OPTIONS:
        raise BTSCanonicalError(
            "BTS completeness failure: insufficient option diversity"
        )

    evaluation = trace["evaluation_scores"]
    if not isinstance(evaluation, dict):
        raise BTSCanonicalError("BTS artifact malformed evaluation_scores")
    option_ids = {
        str(item.get("option_id")) for item in options if isinstance(item, dict)
    }
    if not option_ids.issubset(set(map(str, evaluation.keys()))):
        raise BTSCanonicalError("BTS completeness failure: evaluation missing options")

    rejected = trace["rejected_options"]
    if not isinstance(rejected, list) or not rejected:
        raise BTSCanonicalError("BTS justification failure: rejected options missing")
    for item in rejected:
        if not isinstance(item, dict):
            raise BTSCanonicalError(
                "BTS justification failure: malformed rejected option"
            )
        if not item.get("reason") or not item.get("criteria_links"):
            raise BTSCanonicalError(
                "BTS justification failure: missing reason/criteria linkage"
            )

    chosen = str(trace["chosen_option"])
    if chosen not in option_ids:
        raise BTSCanonicalError(
            "BTS decision consistency failure: chosen option absent"
        )

    score_values = [
        float(v) for v in evaluation.values() if isinstance(v, (int, float))
    ]
    spread = (max(score_values) - min(score_values)) if score_values else 0.0
    confidence_margin = float(evaluation.get(chosen, 0.0)) - max(
        [
            float(v)
            for k, v in evaluation.items()
            if str(k) != chosen and isinstance(v, (int, float))
        ]
        or [0.0]
    )
    diversity = len(signatures) / len(options)
    sufficiency = round(
        (diversity + min(1.0, spread / 10.0) + max(0.0, confidence_margin) / 10.0)
        / 3.0,
        6,
    )
    if sufficiency < 0.15:
        raise BTSCanonicalError("BTS search sufficiency failure")

    implementation_reality = trace.get("implementation_reality")
    if implementation_reality is not None:
        required = {
            "claimed_capability",
            "expected_changed_surfaces",
            "actual_changed_surfaces",
            "diff_classification",
            "claim_consistency_result",
        }
        if not isinstance(implementation_reality, dict) or not required.issubset(
            set(implementation_reality)
        ):
            raise BTSCanonicalError("BTS implementation reality metadata malformed")

    return {
        "task_id": task_id,
        "trace": trace,
        "search_sufficiency_score": sufficiency,
    }


def canonical_bts_hash(root: Path, task_id: str) -> str:
    payload = canonical_bts_payload(root, task_id)
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()

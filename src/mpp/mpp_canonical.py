from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from src.replay_utils import canonical_json

NON_SEMANTIC_FIELDS = frozenset({"ts", "timestamp", "generated_at", "updated_at"})
REQUIRED_REASONING_ARTIFACTS = (
    "stage_01_see_gate.json",
    "stage_02_problem_formalization.json",
    "stage_03_multi_option_generation.json",
    "stage_04_evaluation_matrix.json",
    "stage_05_decision_record.json",
    "stage_07_implementation_plan.json",
    "stage_10_refinement_loop.json",
)


class MPPCanonicalError(RuntimeError):
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


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MPPCanonicalError(f"Malformed MPP artifact: {path.name}") from exc
    if not isinstance(payload, dict):
        raise MPPCanonicalError(f"Invalid MPP artifact object: {path.name}")
    return payload


def canonical_mpp_payload(root: Path, task_id: str) -> dict[str, Any]:
    base = root / "mpp_artifacts" / task_id
    artifacts: list[dict[str, Any]] = []
    for name in REQUIRED_REASONING_ARTIFACTS:
        path = base / name
        if not path.exists():
            raise MPPCanonicalError(
                f"Missing required MPP artifact: {path.relative_to(root)}"
            )
        payload = _strip_non_semantic(_load_json(path))
        artifacts.append({"path": str(path.relative_to(root)), "payload": payload})
    return {"task_id": task_id, "artifacts": artifacts}


def canonical_mpp_hash(root: Path, task_id: str) -> str:
    payload = canonical_mpp_payload(root, task_id)
    canonical = canonical_json(payload)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

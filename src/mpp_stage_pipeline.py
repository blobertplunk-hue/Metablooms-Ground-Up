from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import validate


class MPPStageError(RuntimeError):
    pass


CRITERIA = (
    "correctness",
    "determinism_compatibility",
    "invariant_compatibility",
    "complexity",
    "extensibility",
    "operational_risk",
    "verification_friendliness",
)


@dataclass(frozen=True)
class StageArtifact:
    stage: int
    name: str
    path: Path
    payload: dict[str, Any]


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MPPStageError(f"Malformed JSON artifact: {path.name}") from exc


def _load_schema(root: Path, name: str) -> dict[str, Any]:
    schema_path = root / name
    if not schema_path.exists():
        raise MPPStageError(f"Missing required MPP schema: {name}")
    return _read_json(schema_path)


def _load_policy(
    root: Path, task_id: str, policy: dict[str, Any] | None
) -> dict[str, Any]:
    payload = dict(
        policy
        or {
            "task_id": task_id,
            "requires_see": True,
            "requires_multi_option": True,
            "requires_refinement": True,
        }
    )
    validate(payload, _load_schema(root, "MPP_STAGE_POLICY_SCHEMA.json"))
    if payload["task_id"] != task_id:
        raise MPPStageError("MPP policy task_id mismatch")
    return payload


def _artifact_path(root: Path, task_id: str, stage: int, name: str) -> Path:
    return root / "mpp_artifacts" / task_id / f"stage_{stage:02d}_{name}.json"


def _load_artifact(root: Path, task_id: str, stage: int, name: str) -> StageArtifact:
    path = _artifact_path(root, task_id, stage, name)
    if not path.exists():
        raise MPPStageError(
            f"Missing required MPP stage artifact: {path.relative_to(root)}"
        )
    payload = _read_json(path)
    validate(payload, _load_schema(root, "MPP_STAGE_ARTIFACT_SCHEMA.json"))
    if payload.get("stage") != stage or payload.get("task_id") != task_id:
        raise MPPStageError(f"MPP stage artifact identity mismatch: {path.name}")
    content = payload.get("content")
    if not isinstance(content, dict):
        raise MPPStageError(f"MPP stage artifact missing object content: {path.name}")
    return StageArtifact(stage=stage, name=name, path=path, payload=payload)


def _require_fields(content: dict[str, Any], fields: list[str], stage: int) -> None:
    missing = [field for field in fields if field not in content]
    if missing:
        raise MPPStageError(f"Stage {stage} malformed artifact: missing {missing}")


def _assert_stage_1(artifact: StageArtifact, requires_see: bool) -> None:
    if not requires_see:
        return
    content = artifact.payload["content"]
    _require_fields(
        content,
        [
            "task_framing",
            "constraints",
            "sources",
            "failure_modes",
            "design_implications",
        ],
        stage=1,
    )


def _assert_stage_2(artifact: StageArtifact) -> None:
    content = artifact.payload["content"]
    _require_fields(
        content,
        [
            "inputs",
            "outputs",
            "constraints",
            "invariants_affected",
            "acceptance_criteria",
        ],
        stage=2,
    )


def _distinct_option_signature(
    option: dict[str, Any],
) -> tuple[str, str, tuple[str, ...]]:
    return (
        str(option.get("approach_key", "")).strip().lower(),
        str(option.get("primary_mechanism", "")).strip().lower(),
        tuple(sorted(str(item) for item in option.get("touched_modules", []))),
    )


def _assert_stage_3(artifact: StageArtifact, requires_multi_option: bool) -> list[str]:
    content = artifact.payload["content"]
    options = content.get("options")
    if not isinstance(options, list):
        raise MPPStageError("Stage 3 malformed artifact: options must be a list")
    valid_signatures = set()
    option_ids: list[str] = []
    for option in options:
        if not isinstance(option, dict):
            continue
        _require_fields(
            option,
            ["option_id", "approach_key", "primary_mechanism", "touched_modules"],
            stage=3,
        )
        signature = _distinct_option_signature(option)
        if all(signature):
            valid_signatures.add(signature)
            option_ids.append(str(option["option_id"]))
    if requires_multi_option and len(valid_signatures) < 5:
        raise MPPStageError(
            "Stage 3 fail-closed: fewer than 5 meaningfully distinct options"
        )
    return option_ids


def _assert_stage_4(artifact: StageArtifact, option_ids: list[str]) -> None:
    content = artifact.payload["content"]
    matrix = content.get("matrix")
    if not isinstance(matrix, list) or not matrix:
        raise MPPStageError("Stage 4 malformed artifact: matrix is required")
    scored_ids = set()
    for row in matrix:
        if not isinstance(row, dict):
            raise MPPStageError("Stage 4 malformed artifact: matrix row must be object")
        _require_fields(row, ["option_id", "scores", "rationale"], stage=4)
        scores = row["scores"]
        if not isinstance(scores, dict):
            raise MPPStageError("Stage 4 malformed artifact: scores must be object")
        for criterion in CRITERIA:
            value = scores.get(criterion)
            if not isinstance(value, (int, float)):
                raise MPPStageError(
                    f"Stage 4 malformed artifact: missing criterion {criterion}"
                )
        scored_ids.add(str(row["option_id"]))
    missing = sorted(set(option_ids) - scored_ids)
    if missing:
        raise MPPStageError(f"Stage 4 fail-closed: unscored options {missing}")


def _assert_stage_5(artifact: StageArtifact, option_ids: list[str]) -> None:
    content = artifact.payload["content"]
    _require_fields(
        content,
        [
            "chosen_option_id",
            "rejected_option_ids",
            "tradeoffs",
            "risks",
            "linked_artifacts",
        ],
        stage=5,
    )
    chosen = str(content["chosen_option_id"])
    if chosen not in set(option_ids):
        raise MPPStageError("Stage 5 fail-closed: chosen option not present in Stage 3")


def _assert_stage_6(artifact: StageArtifact) -> None:
    content = artifact.payload["content"]
    _require_fields(
        content,
        ["checked_stage_refs", "unbound_transitions", "unresolved_assumptions"],
        stage=6,
    )
    refs = set(content["checked_stage_refs"])
    if not {2, 3, 4, 5}.issubset(refs):
        raise MPPStageError("Stage 6 fail-closed: missing-middle references incomplete")
    if content["unbound_transitions"]:
        raise MPPStageError("Stage 6 fail-closed: unbound transitions detected")


def _assert_stage_7(artifact: StageArtifact) -> None:
    content = artifact.payload["content"]
    _require_fields(
        content,
        [
            "dependency_graph",
            "touched_modules",
            "invariants_impacted",
            "tests_required",
            "rollback_recovery",
        ],
        stage=7,
    )


def _assert_stage_8(artifact: StageArtifact, stage7: StageArtifact) -> None:
    content = artifact.payload["content"]
    _require_fields(
        content, ["linked_plan_artifact", "executed_steps", "touched_files"], stage=8
    )
    if str(content["linked_plan_artifact"]) != stage7.path.name:
        raise MPPStageError(
            "Stage 8 fail-closed: implementation not linked to stage 7 plan"
        )


def _assert_stage_9(artifact: StageArtifact) -> None:
    content = artifact.payload["content"]
    _require_fields(
        content,
        [
            "validation_surface",
            "invariant_registry_used",
            "validation_artifacts",
            "duplicate_validation_paths",
        ],
        stage=9,
    )
    if content["validation_surface"] != "existing_invariant_registry":
        raise MPPStageError(
            "Stage 9 fail-closed: validation must use invariant registry"
        )
    if not content["invariant_registry_used"]:
        raise MPPStageError("Stage 9 fail-closed: invariant registry was not used")
    if content["duplicate_validation_paths"]:
        raise MPPStageError("Stage 9 fail-closed: duplicate validation paths detected")


def _assert_stage_10(artifact: StageArtifact, required: bool) -> None:
    if not required:
        return
    content = artifact.payload["content"]
    _require_fields(
        content,
        ["comparison_against_rejected_options", "refinement_passes", "stop_reason"],
        stage=10,
    )


def enforce_mpp_stages(
    *,
    root: Path,
    task_id: str,
    policy: dict[str, Any] | None,
) -> dict[str, Any]:
    policy_payload = _load_policy(root, task_id, policy)
    stage1 = _load_artifact(root, task_id, 1, "see_gate")
    stage2 = _load_artifact(root, task_id, 2, "problem_formalization")
    stage3 = _load_artifact(root, task_id, 3, "multi_option_generation")
    stage4 = _load_artifact(root, task_id, 4, "evaluation_matrix")
    stage5 = _load_artifact(root, task_id, 5, "decision_record")
    stage6 = _load_artifact(root, task_id, 6, "missing_middle_detector")
    stage7 = _load_artifact(root, task_id, 7, "implementation_plan")
    stage8 = _load_artifact(root, task_id, 8, "implementation")
    stage9 = _load_artifact(root, task_id, 9, "validation")
    stage10 = _load_artifact(root, task_id, 10, "refinement_loop")

    _assert_stage_1(stage1, policy_payload["requires_see"])
    _assert_stage_2(stage2)
    option_ids = _assert_stage_3(stage3, policy_payload["requires_multi_option"])
    _assert_stage_4(stage4, option_ids)
    _assert_stage_5(stage5, option_ids)
    _assert_stage_6(stage6)
    _assert_stage_7(stage7)
    _assert_stage_8(stage8, stage7)
    _assert_stage_9(stage9)
    _assert_stage_10(stage10, policy_payload["requires_refinement"])

    return {
        "task_id": task_id,
        "policy": policy_payload,
        "stage_artifacts": [
            str(stage1.path.relative_to(root)),
            str(stage2.path.relative_to(root)),
            str(stage3.path.relative_to(root)),
            str(stage4.path.relative_to(root)),
            str(stage5.path.relative_to(root)),
            str(stage6.path.relative_to(root)),
            str(stage7.path.relative_to(root)),
            str(stage8.path.relative_to(root)),
            str(stage9.path.relative_to(root)),
            str(stage10.path.relative_to(root)),
        ],
        "compliant": True,
    }

import json
from pathlib import Path

import pytest

from src.turn_execution_engine import (
    EngineError,
    _execute_once_internal,
    execute_with_recovery,
)
from src.turn_execution_engine import EnginePaths


BASE_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["event_id", "type", "ts", "turn_id", "idempotency_key", "payload"],
    "properties": {
        "event_id": {"type": "string"},
        "type": {"type": "string", "enum": ["STAGE_ENQUEUED", "STAGE_EXECUTED"]},
        "ts": {"type": "string"},
        "turn_id": {"type": "integer"},
        "idempotency_key": {"type": "string"},
        "state_hash_before": {"type": "string"},
        "state_hash_after": {"type": "string"},
        "payload": {
            "type": "object",
            "required": ["stage_id", "bounded"],
            "properties": {
                "stage_id": {"type": "string"},
                "bounded": {"type": "boolean"},
                "mutates": {"type": "boolean"},
                "status": {"type": "string"},
                "params": {"type": "object"},
                "compensation": {"type": "object"},
            },
        },
    },
}


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_controls(root: Path) -> None:
    _write(root / "CURRENT_ROOT.json", {"canonical_root": str(root.resolve())})
    _write(root / "EVENT_SCHEMA.json", BASE_SCHEMA)
    (root / "AGENTS.md").write_text(
        "runtime_state.json\nevents.jsonl\n", encoding="utf-8"
    )
    (root / "MASTER_WORKFLOW_V2.md").write_text(
        "Load all control files\nReplay and validate events\nEnforce execution gates\n",
        encoding="utf-8",
    )
    (root / "REPLAY_RULES.md").write_text(
        "Replay must be deterministic\nstate_hash_before\nstate_hash_after\n",
        encoding="utf-8",
    )
    (root / "EXECUTION_GATE_SPEC.md").write_text(
        "schema_valid\nreplay_valid\nidempotency_valid\ncompensation_valid\nbounded_stage_available\n",
        encoding="utf-8",
    )
    (root / "ACCEPTANCE_TESTS.md").write_text(
        "Replay from `events.jsonl`\nExactly one bounded stage executed\nExecution is blocked\n",
        encoding="utf-8",
    )
    (root / "CDR_SPEC.md").write_text(
        "CDR-RAT\nCDR-ARCH\nCDR-CODE\nCDR-LINT\nTRADEOFFS\nFUTURE_GAPS\n",
        encoding="utf-8",
    )
    (root / "CDR_SECURITY.md").write_text(
        "INPUT_VALIDATION_REQUIRED\nOUTPUT_ENCODING_REQUIRED\nACCESS_CONTROL_REQUIRED\nSECRETS_HANDLING_REQUIRED\nSAFE_ERROR_HANDLING_REQUIRED\nSAFE_FILE_HANDLING_REQUIRED\n",
        encoding="utf-8",
    )
    (root / "CDR_VERIFICATION.md").write_text(
        "UNIT_TEST_REQUIRED\nINTEGRATION_TEST_REQUIRED\nACCEPTANCE_TEST_REQUIRED\nEXTERNAL_VALIDATION_REQUIRED\n",
        encoding="utf-8",
    )
    (root / "CDR_OBSERVABILITY.md").write_text(
        "STRUCTURED_LOGGING_REQUIRED\nSENSITIVE_DATA_REDACTION_REQUIRED\nNO_SECRET_LOGGING\nTRACEABILITY_REQUIRED\n",
        encoding="utf-8",
    )
    (root / "CDR_LIFECYCLE.md").write_text(
        "RATIONALE_MUST_UPDATE_WITH_CODE\nTRADEOFFS_MUST_BE_DOCUMENTED\nFUTURE_GAPS_REQUIRED\nVERSIONED_CHANGE_REQUIRED\n",
        encoding="utf-8",
    )
    (root / "MASTER_MPP_SCHEMA_v1.md").write_text(
        "Stage 11 = VALIDATION_ENGINE\nStage 12 = TRACE_VALIDATION\nStage 13 = COUNTERFACTUAL_TESTING\n",
        encoding="utf-8",
    )
    (root / "VALIDATION_LAYER_SPEC.md").write_text(
        "Stage 11 VALIDATION_ENGINE\nStage 12 TRACE_VALIDATION\nStage 13 COUNTERFACTUAL_TESTING\n",
        encoding="utf-8",
    )
    (root / "VALIDATION_RECEIPT_SCHEMA.json").write_text(
        '{"required":["trace_id","task_id","stage_id","timestamp","inputs_checked","artifacts_checked","rules_checked","mutation_proof","result","failures","warnings"]}',
        encoding="utf-8",
    )
    (root / "TRACE_VALIDATION_RECEIPT_SCHEMA.json").write_text(
        '{"required":["trace_id","task_id","stage_id","timestamp","execution_proof_ref","result","failures"]}',
        encoding="utf-8",
    )
    (root / "COUNTERFACTUAL_TEST_REPORT_SCHEMA.json").write_text(
        '{"required":["scenarios_tested","expected_failures","observed_failures","escaped_failures","result"]}',
        encoding="utf-8",
    )
    (root / "PROOF_REGISTRY_SCHEMA.json").write_text(
        '{"required":["trace_id","task_id","timestamp","execution","validation_receipt_ref","trace_receipt_ref","counterfactual_report_ref","mutation_proof","result"]}',
        encoding="utf-8",
    )


def _paths(root: Path) -> EnginePaths:
    return EnginePaths(
        root=root,
        current_root=root / "CURRENT_ROOT.json",
        events=root / "events.jsonl",
        runtime_state=root / "runtime_state.json",
        schema=root / "EVENT_SCHEMA.json",
        receipts_dir=root / "receipts",
        workflow_spec=root / "MASTER_WORKFLOW_V2.md",
        replay_rules=root / "REPLAY_RULES.md",
        gate_spec=root / "EXECUTION_GATE_SPEC.md",
        acceptance_tests=root / "ACCEPTANCE_TESTS.md",
        agents_instructions=root / "AGENTS.md",
        cdr_spec=root / "CDR_SPEC.md",
        cdr_security=root / "CDR_SECURITY.md",
        cdr_verification=root / "CDR_VERIFICATION.md",
        cdr_observability=root / "CDR_OBSERVABILITY.md",
        cdr_lifecycle=root / "CDR_LIFECYCLE.md",
        mpp_schema=root / "MASTER_MPP_SCHEMA_v1.md",
        validation_layer_spec=root / "VALIDATION_LAYER_SPEC.md",
    )


def _write_events(path: Path, events: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")


def _enqueue_event(event_id: str, turn_id: int, stage_id: str) -> dict:
    return {
        "event_id": event_id,
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": turn_id,
        "idempotency_key": f"enqueue:{stage_id}",
        "payload": {"stage_id": stage_id, "bounded": True, "mutates": False},
    }


def test_hard_failure_blocks_runtime_execution(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_events(
        tmp_path / "events.jsonl",
        [_enqueue_event("e1", 1, "s1"), _enqueue_event("e2", 2, "s2")],
    )
    _execute_once_internal(_paths(tmp_path))
    existing = (
        (tmp_path / "events.jsonl").read_text(encoding="utf-8").strip().splitlines()
    )

    with pytest.raises(EngineError, match="recovery lock|MPP self-test failed"):
        execute_with_recovery(
            _paths(tmp_path),
            task_id="task-hard",
            failure_class="HARD_FAILURE",
            retry_class="NON_RETRYABLE",
            required_override_token="ALLOW",
        )

    events_after = (
        (tmp_path / "events.jsonl").read_text(encoding="utf-8").strip().splitlines()
    )
    assert len(events_after) == len(existing)


def test_soft_failure_allows_one_retry_then_blocks_runtime(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_events(
        tmp_path / "events.jsonl",
        [_enqueue_event("e1", 1, "s1"), _enqueue_event("e2", 2, "s2")],
    )
    _execute_once_internal(_paths(tmp_path))

    first = execute_with_recovery(
        _paths(tmp_path),
        task_id="task-soft",
        failure_class="SOFT_FAILURE",
        retry_class="RETRYABLE",
    )
    assert first["executed_stage_id"] == "s2"
    existing = (
        (tmp_path / "events.jsonl").read_text(encoding="utf-8").strip().splitlines()
    )

    with pytest.raises(EngineError, match="recovery lock|MPP self-test failed"):
        execute_with_recovery(
            _paths(tmp_path),
            task_id="task-soft",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
        )

    lines_after = (
        (tmp_path / "events.jsonl").read_text(encoding="utf-8").strip().splitlines()
    )
    assert len(lines_after) == len(existing)


def test_override_allows_runtime_continuation_after_hard_failure(
    tmp_path: Path,
) -> None:
    _write_controls(tmp_path)
    _write_events(
        tmp_path / "events.jsonl",
        [_enqueue_event("e1", 1, "s1"), _enqueue_event("e2", 2, "s2")],
    )
    _execute_once_internal(_paths(tmp_path))
    existing = (
        (tmp_path / "events.jsonl").read_text(encoding="utf-8").strip().splitlines()
    )

    receipt = execute_with_recovery(
        _paths(tmp_path),
        task_id="task-override",
        failure_class="HARD_FAILURE",
        retry_class="NON_RETRYABLE",
        override_token="ALLOW",
        required_override_token="ALLOW",
    )

    assert receipt["executed_stage_id"] == "s2"
    events_after = (
        (tmp_path / "events.jsonl").read_text(encoding="utf-8").strip().splitlines()
    )
    assert len(events_after) == len(existing) + 1
    assert json.loads(events_after[-1])["type"] == "STAGE_EXECUTED"


def test_recovery_requires_proof_registry_chain(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    _write_events(tmp_path / "events.jsonl", [_enqueue_event("e1", 1, "s1")])
    with pytest.raises(EngineError, match="Missing required artifact|MPP guard failed"):
        execute_with_recovery(
            _paths(tmp_path),
            task_id="task-no-proof",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
        )

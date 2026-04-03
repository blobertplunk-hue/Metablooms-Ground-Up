import json
from pathlib import Path

from src.turn_execution_engine import (
    EnginePaths,
    _execute_once_internal,
    execute_with_recovery,
    replay_state,
)


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
    repo_root = Path(__file__).resolve().parents[1]
    for name in [
        "VALIDATION_RECEIPT_SCHEMA.json",
        "TRACE_VALIDATION_RECEIPT_SCHEMA.json",
        "COUNTERFACTUAL_TEST_REPORT_SCHEMA.json",
        "PROOF_REGISTRY_SCHEMA.json",
    ]:
        (root / name).write_text(
            (repo_root / name).read_text(encoding="utf-8"), encoding="utf-8"
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


def test_full_mpp_pipeline_integrity(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    events = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:00:00+00:00",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
        },
        {
            "event_id": "e2",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:00:30+00:00",
            "turn_id": 2,
            "idempotency_key": "enqueue:s2",
            "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
        },
    ]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )

    first = _execute_once_internal(_paths(tmp_path))
    assert first["executed_stage_id"] == "s1"

    all_events = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl")
        .read_text(encoding="utf-8")
        .strip()
        .splitlines()
    ]
    replayed = replay_state(all_events)
    runtime_state = json.loads(
        (tmp_path / "runtime_state.json").read_text(encoding="utf-8")
    )
    assert replayed == runtime_state

    for artifact in [
        "VALIDATION_RECEIPT.json",
        "TRACE_VALIDATION_RECEIPT.json",
        "COUNTERFACTUAL_TEST_REPORT.json",
        "EXECUTION_PROOF.json",
        "PROOF_REGISTRY.jsonl",
    ]:
        assert (tmp_path / artifact).exists()

    proof_entry = json.loads(
        (tmp_path / "PROOF_REGISTRY.jsonl")
        .read_text(encoding="utf-8")
        .strip()
        .splitlines()[-1]
    )
    trace = proof_entry["trace_id"]
    validation_receipt = json.loads(
        (tmp_path / "VALIDATION_RECEIPT.json").read_text(encoding="utf-8")
    )
    trace_receipt = json.loads(
        (tmp_path / "TRACE_VALIDATION_RECEIPT.json").read_text(encoding="utf-8")
    )
    execution_proof = json.loads(
        (tmp_path / "EXECUTION_PROOF.json").read_text(encoding="utf-8")
    )
    assert validation_receipt["trace_id"] == trace
    assert trace_receipt["trace_id"] == trace
    assert execution_proof["trace_id"] == trace

    second = execute_with_recovery(
        _paths(tmp_path),
        task_id="stage2-turn-execution",
        failure_class="SOFT_FAILURE",
        retry_class="RETRYABLE",
    )
    assert second["executed_stage_id"] == "s2"

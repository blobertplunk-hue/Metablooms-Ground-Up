import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from src import turn_execution_engine as engine
from src.turn_execution_engine import (
    EngineError,
    EnginePaths,
    _execute_once_internal,
    execute_with_recovery,
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


def test_mpp_self_test_failure_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    enqueue = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    enqueue2 = {
        "event_id": "e2",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:30+00:00",
        "turn_id": 2,
        "idempotency_key": "enqueue:s2",
        "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(
        "\n".join([json.dumps(enqueue), json.dumps(enqueue2)]) + "\n", encoding="utf-8"
    )
    _execute_once_internal(_paths(tmp_path))

    monkeypatch.setattr(
        "src.turn_execution_engine.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1, stdout="fail", stderr="boom"
        ),
    )
    event_count_before = len(
        (tmp_path / "events.jsonl").read_text(encoding="utf-8").strip().splitlines()
    )

    with pytest.raises(EngineError, match="MPP self-test failed"):
        execute_with_recovery(
            _paths(tmp_path),
            task_id="self-test",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
            changed_files=["src/turn_execution_engine.py"],
        )

    receipt = json.loads(
        (tmp_path / "MPP_SELF_TEST_RECEIPT.json").read_text(encoding="utf-8")
    )
    assert receipt["result"] == "FAIL"
    assert (
        len(
            (tmp_path / "events.jsonl").read_text(encoding="utf-8").strip().splitlines()
        )
        == event_count_before
    )


def test_mpp_self_test_success_writes_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    enqueue = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    enqueue2 = {
        "event_id": "e2",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:30+00:00",
        "turn_id": 2,
        "idempotency_key": "enqueue:s2",
        "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(
        "\n".join([json.dumps(enqueue), json.dumps(enqueue2)]) + "\n", encoding="utf-8"
    )
    _execute_once_internal(_paths(tmp_path))

    def _fake_run(args, **kwargs):
        if "scripts.mpp_guard" in args:
            (tmp_path / "GUARD_RECEIPT.json").write_text(
                json.dumps(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "mode": "ci",
                        "result": "PASS",
                        "error": "",
                        "run_id": args[args.index("--run-id") + 1],
                        "trace_id": args[args.index("--trace-id") + 1],
                        "execution_id": args[args.index("--execution-id") + 1],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("src.turn_execution_engine.subprocess.run", _fake_run)

    result = execute_with_recovery(
        _paths(tmp_path),
        task_id="self-test-ok",
        failure_class="SOFT_FAILURE",
        retry_class="RETRYABLE",
        changed_files=["src/validation_layer.py"],
    )
    assert result["executed_stage_id"] == "s2"

    receipt = json.loads(
        (tmp_path / "MPP_SELF_TEST_RECEIPT.json").read_text(encoding="utf-8")
    )
    assert receipt["result"] == "PASS"


def test_guard_zero_exit_without_receipt_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    enqueue = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    enqueue2 = {
        "event_id": "e2",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:30+00:00",
        "turn_id": 2,
        "idempotency_key": "enqueue:s2",
        "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(
        "\n".join([json.dumps(enqueue), json.dumps(enqueue2)]) + "\n", encoding="utf-8"
    )
    _execute_once_internal(_paths(tmp_path))

    monkeypatch.setattr(
        "src.turn_execution_engine.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="ok", stderr=""),
    )
    with pytest.raises(EngineError, match="MPP guard missing GUARD_RECEIPT.json"):
        execute_with_recovery(
            _paths(tmp_path),
            task_id="guard-no-receipt",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
            changed_files=["src/validation_layer.py"],
        )


def test_stale_same_day_guard_receipt_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    enqueue = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    enqueue2 = {
        "event_id": "e2",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:30+00:00",
        "turn_id": 2,
        "idempotency_key": "enqueue:s2",
        "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(
        "\n".join([json.dumps(enqueue), json.dumps(enqueue2)]) + "\n", encoding="utf-8"
    )
    _execute_once_internal(_paths(tmp_path))

    stale_time = datetime.now(timezone.utc) - timedelta(minutes=10)

    def _fake_run(args, **kwargs):
        if "scripts.mpp_guard" in args:
            (tmp_path / "GUARD_RECEIPT.json").write_text(
                json.dumps(
                    {
                        "timestamp": stale_time.isoformat(),
                        "mode": "ci",
                        "result": "PASS",
                        "error": "",
                        "run_id": args[args.index("--run-id") + 1],
                        "trace_id": args[args.index("--trace-id") + 1],
                        "execution_id": args[args.index("--execution-id") + 1],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("src.turn_execution_engine.subprocess.run", _fake_run)
    with pytest.raises(
        EngineError, match="Enforcement receipt is stale: GUARD_RECEIPT.json"
    ):
        execute_with_recovery(
            _paths(tmp_path),
            task_id="guard-stale-receipt",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
            changed_files=["src/validation_layer.py"],
        )


def test_guard_receipt_execution_id_mismatch_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    enqueue = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    enqueue2 = {
        "event_id": "e2",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:30+00:00",
        "turn_id": 2,
        "idempotency_key": "enqueue:s2",
        "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(
        "\n".join([json.dumps(enqueue), json.dumps(enqueue2)]) + "\n", encoding="utf-8"
    )
    _execute_once_internal(_paths(tmp_path))

    def _fake_run(args, **kwargs):
        if "scripts.mpp_guard" in args:
            (tmp_path / "GUARD_RECEIPT.json").write_text(
                json.dumps(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "mode": "ci",
                        "result": "PASS",
                        "error": "",
                        "run_id": args[args.index("--run-id") + 1],
                        "trace_id": args[args.index("--trace-id") + 1],
                        "execution_id": "wrong-execution-id",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("src.turn_execution_engine.subprocess.run", _fake_run)
    with pytest.raises(
        EngineError,
        match="Enforcement receipt execution_id mismatch: GUARD_RECEIPT.json",
    ):
        execute_with_recovery(
            _paths(tmp_path),
            task_id="guard-mismatch",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
            changed_files=["src/validation_layer.py"],
        )


def test_self_test_receipt_execution_id_mismatch_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    enqueue = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    enqueue2 = {
        "event_id": "e2",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:30+00:00",
        "turn_id": 2,
        "idempotency_key": "enqueue:s2",
        "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(
        "\n".join([json.dumps(enqueue), json.dumps(enqueue2)]) + "\n", encoding="utf-8"
    )
    _execute_once_internal(_paths(tmp_path))

    real_self_test = engine._run_mpp_self_test

    def _wrong_self_test(root, changed_files, *, execution_id=None):
        return real_self_test(root, changed_files, execution_id="wrong-execution-id")

    monkeypatch.setattr(engine, "_run_mpp_self_test", _wrong_self_test)
    with pytest.raises(
        EngineError,
        match="Enforcement receipt execution_id mismatch: MPP_SELF_TEST_RECEIPT.json",
    ):
        execute_with_recovery(
            _paths(tmp_path),
            task_id="self-test-mismatch",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
            changed_files=["src/validation_layer.py"],
        )


def test_stage_executed_execution_id_mismatch_fails_closed(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    (tmp_path / "events.jsonl").write_text(
        json.dumps(
            {
                "event_id": "e1",
                "type": "STAGE_EXECUTED",
                "turn_id": 1,
                "payload": {"stage_id": "s1", "execution_id": "wrong"},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(EngineError, match="Execution event binding mismatch"):
        engine._verify_execution_event_binding(_paths(tmp_path), "expected")


def test_proof_registry_execution_id_mismatch_fails_closed(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    enqueue = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(enqueue) + "\n", encoding="utf-8")
    _execute_once_internal(_paths(tmp_path), execution_id="expected-execution-id")
    rows = [
        json.loads(x)
        for x in (tmp_path / "PROOF_REGISTRY.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if x.strip()
    ]
    rows[-1]["execution_id"] = "wrong-execution-id"
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
    )
    with pytest.raises(
        EngineError,
        match="Execution identity mismatch in required artifact: PROOF_REGISTRY.jsonl",
    ):
        engine._ensure_proof_registry_hard_dependency(
            _paths(tmp_path), expected_execution_id="expected-execution-id"
        )


def test_stale_receipt_replay_with_wrong_execution_id_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    enqueue = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    enqueue2 = {
        "event_id": "e2",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:30+00:00",
        "turn_id": 2,
        "idempotency_key": "enqueue:s2",
        "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(
        "\n".join([json.dumps(enqueue), json.dumps(enqueue2)]) + "\n", encoding="utf-8"
    )
    _execute_once_internal(_paths(tmp_path))
    stale_time = datetime.now(timezone.utc) - timedelta(minutes=10)

    def _fake_run(args, **kwargs):
        if "scripts.mpp_guard" in args:
            (tmp_path / "GUARD_RECEIPT.json").write_text(
                json.dumps(
                    {
                        "timestamp": stale_time.isoformat(),
                        "mode": "ci",
                        "result": "PASS",
                        "error": "",
                        "run_id": args[args.index("--run-id") + 1],
                        "trace_id": args[args.index("--trace-id") + 1],
                        "execution_id": "wrong-execution-id",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("src.turn_execution_engine.subprocess.run", _fake_run)
    with pytest.raises(
        EngineError,
        match="Enforcement receipt is stale: GUARD_RECEIPT.json|Enforcement receipt execution_id mismatch: GUARD_RECEIPT.json",
    ):
        execute_with_recovery(
            _paths(tmp_path),
            task_id="guard-stale-wrong-id",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
            changed_files=["src/validation_layer.py"],
        )

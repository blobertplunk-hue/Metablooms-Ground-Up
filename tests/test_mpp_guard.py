import json
from pathlib import Path

import pytest

from scripts.mpp_guard import run_guard
from src.turn_execution_engine import EngineError, execute_once
from src.validation_layer import append_registry_atomic


def _event() -> dict:
    return {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "turn_id": 1,
        "idempotency_key": "k",
        "payload": {"stage_id": "s1", "bounded": True},
    }


def test_guard_fails_on_state_mismatch(tmp_path: Path) -> None:
    (tmp_path / "events.jsonl").write_text(
        json.dumps(_event()) + "\n", encoding="utf-8"
    )
    (tmp_path / "runtime_state.json").write_text(
        json.dumps({"bad": True}), encoding="utf-8"
    )
    with pytest.raises(EngineError):
        run_guard(tmp_path, "staged")


def test_guard_fails_on_missing_proof_entry(tmp_path: Path) -> None:
    (tmp_path / "events.jsonl").write_text(
        json.dumps(_event()) + "\n", encoding="utf-8"
    )
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(
            {
                "pending_stages": [
                    {
                        "stage_id": "s1",
                        "bounded": True,
                        "mutates": False,
                        "compensation": None,
                        "params": {},
                    }
                ],
                "completed_stage_ids": [],
                "replayed_event_count": 1,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text("", encoding="utf-8")
    with pytest.raises(EngineError):
        run_guard(tmp_path, "staged")


def test_guard_fails_on_broken_proof_chain(tmp_path: Path) -> None:
    (tmp_path / "events.jsonl").write_text(
        json.dumps(_event()) + "\n", encoding="utf-8"
    )
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(
            {
                "pending_stages": [
                    {
                        "stage_id": "s1",
                        "bounded": True,
                        "mutates": False,
                        "compensation": None,
                        "params": {},
                    }
                ],
                "completed_stage_ids": [],
                "replayed_event_count": 1,
            }
        ),
        encoding="utf-8",
    )
    append_registry_atomic(
        tmp_path / "PROOF_REGISTRY.jsonl",
        {
            "trace_id": "t1",
            "task_id": "x",
            "execution": {},
            "mutation_proof": {},
            "result": "PASS",
            "events_hash": "x",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "validation_receipt_ref": "VALIDATION_RECEIPT.json",
            "trace_receipt_ref": "TRACE_VALIDATION_RECEIPT.json",
            "counterfactual_report_ref": "COUNTERFACTUAL_TEST_REPORT.json",
        },
    )
    lines = [
        json.loads(line)
        for line in (tmp_path / "PROOF_REGISTRY.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    lines[0]["prev_hash"] = "broken"
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text(
        "\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8"
    )
    with pytest.raises(EngineError):
        run_guard(tmp_path, "staged")


def test_guard_fails_on_mismatched_events_hash(tmp_path: Path) -> None:
    (tmp_path / "events.jsonl").write_text(
        json.dumps(_event()) + "\n", encoding="utf-8"
    )
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(
            {
                "pending_stages": [
                    {
                        "stage_id": "s1",
                        "bounded": True,
                        "mutates": False,
                        "compensation": None,
                        "params": {},
                    }
                ],
                "completed_stage_ids": [],
                "replayed_event_count": 1,
            }
        ),
        encoding="utf-8",
    )
    append_registry_atomic(
        tmp_path / "PROOF_REGISTRY.jsonl",
        {
            "trace_id": "t1",
            "task_id": "x",
            "execution": {},
            "mutation_proof": {},
            "result": "PASS",
            "events_hash": "bad",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "validation_receipt_ref": "VALIDATION_RECEIPT.json",
            "trace_receipt_ref": "TRACE_VALIDATION_RECEIPT.json",
            "counterfactual_report_ref": "COUNTERFACTUAL_TEST_REPORT.json",
        },
    )
    with pytest.raises(EngineError):
        run_guard(tmp_path, "staged")


def test_direct_execution_bypass_fails() -> None:
    with pytest.raises(EngineError):
        execute_once(None)  # type: ignore[arg-type]

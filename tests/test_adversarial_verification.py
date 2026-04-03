import json
from pathlib import Path

import pytest

from scripts.mpp_guard import run_guard
from scripts.verify_proof_chain import verify
from src.invariants import canonical_events_hash
from src.replay_utils import replay_state, state_hash
from src.turn_execution_engine import EngineError
from src.validation_layer import append_registry_atomic, load_registry_entries_resilient


def _base_events() -> list[dict]:
    return [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
        },
        {
            "event_id": "e2",
            "type": "STAGE_EXECUTED",
            "turn_id": 2,
            "idempotency_key": "turn-exec:s1",
            "state_hash_before": "a",
            "state_hash_after": state_hash(
                replay_state(
                    [
                        {
                            "event_id": "e1",
                            "type": "STAGE_ENQUEUED",
                            "turn_id": 1,
                            "idempotency_key": "enqueue:s1",
                            "payload": {
                                "stage_id": "s1",
                                "bounded": True,
                                "mutates": False,
                            },
                        },
                        {
                            "event_id": "e2",
                            "type": "STAGE_EXECUTED",
                            "turn_id": 2,
                            "idempotency_key": "turn-exec:s1",
                            "payload": {
                                "stage_id": "s1",
                                "bounded": True,
                                "mutates": False,
                                "output": {"stage_id": "s1", "status": "ok"},
                                "output_hash": "6f2cb0f4fd5fdb1308f463b0f8fbec0d288295f91b8dcf17666f26470b4f50cc",
                            },
                        },
                    ]
                )
            ),
            "payload": {
                "stage_id": "s1",
                "bounded": True,
                "mutates": False,
                "output": {"stage_id": "s1", "status": "ok"},
                "output_hash": "6f2cb0f4fd5fdb1308f463b0f8fbec0d288295f91b8dcf17666f26470b4f50cc",
            },
        },
    ]


def _setup_valid_case(root: Path) -> None:
    events = _base_events()
    (root / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )
    (root / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    entry = {
        "trace_id": "e2",
        "execution_id": "exec-1",
        "task_id": "stage2-turn-execution",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "events_hash": canonical_events_hash(events),
        "state_hash_after": state_hash(replay_state(events)),
        "execution": {"trace_id": "e2", "result": "PASS"},
        "validation_receipt_ref": "VALIDATION_RECEIPT.json",
        "trace_receipt_ref": "TRACE_VALIDATION_RECEIPT.json",
        "counterfactual_report_ref": "COUNTERFACTUAL_TEST_REPORT.json",
        "mutation_proof": {
            "target_id": "s1",
            "delta_observed": True,
            "pre_hash": "a",
            "post_hash": state_hash(replay_state(events)),
        },
        "result": "PASS",
    }
    append_registry_atomic(root / "PROOF_REGISTRY.jsonl", entry)


def test_adversarial_forged_proof_entry_fails_verifier_and_guard(
    tmp_path: Path,
) -> None:
    _setup_valid_case(tmp_path)
    lines = load_registry_entries_resilient(tmp_path / "PROOF_REGISTRY.jsonl")
    lines[-1]["current_hash"] = "forged"
    lines[-1]["entry_hash"] = "forged"
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text(
        "\n".join(json.dumps(e, sort_keys=True) for e in lines) + "\n", encoding="utf-8"
    )
    assert verify(tmp_path)["result"] == "FAIL"
    with pytest.raises(EngineError):
        run_guard(tmp_path, "ci")


def test_adversarial_duplicate_trace_id_fails_verifier_and_guard(
    tmp_path: Path,
) -> None:
    _setup_valid_case(tmp_path)
    line = (tmp_path / "PROOF_REGISTRY.jsonl").read_text(encoding="utf-8").strip()
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text(
        line + "\n" + line + "\n", encoding="utf-8"
    )
    assert verify(tmp_path)["result"] == "FAIL"
    with pytest.raises(EngineError):
        run_guard(tmp_path, "ci")


def test_adversarial_reordered_chain_fails_verifier_and_guard(tmp_path: Path) -> None:
    _setup_valid_case(tmp_path)
    base = load_registry_entries_resilient(tmp_path / "PROOF_REGISTRY.jsonl")[0]
    second = dict(base)
    second["trace_id"] = "e3"
    second["execution_id"] = "exec-2"
    append_registry_atomic(tmp_path / "PROOF_REGISTRY.jsonl", second)
    entries = load_registry_entries_resilient(tmp_path / "PROOF_REGISTRY.jsonl")
    entries = [entries[1], entries[0]]
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text(
        "\n".join(json.dumps(e, sort_keys=True) for e in entries) + "\n",
        encoding="utf-8",
    )
    assert verify(tmp_path)["result"] == "FAIL"
    with pytest.raises(EngineError):
        run_guard(tmp_path, "ci")


def test_adversarial_partial_chain_reconstruction_fails_verifier_and_guard(
    tmp_path: Path,
) -> None:
    _setup_valid_case(tmp_path)
    base = load_registry_entries_resilient(tmp_path / "PROOF_REGISTRY.jsonl")[0]
    second = dict(base)
    second["trace_id"] = "e3"
    second["execution_id"] = "exec-2"
    append_registry_atomic(tmp_path / "PROOF_REGISTRY.jsonl", second)
    entries = load_registry_entries_resilient(tmp_path / "PROOF_REGISTRY.jsonl")
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text(
        json.dumps(entries[-1], sort_keys=True) + "\n", encoding="utf-8"
    )
    assert verify(tmp_path)["result"] == "FAIL"
    with pytest.raises(EngineError):
        run_guard(tmp_path, "ci")


def test_adversarial_canonicalization_bypass_injection_fails_verifier_and_guard(
    tmp_path: Path,
) -> None:
    _setup_valid_case(tmp_path)
    events = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    events[0]["payload"]["injected"] = {"nested": "value"}
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    assert verify(tmp_path)["result"] == "FAIL"
    with pytest.raises(EngineError):
        run_guard(tmp_path, "ci")

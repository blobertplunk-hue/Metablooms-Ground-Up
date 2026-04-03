import json
import sys
import hashlib
import subprocess
from pathlib import Path

import pytest

from scripts.export_bundle import export_bundle
from scripts.verify_proof_chain import TCB_MODULES, verify
from src.bts.bts_canonical import canonical_bts_hash
from src.mpp.mpp_canonical import canonical_mpp_hash
from src.optimality.optimality_model import canonical_optimality_hash
from src.replay_utils import replay_state, state_hash
from src.validation_layer import (
    ValidationError,
    append_registry_atomic,
    load_registry_entries_resilient,
    proof_registry_snapshot_hash,
)


def _write_events(path: Path) -> list[dict]:
    events = [
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
            "payload": {
                "stage_id": "s1",
                "bounded": True,
                "mutates": False,
                "output": {"stage_id": "s1", "status": "ok"},
                "output_hash": "6f2cb0f4fd5fdb1308f463b0f8fbec0d288295f91b8dcf17666f26470b4f50cc",
            },
        },
    ]
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    return events


def _append_proof(root: Path, events: list[dict]) -> None:
    task_id = "stage2-turn-execution"
    artifacts = root / "mpp_artifacts" / task_id
    artifacts.mkdir(parents=True, exist_ok=True)
    seed = {
        "stage_01_see_gate.json": {
            "stage": 1,
            "task_id": task_id,
            "content": {"see": 1},
        },
        "stage_02_problem_formalization.json": {
            "stage": 2,
            "task_id": task_id,
            "content": {"problem": 1},
        },
        "stage_03_multi_option_generation.json": {
            "stage": 3,
            "task_id": task_id,
            "content": {"options": [1, 2, 3, 4, 5]},
        },
        "stage_04_evaluation_matrix.json": {
            "stage": 4,
            "task_id": task_id,
            "content": {"matrix": []},
        },
        "stage_05_decision_record.json": {
            "stage": 5,
            "task_id": task_id,
            "content": {"decision": 1},
        },
        "stage_07_implementation_plan.json": {
            "stage": 7,
            "task_id": task_id,
            "content": {"plan": 1},
        },
        "stage_10_refinement_loop.json": {
            "stage": 10,
            "task_id": task_id,
            "content": {"refinement": 1},
        },
    }
    for name, payload in seed.items():
        (artifacts / name).write_text(
            json.dumps(payload, sort_keys=True), encoding="utf-8"
        )
    bts_dir = root / "bts_artifacts" / task_id
    bts_dir.mkdir(parents=True, exist_ok=True)
    (bts_dir / "bts_trace.json").write_text(
        json.dumps(
            {
                "options": [
                    {
                        "option_id": f"o{i}",
                        "approach_key": f"k{i}",
                        "touched_modules": [f"m{i}"],
                    }
                    for i in range(1, 6)
                ],
                "evaluation_scores": {f"o{i}": float(10 - i) for i in range(1, 6)},
                "rejected_options": [
                    {
                        "option_id": "o2",
                        "reason": "risk",
                        "criteria_links": ["operational_risk"],
                    },
                    {
                        "option_id": "o3",
                        "reason": "cost",
                        "criteria_links": ["complexity"],
                    },
                    {
                        "option_id": "o4",
                        "reason": "fit",
                        "criteria_links": ["extensibility"],
                    },
                    {
                        "option_id": "o5",
                        "reason": "fit",
                        "criteria_links": ["extensibility"],
                    },
                ],
                "decision_criteria": ["correctness", "complexity", "operational_risk"],
                "chosen_option": "o1",
                "decision_confidence": 0.9,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    optimality_dir = root / "optimality_artifacts" / task_id
    optimality_dir.mkdir(parents=True, exist_ok=True)
    (optimality_dir / "optimality_trace.json").write_text(
        json.dumps(
            {
                "chosen_option": "o1",
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
    entry = {
        "trace_id": "e2",
        "execution_id": "exec-1",
        "task_id": task_id,
        "timestamp": "2026-01-01T00:00:00+00:00",
        "events_hash": "placeholder",
        "mpp_hash": canonical_mpp_hash(root, task_id),
        "bts_hash": canonical_bts_hash(root, task_id),
        "optimality_hash": canonical_optimality_hash(root, task_id),
        "state_hash_after": state_hash(replay_state(events)),
        "execution": {"trace_id": "e2", "result": "PASS"},
        "validation_receipt_ref": "VALIDATION_RECEIPT.json",
        "trace_receipt_ref": "TRACE_VALIDATION_RECEIPT.json",
        "counterfactual_report_ref": "COUNTERFACTUAL_TEST_REPORT.json",
        "mutation_proof": {
            "target_id": "s1",
            "delta_observed": True,
            "pre_hash": "a",
            "post_hash": "b",
        },
        "result": "PASS",
    }
    from src.invariants import canonical_events_hash

    entry["events_hash"] = canonical_events_hash(events)
    append_registry_atomic(root / "PROOF_REGISTRY.jsonl", entry)


def test_verify_proof_chain_passes_with_valid_artifacts(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)

    report = verify(tmp_path)
    assert report["result"] == "PASS"
    assert report["invariant_report"]["passed"] is True


def test_export_bundle_verify_passes(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    (tmp_path / "INVARIANT_COVERAGE_MAP.md").write_text("map", encoding="utf-8")
    (tmp_path / "VERIFY_INSTRUCTIONS.md").write_text("instructions", encoding="utf-8")
    _append_proof(tmp_path, events)

    out = tmp_path / "export"
    verify(tmp_path)
    export_bundle(tmp_path, out)
    assert (out / "events.jsonl").exists()
    assert (out / "proof_registry.jsonl").exists()
    assert (out / "audit_log.jsonl").exists()
    assert json.loads((out / "invariant_results.json").read_text())["result"] == "PASS"
    expected_snapshot = proof_registry_snapshot_hash(
        load_registry_entries_resilient(tmp_path / "PROOF_REGISTRY.jsonl")
    )
    assert (out / "proof_registry_snapshot.sha256").read_text(
        encoding="utf-8"
    ).strip() == expected_snapshot
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert all(item["path"] != "audit_log.jsonl" for item in manifest["files"])


def test_append_registry_rejects_prev_hash_overwrite_attempt(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    _append_proof(tmp_path, events)
    bad = {
        "trace_id": "e3",
        "execution_id": "exec-2",
        "task_id": "task",
        "timestamp": "2026-01-01T00:00:01+00:00",
        "events_hash": "x",
        "state_hash_after": "x",
        "execution": {"trace_id": "e3", "result": "PASS"},
        "validation_receipt_ref": "VALIDATION_RECEIPT.json",
        "trace_receipt_ref": "TRACE_VALIDATION_RECEIPT.json",
        "counterfactual_report_ref": "COUNTERFACTUAL_TEST_REPORT.json",
        "mutation_proof": {
            "target_id": "s1",
            "delta_observed": True,
            "pre_hash": "a",
            "post_hash": "b",
        },
        "result": "PASS",
        "prev_hash": "malicious-overwrite",
    }
    with pytest.raises(ValidationError, match="invalid prev_hash"):
        append_registry_atomic(tmp_path / "PROOF_REGISTRY.jsonl", bad)


def test_verifier_is_deterministic_across_runs(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)

    reports = [verify(tmp_path) for _ in range(5)]
    assert all(report == reports[0] for report in reports)


def test_verifier_remains_independent_from_runtime_engine(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)

    class _BlockedModule:
        def __getattr__(self, name: str) -> None:  # pragma: no cover - defensive
            raise AssertionError("runtime engine import should not be used by verifier")

    sys.modules["src.turn_execution_engine"] = _BlockedModule()  # type: ignore[assignment]
    try:
        report = verify(tmp_path)
    finally:
        sys.modules.pop("src.turn_execution_engine", None)
    assert report["result"] == "PASS"


def test_tcb_definition_excludes_runtime_engine() -> None:
    assert "src.turn_execution_engine" not in TCB_MODULES


def test_export_bundle_is_byte_deterministic(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    (tmp_path / "INVARIANT_COVERAGE_MAP.md").write_text("map", encoding="utf-8")
    (tmp_path / "VERIFY_INSTRUCTIONS.md").write_text("instructions", encoding="utf-8")
    _append_proof(tmp_path, events)

    out_a = tmp_path / "export_a"
    out_b = tmp_path / "export_b"
    export_bundle(tmp_path, out_a)
    export_bundle(tmp_path, out_b)

    files_a = sorted(
        str(p.relative_to(out_a))
        for p in out_a.rglob("*")
        if p.is_file() and p.name != "audit_log.jsonl"
    )
    files_b = sorted(
        str(p.relative_to(out_b))
        for p in out_b.rglob("*")
        if p.is_file() and p.name != "audit_log.jsonl"
    )
    assert files_a == files_b
    for name in files_a:
        data_a = (out_a / name).read_bytes()
        data_b = (out_b / name).read_bytes()
        assert data_a == data_b
        assert hashlib.sha256(data_a).hexdigest() == hashlib.sha256(data_b).hexdigest()


def test_verifier_fails_on_invariant_registry_version_mismatch(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    entries = load_registry_entries_resilient(tmp_path / "PROOF_REGISTRY.jsonl")
    entries[-1]["invariant_registry_version"] = "0.0.1"
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text(
        "\n".join(json.dumps(e, sort_keys=True) for e in entries) + "\n",
        encoding="utf-8",
    )
    assert verify(tmp_path)["result"] == "FAIL"


def test_verifier_fails_when_snapshot_hash_mismatch(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    (tmp_path / "proof_registry_snapshot.sha256").write_text(
        "bad-hash\n", encoding="utf-8"
    )
    assert verify(tmp_path)["result"] == "FAIL"


def test_verifier_fails_when_mpp_hash_mismatch(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    entries = load_registry_entries_resilient(tmp_path / "PROOF_REGISTRY.jsonl")
    entries[-1]["mpp_hash"] = "tampered"
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text(
        "\n".join(json.dumps(e, sort_keys=True) for e in entries) + "\n",
        encoding="utf-8",
    )
    assert verify(tmp_path)["result"] == "FAIL"


@pytest.mark.parametrize(
    "artifact_name",
    [
        "stage_01_see_gate.json",
        "stage_03_multi_option_generation.json",
        "stage_05_decision_record.json",
    ],
)
def test_verifier_fails_on_reasoning_artifact_tamper(
    tmp_path: Path, artifact_name: str
) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    artifact = tmp_path / "mpp_artifacts" / "stage2-turn-execution" / artifact_name
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    payload["content"]["tampered"] = True
    artifact.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    assert verify(tmp_path)["result"] == "FAIL"


def test_verifier_fails_on_bts_score_tamper(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    path = tmp_path / "bts_artifacts" / "stage2-turn-execution" / "bts_trace.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["evaluation_scores"]["o1"] = -100.0
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    assert verify(tmp_path)["result"] == "FAIL"


def test_verifier_fails_on_missing_bts_artifact(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    (tmp_path / "bts_artifacts" / "stage2-turn-execution" / "bts_trace.json").unlink()
    assert verify(tmp_path)["result"] == "FAIL"


def test_verifier_fails_on_optimality_tamper(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    path = (
        tmp_path
        / "optimality_artifacts"
        / "stage2-turn-execution"
        / "optimality_trace.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["chosen_option"] = "o5"
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    assert verify(tmp_path)["result"] == "FAIL"


def test_verifier_fails_when_manifest_mismatch(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    export_bundle(tmp_path, tmp_path / "export")
    manifest_path = tmp_path / "export" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"][0]["sha256"] = "bad"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    assert verify(tmp_path / "export")["result"] == "FAIL"


def test_old_version_entry_is_rejected_deterministically(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    entries = load_registry_entries_resilient(tmp_path / "PROOF_REGISTRY.jsonl")
    entries[-1].pop("invariant_registry_version", None)
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text(
        "\n".join(json.dumps(e, sort_keys=True) for e in entries) + "\n",
        encoding="utf-8",
    )
    report = verify(tmp_path)
    assert report["result"] == "FAIL"
    assert report["error"] == "invariant registry version incompatible"


def test_verifier_watch_mode_runs_and_exits_cleanly(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    result = subprocess.run(
        [
            "python",
            "-m",
            "scripts.verify_proof_chain",
            "--root",
            str(tmp_path),
            "--watch",
            "--max-iterations",
            "1",
            "--interval",
            "0.01",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


def test_release_signature_validates_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    monkeypatch.setenv("RELEASE_SIGNING_KEY", "test-key")
    export_bundle(tmp_path, tmp_path / "export")
    assert verify(tmp_path / "export")["result"] == "PASS"


def test_release_signature_mismatch_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    monkeypatch.setenv("RELEASE_SIGNING_KEY", "test-key")
    export_bundle(tmp_path, tmp_path / "export")
    payload = json.loads((tmp_path / "export" / "release_signature.json").read_text())
    payload["signature"] = "tampered"
    (tmp_path / "export" / "release_signature.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    assert verify(tmp_path / "export")["result"] == "FAIL"


def test_reproducible_bundle_hashes_match(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    out_a = tmp_path / "export_a"
    out_b = tmp_path / "export_b"
    export_bundle(tmp_path, out_a)
    export_bundle(tmp_path, out_b)
    sig_a = json.loads((out_a / "release_signature.json").read_text(encoding="utf-8"))
    sig_b = json.loads((out_b / "release_signature.json").read_text(encoding="utf-8"))
    assert sig_a["manifest_hash"] == sig_b["manifest_hash"]
    assert sig_a["bundle_hash"] == sig_b["bundle_hash"]


def test_cli_text_modes_return_success(tmp_path: Path) -> None:
    events = _write_events(tmp_path / "events.jsonl")
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(replay_state(events), sort_keys=True), encoding="utf-8"
    )
    _append_proof(tmp_path, events)
    export = subprocess.run(
        [
            "python",
            "-m",
            "scripts.export_bundle",
            "--root",
            str(tmp_path),
            "--output",
            str(tmp_path / "export"),
            "--format",
            "text",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    replay = subprocess.run(
        [
            "python",
            "-m",
            "scripts.replay",
            "--root",
            str(tmp_path),
            "--format",
            "text",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    verify_cmd = subprocess.run(
        [
            "python",
            "-m",
            "scripts.verify_proof_chain",
            "--root",
            str(tmp_path / "export"),
            "--format",
            "text",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert export.returncode == 0
    assert replay.returncode == 0
    assert verify_cmd.returncode == 0

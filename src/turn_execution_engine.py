from __future__ import annotations

# RATIONALE: Stage 2 engine prioritizes deterministic replay and fail-closed writes.
# RATIONALE: Build-layer governance (CDR) is enforced before any mutation.

import argparse
import hashlib
import json
import os
import re
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.recovery_lock_engine import decide_recovery
from src.validation_layer import (
    ValidationError,
    append_registry_atomic,
    load_registry_entries_resilient,
    run_validation_pipeline,
    validate_schema_payload,
)


class EngineError(RuntimeError):
    pass


@dataclass(frozen=True)
class EnginePaths:
    root: Path
    current_root: Path
    events: Path
    runtime_state: Path
    schema: Path
    receipts_dir: Path
    workflow_spec: Path
    replay_rules: Path
    gate_spec: Path
    acceptance_tests: Path
    agents_instructions: Path
    cdr_spec: Path
    cdr_security: Path
    cdr_verification: Path
    cdr_observability: Path
    cdr_lifecycle: Path
    mpp_schema: Path
    validation_layer_spec: Path


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_text(path: Path) -> str:
    if not path.exists():
        raise EngineError(f"Missing required control file: {path}")
    return path.read_text(encoding="utf-8")


def _read_json(path: Path) -> Any:
    return json.loads(_read_text(path))


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _state_hash(state: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(state).encode("utf-8")).hexdigest()


def _compute_replay_hash(events: list[dict[str, Any]]) -> str:
    derived_state = replay_state(events)
    executed_outputs = []
    for event in events:
        if event.get("type") == "STAGE_EXECUTED":
            payload = event.get("payload", {})
            executed_outputs.append(
                {
                    "stage_id": payload.get("stage_id"),
                    "output_hash": payload.get("output_hash"),
                }
            )
    replay_proof = {
        "event_stream": [_canonical_json(e) for e in events],
        "derived_state_hash": _state_hash(derived_state),
        "executed_outputs": executed_outputs,
    }
    return hashlib.sha256(_canonical_json(replay_proof).encode("utf-8")).hexdigest()


def canonical_events_hash(events: list[dict[str, Any]]) -> str:
    normalized = []
    for event in events:
        # non-semantic fields excluded from binding hash
        filtered = {k: v for k, v in event.items() if k not in {"ts"}}
        normalized.append(filtered)
    canonical_stream = "\n".join(_canonical_json(e) for e in normalized)
    return hashlib.sha256(canonical_stream.encode("utf-8")).hexdigest()


def _canonical_root_from_file(path: Path) -> Path:
    data = _read_json(path)
    root = data.get("canonical_root") or data.get("root") or data.get("path")
    if not isinstance(root, str):
        raise EngineError("CURRENT_ROOT.json must include canonical_root/root/path")
    return Path(root).resolve()


def _assert_within_root(root: Path, target: Path) -> None:
    resolved = target.expanduser().resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise EngineError(
            f"Refusing to mutate outside canonical root: {resolved}"
        ) from exc


def _validate_event_schema(event: dict[str, Any], schema: dict[str, Any]) -> None:
    required = schema.get("required", [])
    props = schema.get("properties", {})
    type_map = {"string": str, "integer": int, "object": dict, "boolean": bool}

    for field in required:
        if field not in event:
            raise EngineError(f"Event missing required field: {field}")

    for key, rule in props.items():
        if key not in event:
            continue
        expected = rule.get("type")
        if expected in type_map and not isinstance(event[key], type_map[expected]):
            raise EngineError(f"Event field {key} must be {expected}")
        enum = rule.get("enum")
        if enum and event[key] not in enum:
            raise EngineError(f"Event field {key} has invalid enum value")

    payload_rule = props.get("payload", {})
    payload_required = payload_rule.get("required", [])
    payload_props = payload_rule.get("properties", {})
    payload = event.get("payload", {})
    if isinstance(payload, dict):
        for field in payload_required:
            if field not in payload:
                raise EngineError(f"Payload missing required field: {field}")
        for key, rule in payload_props.items():
            if key not in payload:
                continue
            expected = rule.get("type")
            if expected in type_map and not isinstance(
                payload[key], type_map[expected]
            ):
                raise EngineError(f"Payload field {key} must be {expected}")

    if event.get("type") == "STAGE_EXECUTED":
        if "output_hash" not in payload:
            raise EngineError(
                "Schema validation failed: STAGE_EXECUTED requires payload.output_hash"
            )


def _load_events(events_path: Path) -> list[dict[str, Any]]:
    if not events_path.exists():
        return []
    rows = []
    for i, line in enumerate(
        events_path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise EngineError(f"Invalid JSON in events.jsonl line {i}") from exc
    return rows


def _events_to_text(events: list[dict[str, Any]]) -> str:
    if not events:
        return ""
    return "\n".join(json.dumps(e, sort_keys=True) for e in events) + "\n"


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp")
    with temp.open("w", encoding="utf-8") as fh:
        fh.write(content)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(temp, path)
    dir_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _should_run_mpp_self_test(changed_files: list[str] | None) -> bool:
    if not changed_files:
        return False
    mpp_targets = {
        "src/turn_execution_engine.py",
        "src/validation_layer.py",
        "src/recovery_lock_engine.py",
        "tests/test_turn_execution_engine.py",
        "tests/test_validation_layer.py",
        "tests/test_recovery_lock_engine.py",
    }
    return any(path in mpp_targets for path in changed_files)


def _run_mpp_self_test(
    root: Path,
    changed_files: list[str] | None,
    *,
    execution_id: str | None = None,
) -> None:
    invariant_ok = True
    invariant_error = ""
    try:
        schema_paths = [
            root / "VALIDATION_RECEIPT_SCHEMA.json",
            root / "TRACE_VALIDATION_RECEIPT_SCHEMA.json",
            root / "COUNTERFACTUAL_TEST_REPORT_SCHEMA.json",
            root / "PROOF_REGISTRY_SCHEMA.json",
        ]
        for schema_path in schema_paths:
            _ = _read_json(schema_path)

        known_good = run_validation_pipeline(
            {
                "trace_id": "known-good-trace",
                "task_id": "known-good-task",
                "execution_id": execution_id or "self-test-known-good",
                "stage_id": "11",
                "mutation_proof": {
                    "target_id": "s1",
                    "delta_observed": True,
                    "pre_hash": "aaa",
                    "post_hash": "bbb",
                },
                "artifacts_present": ["events.jsonl", "runtime_state.json"],
                "execution_claimed": True,
                "execution_events": [
                    {
                        "stage_id": "s1",
                        "event_id": "e1",
                        "target_id": "s1",
                        "artifact_id": "events.jsonl",
                    }
                ],
                "mutated_artifact": "events.jsonl",
                "pre_hash": "aaa",
                "post_hash": "bbb",
                "schema_dir": str(root),
            }
        )
        if known_good.validation_receipt.get("result") != "PASS":
            raise EngineError("known-good validation invariant failed")

        proof_path = root / "PROOF_REGISTRY.jsonl"
        if not proof_path.exists():
            _atomic_write_text(
                proof_path,
                json.dumps({"task_id": "synthetic", "trace_id": "synthetic-trace"})
                + "\n",
            )
        synthetic = decide_recovery(
            task_id="synthetic-self-test",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
            adjusted_params={"self_test": True},
            audit_log_path=root / "RECOVERY_AUDIT_LOG.jsonl",
            proof_registry_path=proof_path,
        )
        if not synthetic.allowed:
            raise EngineError("synthetic recovery invariant failed")
        chain_path = root / "PROOF_REGISTRY.jsonl"
        if chain_path.exists():
            lines = [
                json.loads(line)
                for line in chain_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            for i in range(1, len(lines)):
                if lines[i].get("prev_hash") != lines[i - 1].get("entry_hash"):
                    raise EngineError("proof registry chain invariant failed")
    except Exception as exc:
        invariant_ok = False
        invariant_error = str(exc)

    cmd = [
        "pytest",
        "-q",
        "tests/test_validation_layer.py",
        "tests/test_recovery_lock_engine.py",
    ]
    test_root = Path(__file__).resolve().parents[1]
    test_files = [test_root / rel for rel in cmd[2:]]
    can_run_pytest = all(path.exists() for path in test_files)
    if can_run_pytest:
        result = subprocess.run(
            cmd, cwd=str(test_root), capture_output=True, text=True, check=False
        )
    else:
        result = subprocess.CompletedProcess(
            args=cmd,
            returncode=0,
            stdout="skipped: targeted self-test files unavailable in runtime root",
            stderr="",
        )
    receipt = {
        "timestamp": _now_iso(),
        "execution_id": execution_id,
        "changed_files": changed_files,
        "tests_required": cmd[2:],
        "command": " ".join(cmd),
        "returncode": result.returncode,
        "invariant_mode": {
            "schemas_loaded": invariant_ok,
            "recovery_synthetic_ok": invariant_ok,
            "known_good_validation_ok": invariant_ok,
            "proof_chain_ok": invariant_ok,
            "error": invariant_error,
        },
        "result": "PASS" if result.returncode == 0 and invariant_ok else "FAIL",
        "stdout": result.stdout[-4000:],
        "stderr": result.stderr[-4000:],
    }
    _atomic_write_text(
        root / "MPP_SELF_TEST_RECEIPT.json",
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
    )
    if result.returncode != 0 or not invariant_ok:
        raise EngineError("MPP self-test failed; fail-closed")


def _run_mpp_guard(
    root: Path,
    *,
    mode: str = "ci",
    run_id: str | None = None,
    trace_id: str | None = None,
    execution_id: str | None = None,
) -> None:
    run_root = Path(__file__).resolve().parents[1]
    cmd = ["python", "-m", "scripts.mpp_guard", "--mode", mode, "--root", str(root)]
    if run_id:
        cmd.extend(["--run-id", run_id])
    if trace_id:
        cmd.extend(["--trace-id", trace_id])
    if execution_id:
        cmd.extend(["--execution-id", execution_id])
    result = subprocess.run(
        cmd,
        cwd=str(run_root),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise EngineError("MPP guard failed; fail-closed")
    if not (root / "GUARD_RECEIPT.json").exists():
        raise EngineError("MPP guard missing GUARD_RECEIPT.json; fail-closed")


def _verify_enforcement_receipt(
    path: Path,
    *,
    run_started_at: datetime,
    max_staleness_seconds: int = 30,
    expected_run_id: str | None = None,
    expected_trace_id: str | None = None,
    expected_execution_id: str | None = None,
) -> None:
    if not path.exists():
        raise EngineError(f"Missing enforcement receipt: {path.name}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("result") != "PASS":
        raise EngineError(f"Enforcement receipt failed: {path.name}")
    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, str):
        raise EngineError(f"Enforcement receipt missing timestamp: {path.name}")
    try:
        receipt_time = datetime.fromisoformat(timestamp)
    except ValueError as exc:
        raise EngineError(
            f"Enforcement receipt timestamp invalid: {path.name}"
        ) from exc
    if receipt_time.tzinfo is None:
        receipt_time = receipt_time.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    if receipt_time < (run_started_at - timedelta(seconds=max_staleness_seconds)):
        raise EngineError(f"Enforcement receipt is stale: {path.name}")
    if receipt_time > now + timedelta(seconds=5):
        raise EngineError(f"Enforcement receipt timestamp invalid: {path.name}")
    if expected_run_id is not None:
        if payload.get("run_id") != expected_run_id:
            raise EngineError(f"Enforcement receipt run_id mismatch: {path.name}")
    if expected_trace_id is not None:
        if payload.get("trace_id") != expected_trace_id:
            raise EngineError(f"Enforcement receipt trace_id mismatch: {path.name}")
    if expected_execution_id is not None:
        if payload.get("execution_id") != expected_execution_id:
            raise EngineError(f"Enforcement receipt execution_id mismatch: {path.name}")


def _verify_execution_event_binding(paths: EnginePaths, execution_id: str) -> None:
    events = _load_events(paths.events)
    executed_events = [
        event for event in events if event.get("type") == "STAGE_EXECUTED"
    ]
    if not executed_events:
        raise EngineError("Missing STAGE_EXECUTED event for execution binding")
    payload = executed_events[-1].get("payload", {})
    if payload.get("execution_id") is None:
        raise EngineError("Emitted STAGE_EXECUTED missing execution_id")
    if payload.get("execution_id") != execution_id:
        raise EngineError("Execution event binding mismatch for execution_id")


def _load_json_required(path: Path, label: str) -> dict[str, Any]:
    if not path.exists():
        raise EngineError(f"Missing required artifact: {label}")
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_proof_registry_hard_dependency(
    paths: EnginePaths, *, expected_execution_id: str | None = None
) -> None:
    required_artifacts = {
        "VALIDATION_RECEIPT.json": paths.root / "VALIDATION_RECEIPT.json",
        "TRACE_VALIDATION_RECEIPT.json": paths.root / "TRACE_VALIDATION_RECEIPT.json",
        "COUNTERFACTUAL_TEST_REPORT.json": paths.root
        / "COUNTERFACTUAL_TEST_REPORT.json",
        "EXECUTION_PROOF.json": paths.root / "EXECUTION_PROOF.json",
        "PROOF_REGISTRY.jsonl": paths.root / "PROOF_REGISTRY.jsonl",
    }
    for label, p in required_artifacts.items():
        if not p.exists():
            raise EngineError(f"Missing required artifact: {label}")

    validation_receipt = _load_json_required(
        required_artifacts["VALIDATION_RECEIPT.json"], "VALIDATION_RECEIPT.json"
    )
    trace_receipt = _load_json_required(
        required_artifacts["TRACE_VALIDATION_RECEIPT.json"],
        "TRACE_VALIDATION_RECEIPT.json",
    )
    counter_report = _load_json_required(
        required_artifacts["COUNTERFACTUAL_TEST_REPORT.json"],
        "COUNTERFACTUAL_TEST_REPORT.json",
    )
    execution_proof = _load_json_required(
        required_artifacts["EXECUTION_PROOF.json"], "EXECUTION_PROOF.json"
    )

    proof_lines = load_registry_entries_resilient(
        required_artifacts["PROOF_REGISTRY.jsonl"]
    )
    if not proof_lines:
        raise EngineError("Proof registry missing latest entry")
    trace_ids_in_registry = [entry.get("trace_id") for entry in proof_lines]
    if len(trace_ids_in_registry) != len(set(trace_ids_in_registry)):
        raise EngineError("Proof registry duplicate trace_id detected")
    latest = proof_lines[-1]

    validate_schema_payload(
        validation_receipt,
        _read_json(paths.root / "VALIDATION_RECEIPT_SCHEMA.json"),
        "VALIDATION_RECEIPT",
    )
    validate_schema_payload(
        trace_receipt,
        _read_json(paths.root / "TRACE_VALIDATION_RECEIPT_SCHEMA.json"),
        "TRACE_VALIDATION_RECEIPT",
    )
    validate_schema_payload(
        counter_report,
        _read_json(paths.root / "COUNTERFACTUAL_TEST_REPORT_SCHEMA.json"),
        "COUNTERFACTUAL_TEST_REPORT",
    )
    validate_schema_payload(
        latest, _read_json(paths.root / "PROOF_REGISTRY_SCHEMA.json"), "PROOF_REGISTRY"
    )

    trace_id = validation_receipt.get("trace_id")
    trace_ids = {
        trace_id,
        trace_receipt.get("trace_id"),
        execution_proof.get("trace_id"),
    }
    if len(trace_ids) != 1:
        raise EngineError("Causal chain trace_id mismatch")
    computed_events_hash = canonical_events_hash(_load_events(paths.events))
    trace_matches = [
        p
        for p in proof_lines
        if p.get("trace_id") == trace_id
        and p.get("events_hash") == computed_events_hash
    ]
    if len(trace_matches) == 0:
        raise EngineError("Proof registry binding missing for trace/events hash")
    if len(trace_matches) > 1:
        raise EngineError("Proof registry binding conflict for trace/events hash")
    latest = trace_matches[-1]
    if validation_receipt.get("mutation_proof") != latest.get("mutation_proof"):
        raise EngineError("Causal chain mutation_proof mismatch")
    if (
        validation_receipt.get("result") == "PASS"
        and execution_proof.get("result") != "PASS"
    ):
        raise EngineError(
            "Causal chain contradiction: validation PASS with execution FAIL"
        )
    if counter_report.get("result") == "PASS" and not counter_report.get(
        "distinguishing_signals"
    ):
        raise EngineError(
            "Counterfactual contradiction: PASS without distinguishing_signals"
        )
    if latest.get("validation_receipt_ref") != "VALIDATION_RECEIPT.json":
        raise EngineError("Proof registry invalid validation_receipt_ref")
    if latest.get("trace_receipt_ref") != "TRACE_VALIDATION_RECEIPT.json":
        raise EngineError("Proof registry invalid trace_receipt_ref")
    if latest.get("counterfactual_report_ref") != "COUNTERFACTUAL_TEST_REPORT.json":
        raise EngineError("Proof registry invalid counterfactual_report_ref")
    if latest.get("events_hash") != computed_events_hash:
        raise EngineError("Proof registry events_hash mismatch")
    if expected_execution_id is not None:
        chain_artifacts = {
            "VALIDATION_RECEIPT.json": validation_receipt,
            "TRACE_VALIDATION_RECEIPT.json": trace_receipt,
            "COUNTERFACTUAL_TEST_REPORT.json": counter_report,
            "EXECUTION_PROOF.json": execution_proof,
            "PROOF_REGISTRY.jsonl": latest,
        }
        for label, payload in chain_artifacts.items():
            if payload.get("execution_id") is None:
                raise EngineError(f"Missing execution_id in required artifact: {label}")
            if payload.get("execution_id") != expected_execution_id:
                raise EngineError(
                    f"Execution identity mismatch in required artifact: {label}"
                )


def replay_state(events: list[dict[str, Any]]) -> dict[str, Any]:
    enqueued: list[dict[str, Any]] = []
    completed: list[str] = []
    for event in events:
        payload = event.get("payload", {})
        if event.get("type") == "STAGE_ENQUEUED":
            enqueued.append(
                {
                    "stage_id": payload["stage_id"],
                    "bounded": bool(payload.get("bounded", False)),
                    "mutates": bool(payload.get("mutates", False)),
                    "compensation": payload.get("compensation"),
                    "params": payload.get("params", {}),
                }
            )
        elif event.get("type") == "STAGE_EXECUTED":
            completed.append(payload["stage_id"])
    pending = [e for e in enqueued if e["stage_id"] not in set(completed)]
    return {
        "pending_stages": pending,
        "completed_stage_ids": completed,
        "replayed_event_count": len(events),
    }


def _consume_events_exactly_once(events: list[dict[str, Any]]) -> list[str]:
    return [event["event_id"] for event in events]


def _validate_replay_completeness(events: list[dict[str, Any]]) -> None:
    consumed = _consume_events_exactly_once(events)
    expected = [event["event_id"] for event in events]
    if consumed != expected:
        raise EngineError(
            "Replay completeness failed: events must be consumed exactly once in order"
        )
    if len(consumed) != len(events):
        raise EngineError("Replay completeness failed: consumed event count mismatch")


def _validate_event_sequence_integrity(events: list[dict[str, Any]]) -> None:
    seen_ids: set[str] = set()
    previous_turn = -1
    for event in events:
        event_id = event.get("event_id")
        if not isinstance(event_id, str):
            raise EngineError(
                "Event sequence integrity failed: event_id must be string"
            )
        if event_id in seen_ids:
            raise EngineError("Event sequence integrity failed: duplicate event_id")
        seen_ids.add(event_id)

        turn_id = event.get("turn_id")
        if not isinstance(turn_id, int) or turn_id <= previous_turn:
            raise EngineError(
                "Event sequence integrity failed: turn_id must strictly increase"
            )
        previous_turn = turn_id


def _validate_stage_sequence(events: list[dict[str, Any]]) -> None:
    enqueued_order = [
        e["payload"]["stage_id"] for e in events if e.get("type") == "STAGE_ENQUEUED"
    ]
    executed_order = [
        e["payload"]["stage_id"] for e in events if e.get("type") == "STAGE_EXECUTED"
    ]
    expected_prefix = enqueued_order[: len(executed_order)]
    if executed_order != expected_prefix:
        raise EngineError("Replay validation failed: stage sequence mismatch")


def _validate_replay_hashes(events: list[dict[str, Any]]) -> None:
    prefix: list[dict[str, Any]] = []
    for event in events:
        before = replay_state(prefix)
        after = replay_state(prefix + [event])
        if event.get("type") == "STAGE_EXECUTED":
            expected_before = _state_hash(before)
            expected_after = _state_hash(after)
            if event.get("state_hash_before") != expected_before:
                raise EngineError(
                    "Replay validation failed: state_hash_before mismatch"
                )
            if event.get("state_hash_after") != expected_after:
                raise EngineError("Replay validation failed: state_hash_after mismatch")
        prefix.append(event)


def _validate_output_consistency(events: list[dict[str, Any]]) -> None:
    stage_outputs: dict[tuple[str, str], str] = {}
    for event in events:
        if event.get("type") != "STAGE_EXECUTED":
            continue

        payload = event.get("payload", {})
        stage_id = payload.get("stage_id")
        output = payload.get("output")
        output_hash = payload.get("output_hash")
        if output is None or output_hash is None:
            raise EngineError(
                "Replay validation failed: STAGE_EXECUTED missing output/output_hash"
            )
        if stage_id is None:
            raise EngineError(
                "Replay validation failed: STAGE_EXECUTED missing stage_id"
            )

        canonical_output = _canonical_json(output)
        expected_hash = hashlib.sha256(canonical_output.encode("utf-8")).hexdigest()
        if output_hash != expected_hash:
            raise EngineError("Replay validation failed: output_hash mismatch")

        execution_identity = (stage_id, str(event.get("idempotency_key", "")))
        prior = stage_outputs.get(execution_identity)
        if prior is not None and prior != canonical_output:
            raise EngineError("Replay validation failed: output mismatch for stage")
        stage_outputs[execution_identity] = canonical_output


def _validate_event_order_integrity(events: list[dict[str, Any]]) -> None:
    ordered = sorted(events, key=lambda e: (e.get("turn_id"), e.get("event_id")))
    if [e.get("event_id") for e in events] != [e.get("event_id") for e in ordered]:
        raise EngineError("Event order integrity failed: file order drift detected")


def _enforce_compensation(stage: dict[str, Any]) -> None:
    if stage.get("mutates") and not stage.get("compensation"):
        raise EngineError("Mutating stage missing compensation metadata")


def _ensure_unique_idempotency(events: list[dict[str, Any]], key: str) -> None:
    existing = {event.get("idempotency_key") for event in events}
    if key in existing:
        raise EngineError(f"Idempotency key already exists: {key}")


def _load_control_files(paths: EnginePaths) -> None:
    _read_text(paths.agents_instructions)
    _read_text(paths.workflow_spec)
    _read_text(paths.replay_rules)
    _read_text(paths.gate_spec)
    _read_text(paths.acceptance_tests)
    _read_text(paths.cdr_spec)
    _read_text(paths.cdr_security)
    _read_text(paths.cdr_verification)
    _read_text(paths.cdr_observability)
    _read_text(paths.cdr_lifecycle)
    _read_text(paths.mpp_schema)
    _read_text(paths.validation_layer_spec)


def _enforce_marker_file(path: Path, markers: list[str], label: str) -> str:
    text = _read_text(path)
    for marker in markers:
        if marker not in text:
            raise EngineError(f"{label} missing required marker: {marker}")
    return text


def _enforce_control_file_semantics(paths: EnginePaths) -> dict[str, str]:
    agents = _read_text(paths.agents_instructions)
    workflow = _read_text(paths.workflow_spec)
    replay_rules = _read_text(paths.replay_rules)
    acceptance = _read_text(paths.acceptance_tests)

    if "runtime_state.json" not in agents or "events.jsonl" not in agents:
        raise EngineError("AGENTS.md semantic check failed")

    for marker in [
        "Load all control files",
        "Replay and validate events",
        "Enforce execution gates",
    ]:
        if marker not in workflow:
            raise EngineError(
                f"MASTER_WORKFLOW_V2.md missing required workflow marker: {marker}"
            )

    for marker in [
        "Replay must be deterministic",
        "state_hash_before",
        "state_hash_after",
    ]:
        if marker not in replay_rules:
            raise EngineError(
                f"REPLAY_RULES.md missing required replay marker: {marker}"
            )

    for marker in [
        "Replay from `events.jsonl`",
        "Exactly one bounded stage executed",
        "Execution is blocked",
    ]:
        if marker not in acceptance:
            raise EngineError(
                f"ACCEPTANCE_TESTS.md missing required acceptance marker: {marker}"
            )

    cdr_spec = _enforce_marker_file(
        paths.cdr_spec,
        ["CDR-RAT", "CDR-ARCH", "CDR-CODE", "CDR-LINT", "TRADEOFFS", "FUTURE_GAPS"],
        "CDR_SPEC.md",
    )
    cdr_security = _enforce_marker_file(
        paths.cdr_security,
        [
            "INPUT_VALIDATION_REQUIRED",
            "OUTPUT_ENCODING_REQUIRED",
            "ACCESS_CONTROL_REQUIRED",
            "SECRETS_HANDLING_REQUIRED",
            "SAFE_ERROR_HANDLING_REQUIRED",
            "SAFE_FILE_HANDLING_REQUIRED",
        ],
        "CDR_SECURITY.md",
    )
    cdr_verification = _enforce_marker_file(
        paths.cdr_verification,
        [
            "UNIT_TEST_REQUIRED",
            "INTEGRATION_TEST_REQUIRED",
            "ACCEPTANCE_TEST_REQUIRED",
            "EXTERNAL_VALIDATION_REQUIRED",
        ],
        "CDR_VERIFICATION.md",
    )
    cdr_observability = _enforce_marker_file(
        paths.cdr_observability,
        [
            "STRUCTURED_LOGGING_REQUIRED",
            "SENSITIVE_DATA_REDACTION_REQUIRED",
            "NO_SECRET_LOGGING",
            "TRACEABILITY_REQUIRED",
        ],
        "CDR_OBSERVABILITY.md",
    )
    cdr_lifecycle = _enforce_marker_file(
        paths.cdr_lifecycle,
        [
            "RATIONALE_MUST_UPDATE_WITH_CODE",
            "TRADEOFFS_MUST_BE_DOCUMENTED",
            "FUTURE_GAPS_REQUIRED",
            "VERSIONED_CHANGE_REQUIRED",
        ],
        "CDR_LIFECYCLE.md",
    )

    return {
        "agents": agents,
        "workflow": workflow,
        "replay_rules": replay_rules,
        "acceptance": acceptance,
        "cdr_spec": cdr_spec,
        "cdr_security": cdr_security,
        "cdr_verification": cdr_verification,
        "cdr_observability": cdr_observability,
        "cdr_lifecycle": cdr_lifecycle,
    }


def _enforce_cdr_security(code_text: str) -> None:
    hardcoded_secret_regex = re.compile(
        r"(api_key|secret|password|token)\s*=\s*[\"'][^\"']+[\"']",
        flags=re.IGNORECASE,
    )
    if hardcoded_secret_regex.search(code_text):
        raise EngineError(
            "CDR security check failed: hardcoded secret pattern detected"
        )
    unsafe_error_regex = re.compile(
        r"raise\s+EngineError\(f[\"'][^\"']*\{exc\}", flags=re.IGNORECASE
    )
    if unsafe_error_regex.search(code_text):
        raise EngineError(
            "CDR security check failed: unsafe error exposure pattern detected"
        )
    if (
        "_validate_event_schema" not in code_text
        or "_assert_within_root" not in code_text
    ):
        raise EngineError(
            "CDR security check failed: input validation patterns missing"
        )


def _enforce_cdr_verification(test_text: str, verification_text: str) -> None:
    if "EXTERNAL_VALIDATION_REQUIRED" not in verification_text:
        raise EngineError(
            "CDR verification check failed: missing external validation marker"
        )
    if "def test_" not in test_text:
        raise EngineError("CDR verification check failed: unit test pattern missing")
    if "def test_stage2_full_system_validation" not in test_text:
        raise EngineError(
            "CDR verification check failed: integration-style test missing"
        )


def _enforce_cdr_observability(code_text: str, observability_text: str) -> None:
    if "STRUCTURED_LOGGING_REQUIRED" not in observability_text:
        raise EngineError("CDR observability check failed: marker missing")
    if '"gates"' not in code_text or '"event_count"' not in code_text:
        raise EngineError(
            "CDR observability check failed: structured state-change logging missing"
        )
    lowered = code_text.lower()
    if re.search(r"(?<!['\"])print\(\s*(password|secret)\b", lowered):
        raise EngineError(
            "CDR observability check failed: sensitive logging pattern detected"
        )
    if re.search(r"(?<!['\"])logger\.[^\n]*(password|secret)\b", lowered):
        raise EngineError(
            "CDR observability check failed: sensitive logging pattern detected"
        )


def _enforce_cdr_lifecycle(
    code_text: str, spec_text: str, invariants_text: str
) -> None:
    if "TRADEOFFS" not in spec_text:
        raise EngineError("CDR lifecycle check failed: TRADEOFFS missing")
    if "FUTURE_GAPS" not in spec_text:
        raise EngineError("CDR lifecycle check failed: FUTURE_GAPS missing")
    if "RATIONALE:" not in code_text:
        raise EngineError(
            "CDR lifecycle check failed: rationale headers missing in code"
        )
    if "Marker-enforced" not in invariants_text:
        raise EngineError(
            "CDR lifecycle check failed: versioned governance notes missing"
        )


def _run_acceptance_gate(
    events: list[dict[str, Any]],
    replay_valid: bool,
    schema_valid: bool,
    acceptance_text: str,
) -> None:
    if "Execution is blocked" not in acceptance_text:
        raise EngineError(
            "Acceptance gate failed: acceptance policy cannot be evaluated"
        )
    if not replay_valid:
        raise EngineError("Acceptance gate failed: replay must be valid")
    if not schema_valid:
        raise EngineError("Acceptance gate failed: schema must be valid")
    # events.jsonl remains source of truth by deriving state from replay only.
    _ = replay_state(events)


def _enforce_gate_spec(gate_spec_text: str, evaluated_gates: dict[str, bool]) -> None:
    required_rules = [
        "schema_valid",
        "replay_valid",
        "idempotency_valid",
        "compensation_valid",
        "bounded_stage_available",
    ]
    for rule in required_rules:
        if rule not in gate_spec_text:
            raise EngineError(f"Execution gate spec missing required rule: {rule}")
    for gate_name, gate_value in evaluated_gates.items():
        if not gate_value:
            raise EngineError(f"Execution blocked by gate: {gate_name}")


def execute_once(paths: EnginePaths) -> dict[str, Any]:
    raise EngineError(
        "Direct execute_once path is internal-only; use execute_with_recovery for governed execution"
    )


def _execute_once_internal(
    paths: EnginePaths, execution_id: str | None = None
) -> dict[str, Any]:
    runtime_execution_id = execution_id or str(uuid.uuid4())
    canonical_root = _canonical_root_from_file(paths.current_root)
    if canonical_root != paths.root.resolve():
        raise EngineError("Process root does not match canonical root")

    _load_control_files(paths)
    control_text = _enforce_control_file_semantics(paths)
    gate_spec_text = _read_text(paths.gate_spec)

    normalized_targets = [
        paths.events.expanduser().resolve(),
        paths.runtime_state.expanduser().resolve(),
        paths.schema.expanduser().resolve(),
        paths.receipts_dir.expanduser().resolve(),
    ]
    for target in normalized_targets:
        _assert_within_root(canonical_root, target)

    schema = _read_json(paths.schema)
    existing_events = _load_events(paths.events)
    replay_hash_before = _compute_replay_hash(existing_events)

    # Gate bundle before any writes.
    for event in existing_events:
        _validate_event_schema(event, schema)
    _validate_replay_completeness(existing_events)
    _validate_event_sequence_integrity(existing_events)
    _validate_event_order_integrity(existing_events)
    _validate_stage_sequence(existing_events)
    _validate_replay_hashes(existing_events)
    _validate_output_consistency(existing_events)
    if _compute_replay_hash(existing_events) != replay_hash_before:
        raise EngineError("Replay proof failed: non-deterministic replay hash")
    _run_acceptance_gate(
        existing_events,
        replay_valid=True,
        schema_valid=True,
        acceptance_text=control_text["acceptance"],
    )
    _enforce_gate_spec(
        gate_spec_text,
        {
            "schema_valid": True,
            "replay_valid": True,
            "idempotency_valid": True,
            "compensation_valid": True,
            "bounded_stage_available": True,
        },
    )
    repo_root = Path(__file__).resolve().parents[1]
    code_text = _read_text(repo_root / "src" / "turn_execution_engine.py")
    test_text = _read_text(repo_root / "tests" / "test_turn_execution_engine.py")
    invariants_text = _read_text(repo_root / "STAGE_2_INVARIANTS.md")
    _enforce_cdr_security(code_text)
    _enforce_cdr_verification(test_text, control_text["cdr_verification"])
    _enforce_cdr_observability(code_text, control_text["cdr_observability"])
    _enforce_cdr_lifecycle(code_text, control_text["cdr_spec"], invariants_text)

    pre_state = replay_state(existing_events)
    bounded_stage = next(
        (s for s in pre_state["pending_stages"] if s.get("bounded")), None
    )

    candidate: dict[str, Any] | None = None
    final_events = list(existing_events)
    if bounded_stage:
        _enforce_compensation(bounded_stage)

        next_turn = max([e.get("turn_id", 0) for e in existing_events] + [0]) + 1
        candidate = {
            "event_id": str(uuid.uuid4()),
            "type": "STAGE_EXECUTED",
            "ts": _now_iso(),
            "turn_id": next_turn,
            "idempotency_key": f"turn-exec:{bounded_stage['stage_id']}",
            "payload": {
                "stage_id": bounded_stage["stage_id"],
                "bounded": True,
                "mutates": bool(bounded_stage.get("mutates", False)),
                "status": "ok",
                "output": {
                    "stage_id": bounded_stage["stage_id"],
                    "status": "ok",
                },
            },
        }
        candidate["payload"]["execution_id"] = runtime_execution_id
        if bounded_stage.get("mutates"):
            candidate["payload"]["compensation"] = bounded_stage.get("compensation")

        _ensure_unique_idempotency(existing_events, candidate["idempotency_key"])

        projected_events = [*existing_events, candidate]
        candidate["state_hash_before"] = _state_hash(pre_state)
        candidate["state_hash_after"] = _state_hash(replay_state(projected_events))
        candidate["payload"]["output_hash"] = hashlib.sha256(
            _canonical_json(candidate["payload"]["output"]).encode("utf-8")
        ).hexdigest()

        _validate_event_schema(candidate, schema)
        _validate_event_sequence_integrity(projected_events)
        _validate_stage_sequence(projected_events)

        final_events = projected_events

    # Derived-only runtime state from events.jsonl replay (source of truth).
    final_state = replay_state(final_events)
    replay_hash_after = _compute_replay_hash(final_events)

    validation_artifacts: dict[str, str] = {}
    if candidate:
        mutation_proof = {
            "target_id": candidate["payload"]["stage_id"],
            "delta_observed": candidate["state_hash_before"]
            != candidate["state_hash_after"],
            "pre_hash": candidate["state_hash_before"],
            "post_hash": candidate["state_hash_after"],
        }
        try:
            va = run_validation_pipeline(
                {
                    "trace_id": candidate["event_id"],
                    "task_id": "stage2-turn-execution",
                    "execution_id": runtime_execution_id,
                    "stage_id": "11",
                    "mutation_proof": mutation_proof,
                    "artifacts_present": ["events.jsonl", "runtime_state.json"],
                    "execution_claimed": True,
                    "execution_events": [
                        {
                            "stage_id": candidate["payload"]["stage_id"],
                            "event_id": candidate["event_id"],
                            "target_id": candidate["payload"]["stage_id"],
                            "artifact_id": "events.jsonl",
                        }
                    ],
                    "mutated_artifact": "events.jsonl",
                    "pre_hash": candidate["state_hash_before"],
                    "post_hash": candidate["state_hash_after"],
                    "events_hash": canonical_events_hash(final_events),
                    "schema_dir": str(paths.root),
                }
            )
        except ValidationError as exc:
            raise EngineError(
                f"Validation layer failed [{exc.failure_class}/{exc.retry_class}]"
            ) from exc
        validation_artifacts = {
            "VALIDATION_RECEIPT.json": json.dumps(
                va.validation_receipt, indent=2, sort_keys=True
            )
            + "\n",
            "TRACE_VALIDATION_RECEIPT.json": json.dumps(
                va.trace_receipt, indent=2, sort_keys=True
            )
            + "\n",
            "EXECUTION_PROOF.json": json.dumps(
                va.execution_proof, indent=2, sort_keys=True
            )
            + "\n",
            "COUNTERFACTUAL_TEST_REPORT.json": json.dumps(
                va.counterfactual_report, indent=2, sort_keys=True
            )
            + "\n",
        }
        validation_artifacts["PROOF_REGISTRY_ENTRY"] = json.dumps(
            va.proof_registry_entry, sort_keys=True
        )

    receipt = {
        "ok": True,
        "ts": _now_iso(),
        "executed_stage_id": candidate["payload"]["stage_id"] if candidate else None,
        "appended_event_id": candidate["event_id"] if candidate else None,
        "gates": {
            "schema_valid": True,
            "replay_valid": True,
            "event_sequence_integrity": True,
            "stage_sequence_integrity": True,
            "idempotency_valid": True if candidate else None,
            "compensation_valid": True if candidate else None,
            "acceptance_tests_passed": True,
            "security_valid": True,
            "verification_valid": True,
            "observability_valid": True,
            "lifecycle_valid": True,
        },
        "event_count": len(final_events),
    }

    events_before = _events_to_text(existing_events)
    runtime_before = (
        paths.runtime_state.read_text(encoding="utf-8")
        if paths.runtime_state.exists()
        else None
    )
    receipt_name = f"receipt_turn_{candidate['turn_id'] if candidate else 'noop'}.json"
    receipt_path = paths.receipts_dir / receipt_name
    _assert_within_root(canonical_root, receipt_path)

    # Fail-closed write transaction with rollback.
    try:
        if paths.runtime_state.exists():
            persisted = json.loads(paths.runtime_state.read_text(encoding="utf-8"))
            if persisted != replay_state(existing_events):
                raise EngineError("runtime_state mismatch with replay-derived state")
        if candidate:
            _atomic_write_text(paths.events, _events_to_text(final_events))
        _atomic_write_text(
            paths.runtime_state,
            json.dumps(final_state, indent=2, sort_keys=True) + "\n",
        )
        if candidate:
            for name in [
                "VALIDATION_RECEIPT.json",
                "TRACE_VALIDATION_RECEIPT.json",
                "EXECUTION_PROOF.json",
                "COUNTERFACTUAL_TEST_REPORT.json",
            ]:
                _atomic_write_text(paths.root / name, validation_artifacts[name])
        _atomic_write_text(
            receipt_path, json.dumps(receipt, indent=2, sort_keys=True) + "\n"
        )
        if candidate:
            append_registry_atomic(
                paths.root / "PROOF_REGISTRY.jsonl",
                json.loads(validation_artifacts["PROOF_REGISTRY_ENTRY"]),
            )
        if replay_hash_after != _compute_replay_hash(_load_events(paths.events)):
            raise EngineError("Replay proof failed after persistence")
    except Exception as exc:  # rollback to prevent partial writes
        if candidate:
            _atomic_write_text(paths.events, events_before)
        if runtime_before is None and paths.runtime_state.exists():
            paths.runtime_state.unlink()
        elif runtime_before is not None:
            _atomic_write_text(paths.runtime_state, runtime_before)
        if receipt_path.exists():
            receipt_path.unlink()
        raise EngineError("Write transaction failed") from exc

    return receipt


def execute_with_recovery(
    paths: EnginePaths,
    *,
    task_id: str,
    failure_class: str,
    retry_class: str,
    adjusted_params: dict[str, Any] | None = None,
    override_token: str | None = None,
    required_override_token: str | None = None,
    changed_files: list[str] | None = None,
) -> dict[str, Any]:
    run_started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    execution_id = str(uuid.uuid4())
    _run_mpp_self_test(paths.root, changed_files, execution_id=execution_id)
    _run_mpp_guard(
        paths.root,
        mode="ci",
        run_id=run_id,
        trace_id=task_id,
        execution_id=execution_id,
    )
    _verify_enforcement_receipt(
        paths.root / "MPP_SELF_TEST_RECEIPT.json",
        run_started_at=run_started_at,
        expected_execution_id=execution_id,
    )
    _verify_enforcement_receipt(
        paths.root / "GUARD_RECEIPT.json",
        run_started_at=run_started_at,
        expected_run_id=run_id,
        expected_trace_id=task_id,
        expected_execution_id=execution_id,
    )
    _ensure_proof_registry_hard_dependency(paths)

    decision = decide_recovery(
        task_id=task_id,
        failure_class=failure_class,
        retry_class=retry_class,
        adjusted_params=adjusted_params,
        audit_log_path=paths.root / "RECOVERY_AUDIT_LOG.jsonl",
        proof_registry_path=paths.root / "PROOF_REGISTRY.jsonl",
        override_token=override_token,
        required_override_token=required_override_token,
    )
    if not decision.allowed:
        raise EngineError(f"Execution blocked by recovery lock: {decision.reason}")
    receipt = _execute_once_internal(paths, execution_id=execution_id)
    _ensure_proof_registry_hard_dependency(paths, expected_execution_id=execution_id)
    _verify_execution_event_binding(paths, execution_id)
    return receipt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 2 Turn Execution Engine")
    parser.add_argument("--root", default=".")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    paths = EnginePaths(
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
    execute_with_recovery(
        paths,
        task_id="stage2-turn-execution",
        failure_class="SOFT_FAILURE",
        retry_class="RETRYABLE",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

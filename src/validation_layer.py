from __future__ import annotations

import json
import os
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jsonschema import (
    Draft202012Validator,
    ValidationError as JsonSchemaValidationError,
    validate,
)
from src.invariants import INVARIANT_REGISTRY_VERSION, run_invariants
from src.bts.bts_canonical import BTSCanonicalError, canonical_bts_hash
from src.mpp.mpp_canonical import MPPCanonicalError, canonical_mpp_hash
from src.optimality.optimality_model import OptimalityError, canonical_optimality_hash


class ValidationError(RuntimeError):
    def __init__(self, message: str, failure_class: str, retry_class: str) -> None:
        super().__init__(message)
        self.failure_class = failure_class
        self.retry_class = retry_class


def _load_schema(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_schema(
    payload: dict[str, Any], schema: dict[str, Any], label: str
) -> None:
    try:
        Draft202012Validator.check_schema(schema)
        validate(instance=payload, schema=schema)
    except JsonSchemaValidationError as exc:
        raise ValidationError(
            f"{label} schema validation failed: {exc.message}",
            "HARD_FAILURE",
            "NON_RETRYABLE",
        ) from exc


def validate_schema_payload(
    payload: dict[str, Any], schema: dict[str, Any], label: str
) -> None:
    _validate_schema(payload, schema, label)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def classify_failure(reason: str) -> tuple[str, str]:
    lowered = reason.lower()
    if "timeout" in lowered or "transient" in lowered or "io" in lowered:
        return ("SOFT_FAILURE", "RETRYABLE")
    return ("HARD_FAILURE", "NON_RETRYABLE")


@dataclass(frozen=True)
class ValidationArtifacts:
    validation_receipt: dict[str, Any]
    trace_receipt: dict[str, Any]
    execution_proof: dict[str, Any]
    counterfactual_report: dict[str, Any]
    proof_registry_entry: dict[str, Any]


def _require_fields(
    payload: dict[str, Any], fields: list[str], label: str
) -> list[str]:
    missing = [f for f in fields if f not in payload]
    if missing:
        raise ValidationError(
            f"{label} missing required fields: {missing}",
            "HARD_FAILURE",
            "NON_RETRYABLE",
        )
    return missing


def run_validation_pipeline(context: dict[str, Any]) -> ValidationArtifacts:
    trace_id = context["trace_id"]
    task_id = context["task_id"]
    stage_id = context.get("stage_id", "11")
    execution_id = context.get("execution_id", trace_id)
    mutation_proof = context.get("mutation_proof")
    artifacts_present = context.get("artifacts_present", [])
    execution_events = context.get("execution_events", [])
    schema_dir = Path(context.get("schema_dir", "."))
    events_hash = context.get("events_hash", "")
    mpp_task_id = context.get("mpp_task_id")
    provided_mpp_hash = context.get("mpp_hash", "")
    computed_mpp_hash = ""
    bts_task_id = context.get("bts_task_id")
    provided_bts_hash = context.get("bts_hash", "")
    computed_bts_hash = ""
    optimality_task_id = context.get("optimality_task_id")
    provided_optimality_hash = context.get("optimality_hash", "")
    computed_optimality_hash = ""
    if mpp_task_id:
        try:
            computed_mpp_hash = canonical_mpp_hash(schema_dir, str(mpp_task_id))
        except MPPCanonicalError as exc:
            raise ValidationError(
                "mpp canonicalization failed: " + str(exc),
                "HARD_FAILURE",
                "NON_RETRYABLE",
            ) from exc
        if provided_mpp_hash and provided_mpp_hash != computed_mpp_hash:
            raise ValidationError(
                "mpp hash mismatch",
                "HARD_FAILURE",
                "NON_RETRYABLE",
            )
    if bts_task_id:
        try:
            computed_bts_hash = canonical_bts_hash(schema_dir, str(bts_task_id))
        except BTSCanonicalError as exc:
            raise ValidationError(
                "bts canonicalization failed: " + str(exc),
                "HARD_FAILURE",
                "NON_RETRYABLE",
            ) from exc
        if provided_bts_hash and provided_bts_hash != computed_bts_hash:
            raise ValidationError(
                "bts hash mismatch",
                "HARD_FAILURE",
                "NON_RETRYABLE",
            )
    if optimality_task_id:
        try:
            computed_optimality_hash = canonical_optimality_hash(
                schema_dir, str(optimality_task_id)
            )
        except OptimalityError as exc:
            raise ValidationError(
                "optimality canonicalization failed: " + str(exc),
                "HARD_FAILURE",
                "NON_RETRYABLE",
            ) from exc
        if (
            provided_optimality_hash
            and provided_optimality_hash != computed_optimality_hash
        ):
            raise ValidationError(
                "optimality hash mismatch",
                "HARD_FAILURE",
                "NON_RETRYABLE",
            )
    invariant_report = run_invariants(
        context,
        names={
            "MutationProofInvariant",
            "TraceConsistencyInvariant",
            "CounterfactualInvariant",
        },
    )
    result_map = {r.name: r for r in invariant_report["results"]}
    mutation_result = result_map["MutationProofInvariant"]
    trace_result_obj = result_map["TraceConsistencyInvariant"]
    counter_result_obj = result_map["CounterfactualInvariant"]
    failures = mutation_result.metadata.get("structured_failures", [])
    warnings: list[str] = []
    required_artifacts = mutation_result.metadata.get("required_artifacts", [])

    validation_result = "PASS" if not failures else "FAIL"
    validation_receipt = {
        "trace_id": trace_id,
        "execution_id": execution_id,
        "task_id": task_id,
        "stage_id": stage_id,
        "timestamp": _now_iso(),
        "inputs_checked": ["mutation_proof", "artifacts_present"],
        "artifacts_checked": required_artifacts,
        "rules_checked": [
            "requirement_validation",
            "schema_validation",
            "artifact_presence",
            "mutation_proof",
            "no_silent_success",
        ],
        "mutation_proof": mutation_proof,
        "result": validation_result,
        "failures": failures,
        "warnings": warnings,
    }

    trace_failures = trace_result_obj.failures
    trace_result = "PASS" if not trace_failures else "FAIL"
    execution_proof = {
        "trace_id": trace_id,
        "execution_id": execution_id,
        "execution_events": execution_events,
        "artifacts_produced": artifacts_present,
        "proof_bindings": {
            "mutation_target": mutation_proof.get("target_id")
            if mutation_proof
            else None
        },
        "mismatches": trace_failures,
        "result": trace_result,
    }
    trace_receipt = {
        "trace_id": trace_id,
        "execution_id": execution_id,
        "task_id": task_id,
        "stage_id": "12",
        "timestamp": _now_iso(),
        "execution_proof_ref": "EXECUTION_PROOF.json",
        "result": trace_result,
        "failures": trace_failures,
    }

    counter_failures = counter_result_obj.metadata.get("structured_failures", [])
    distinguishing_signals = counter_result_obj.metadata.get(
        "distinguishing_signals", []
    )

    scenarios = [
        "state_reversal",
        "boundary_null_missing_input",
        "duplicate_request",
        "missing_artifact",
        "soft_fake_success",
    ]
    counter_result = "PASS" if not counter_failures else "FAIL"
    counterfactual_report = {
        "execution_id": execution_id,
        "scenarios_tested": scenarios,
        "expected_failures": ["counterfactual_indistinguishable"],
        "observed_failures": [f["rule"] for f in counter_failures],
        "escaped_failures": [],
        "distinguishing_signals": distinguishing_signals,
        "result": counter_result,
    }

    # Schema enforcement for generated artifacts.
    _validate_schema(
        validation_receipt,
        _load_schema(schema_dir / "VALIDATION_RECEIPT_SCHEMA.json"),
        "VALIDATION_RECEIPT",
    )
    _validate_schema(
        trace_receipt,
        _load_schema(schema_dir / "TRACE_VALIDATION_RECEIPT_SCHEMA.json"),
        "TRACE_VALIDATION_RECEIPT",
    )
    _validate_schema(
        counterfactual_report,
        _load_schema(schema_dir / "COUNTERFACTUAL_TEST_REPORT_SCHEMA.json"),
        "COUNTERFACTUAL_TEST_REPORT",
    )

    if (
        validation_result != "PASS"
        or trace_result != "PASS"
        or counter_result != "PASS"
    ):
        primary = (failures + trace_failures + counter_failures)[0]
        fclass, rclass = primary["failure_class"], primary["retry_class"]
        raise ValidationError(
            f"validation layer failed: {primary['rule']}", fclass, rclass
        )

    proof_registry_entry = {
        "trace_id": trace_id,
        "execution_id": execution_id,
        "invariant_registry_version": INVARIANT_REGISTRY_VERSION,
        "task_id": task_id,
        "timestamp": _now_iso(),
        "prev_hash": "",
        "current_hash": "",
        "entry_hash": "",
        "events_hash": events_hash,
        "mpp_hash": computed_mpp_hash or provided_mpp_hash,
        "bts_hash": computed_bts_hash or provided_bts_hash,
        "optimality_hash": computed_optimality_hash or provided_optimality_hash,
        "state_hash_after": mutation_proof.get("post_hash") if mutation_proof else "",
        "execution": execution_proof,
        "validation_receipt_ref": "VALIDATION_RECEIPT.json",
        "trace_receipt_ref": "TRACE_VALIDATION_RECEIPT.json",
        "counterfactual_report_ref": "COUNTERFACTUAL_TEST_REPORT.json",
        "mutation_proof": mutation_proof,
        "result": "PASS",
    }
    _validate_schema(
        proof_registry_entry,
        _load_schema(schema_dir / "PROOF_REGISTRY_SCHEMA.json"),
        "PROOF_REGISTRY",
    )

    return ValidationArtifacts(
        validation_receipt=validation_receipt,
        trace_receipt=trace_receipt,
        execution_proof=execution_proof,
        counterfactual_report=counterfactual_report,
        proof_registry_entry=proof_registry_entry,
    )


def append_registry_atomic(path: Path, entry: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_entries = load_registry_entries_resilient(path)
    trace_id = entry.get("trace_id")
    if trace_id is not None and any(
        e.get("trace_id") == trace_id for e in existing_entries
    ):
        raise ValidationError(
            "proof registry duplicate trace_id", "HARD_FAILURE", "NON_RETRYABLE"
        )
    for i in range(1, len(existing_entries)):
        prev_current_hash = existing_entries[i - 1].get(
            "current_hash", existing_entries[i - 1].get("entry_hash")
        )
        if existing_entries[i].get("prev_hash") != prev_current_hash:
            raise ValidationError(
                "proof registry broken chain", "HARD_FAILURE", "NON_RETRYABLE"
            )
    for prior in existing_entries:
        prior_hash = prior.get("current_hash", prior.get("entry_hash"))
        prior_prev = prior.get("prev_hash", "")
        calc_prior = _now_hash(
            {"proof": _proof_hash_payload(prior), "prev_hash": prior_prev}
        )
        if prior_hash != calc_prior or prior.get("entry_hash") != prior_hash:
            raise ValidationError(
                "proof registry tampered prior entry", "HARD_FAILURE", "NON_RETRYABLE"
            )

    prior_hash = (
        existing_entries[-1].get("current_hash", existing_entries[-1].get("entry_hash"))
        if existing_entries
        else ""
    )
    entry = dict(entry)
    entry.setdefault("invariant_registry_version", INVARIANT_REGISTRY_VERSION)
    provided_prev = entry.get("prev_hash")
    if provided_prev not in (None, "", prior_hash):
        raise ValidationError(
            "proof registry invalid prev_hash for append",
            "HARD_FAILURE",
            "NON_RETRYABLE",
        )
    entry.setdefault("state_hash_after", "")
    entry["prev_hash"] = prior_hash
    current_hash = _now_hash(
        {"proof": _proof_hash_payload(entry), "prev_hash": prior_hash}
    )
    entry["current_hash"] = current_hash
    entry["entry_hash"] = current_hash

    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    dir_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _now_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _proof_hash_payload(entry: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "trace_id": entry.get("trace_id"),
        "invariant_registry_version": entry.get("invariant_registry_version"),
        "task_id": entry.get("task_id"),
        "execution": entry.get("execution"),
        "mutation_proof": entry.get("mutation_proof"),
        "state_hash_after": entry.get("state_hash_after"),
        "result": entry.get("result"),
    }
    if "mpp_hash" in entry:
        payload["mpp_hash"] = entry.get("mpp_hash", "")
    if "bts_hash" in entry:
        payload["bts_hash"] = entry.get("bts_hash", "")
    if "optimality_hash" in entry:
        payload["optimality_hash"] = entry.get("optimality_hash", "")
    return payload


def proof_registry_snapshot_hash(entries: list[dict[str, Any]]) -> str:
    rolling = ""
    for entry in entries:
        current = entry.get("current_hash", entry.get("entry_hash", ""))
        rolling = _now_hash({"prev": rolling, "current": current})
    return rolling


def load_registry_entries_resilient(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    data = path.read_bytes()
    lines = data.splitlines(keepends=True)
    valid_entries: list[dict[str, Any]] = []
    valid_bytes = 0
    for line in lines:
        text = line.decode("utf-8")
        if not text.strip():
            valid_bytes += len(line)
            continue
        try:
            valid_entries.append(json.loads(text))
            valid_bytes += len(line)
        except json.JSONDecodeError:
            break
    if valid_bytes != len(data):
        with path.open("wb") as fh:
            fh.write(data[:valid_bytes])
            fh.flush()
            os.fsync(fh.fileno())
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    return valid_entries

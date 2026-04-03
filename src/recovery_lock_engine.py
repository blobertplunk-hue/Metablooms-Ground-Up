from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RecoveryDecision:
    task_id: str
    allowed: bool
    action: str
    reason: str
    linked_proof: dict[str, Any] | None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _append_jsonl_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    tmp = path.with_name(f".{path.name}.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        fh.write(existing + json.dumps(payload, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    dir_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _latest_proof_for_task(
    task_id: str, proof_registry_path: Path
) -> dict[str, Any] | None:
    entries = _read_jsonl(proof_registry_path)
    for entry in reversed(entries):
        if entry.get("task_id") == task_id:
            return entry
    return None


def decide_recovery(
    *,
    task_id: str,
    failure_class: str,
    retry_class: str,
    adjusted_params: dict[str, Any] | None,
    audit_log_path: Path,
    proof_registry_path: Path,
    override_token: str | None = None,
    required_override_token: str | None = None,
) -> RecoveryDecision:
    prior = [r for r in _read_jsonl(audit_log_path) if r.get("task_id") == task_id]
    soft_retries = [r for r in prior if r.get("action") == "retry"]
    linked_proof = _latest_proof_for_task(task_id, proof_registry_path)

    allowed = False
    action = "block"
    reason = "fail_closed"

    if failure_class == "HARD_FAILURE" or retry_class == "NON_RETRYABLE":
        if required_override_token and override_token == required_override_token:
            allowed = True
            action = "manual_continue"
            reason = "override_accepted"
        else:
            allowed = False
            action = "block"
            reason = "hard_failure_requires_override"
    elif failure_class == "SOFT_FAILURE" and retry_class == "RETRYABLE":
        if len(soft_retries) == 0:
            allowed = True
            action = "retry"
            reason = "single_soft_retry_allowed"
        else:
            allowed = False
            action = "block"
            reason = "soft_retry_limit_reached"
    else:
        allowed = False
        action = "block"
        reason = "unknown_failure_class"

    decision_record = {
        "timestamp": _now_iso(),
        "task_id": task_id,
        "failure_class": failure_class,
        "retry_class": retry_class,
        "action": action,
        "allowed": allowed,
        "reason": reason,
        "adjusted_params": adjusted_params or {},
        "override_used": bool(override_token),
        "linked_proof": linked_proof,
    }
    _append_jsonl_atomic(audit_log_path, decision_record)

    return RecoveryDecision(
        task_id=task_id,
        allowed=allowed,
        action=action,
        reason=reason,
        linked_proof=linked_proof,
    )

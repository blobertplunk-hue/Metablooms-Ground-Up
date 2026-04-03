import json
from pathlib import Path

from src.recovery_lock_engine import decide_recovery


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")


def test_retry_blocking_after_hard_failure(tmp_path: Path) -> None:
    proof = tmp_path / "PROOF_REGISTRY.jsonl"
    _write_jsonl(proof, [{"task_id": "t1", "trace_id": "x"}])
    audit = tmp_path / "RECOVERY_AUDIT_LOG.jsonl"

    decision = decide_recovery(
        task_id="t1",
        failure_class="HARD_FAILURE",
        retry_class="NON_RETRYABLE",
        adjusted_params=None,
        audit_log_path=audit,
        proof_registry_path=proof,
        required_override_token="ALLOW",
    )
    assert decision.allowed is False
    assert decision.action == "block"
    assert decision.linked_proof is not None


def test_soft_retry_allowance_single(tmp_path: Path) -> None:
    proof = tmp_path / "PROOF_REGISTRY.jsonl"
    _write_jsonl(proof, [{"task_id": "t2", "trace_id": "x"}])
    audit = tmp_path / "RECOVERY_AUDIT_LOG.jsonl"

    first = decide_recovery(
        task_id="t2",
        failure_class="SOFT_FAILURE",
        retry_class="RETRYABLE",
        adjusted_params={"temperature": 0.1},
        audit_log_path=audit,
        proof_registry_path=proof,
    )
    second = decide_recovery(
        task_id="t2",
        failure_class="SOFT_FAILURE",
        retry_class="RETRYABLE",
        adjusted_params={"temperature": 0.2},
        audit_log_path=audit,
        proof_registry_path=proof,
    )
    assert first.allowed is True and first.action == "retry"
    assert second.allowed is False and second.action == "block"


def test_override_behavior_allows_manual_continue(tmp_path: Path) -> None:
    proof = tmp_path / "PROOF_REGISTRY.jsonl"
    _write_jsonl(proof, [{"task_id": "t3", "trace_id": "x"}])
    audit = tmp_path / "RECOVERY_AUDIT_LOG.jsonl"

    decision = decide_recovery(
        task_id="t3",
        failure_class="HARD_FAILURE",
        retry_class="NON_RETRYABLE",
        adjusted_params={"manual": True},
        audit_log_path=audit,
        proof_registry_path=proof,
        override_token="ALLOW",
        required_override_token="ALLOW",
    )
    assert decision.allowed is True
    assert decision.action == "manual_continue"

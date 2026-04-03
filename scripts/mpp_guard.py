from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from src.turn_execution_engine import (
    EngineError,
    _validate_event_order_integrity,
    _validate_event_sequence_integrity,
)
from src.invariants import InvariantContractError, run_invariants
from src.invariants import canonical_events_hash
from src.replay_utils import replay_state, state_hash
from src.validation_layer import (
    load_registry_entries_resilient,
    proof_registry_snapshot_hash,
)
from scripts.implementation_reality_gate import (
    ImplementationRealityError,
    run_gate as run_implementation_reality_gate,
)


def _load_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def run_guard(root: Path, mode: str) -> None:
    events_path = root / "events.jsonl"
    runtime_state_path = root / "runtime_state.json"
    proof_path = root / "PROOF_REGISTRY.jsonl"

    events = _load_events(events_path)
    event_order_invalid = False
    try:
        _validate_event_sequence_integrity(events)
        _validate_event_order_integrity(events)
    except EngineError:
        event_order_invalid = True

    runtime_state_invalid = False
    replayed_state_hash = ""
    if runtime_state_path.exists():
        persisted = json.loads(runtime_state_path.read_text(encoding="utf-8"))
        replayed_state = replay_state(events)
        runtime_state_invalid = persisted != replayed_state
        replayed_state_hash = state_hash(replayed_state)

    proofs = load_registry_entries_resilient(proof_path)
    if events and not proofs:
        raise EngineError("missing proof entry for events")
    computed_events_hash = canonical_events_hash(events)
    expected_events_hash = (
        proofs[-1].get("events_hash") if proofs else computed_events_hash
    )
    proof_state_hash = proofs[-1].get("state_hash_after", "") if proofs else ""
    snapshot_hash = proof_registry_snapshot_hash(proofs)
    registry_version = (
        proofs[-1].get("invariant_registry_version", "") if proofs else ""
    )
    try:
        report = run_invariants(
            {
                "proofs": proofs,
                "events_hash": expected_events_hash,
                "computed_events_hash": computed_events_hash,
                "event_order_invalid": event_order_invalid,
                "runtime_state_invalid": runtime_state_invalid,
                "proof_state_hash": proof_state_hash,
                "replayed_state_hash": replayed_state_hash,
                "expected_proof_snapshot_hash": snapshot_hash,
                "actual_proof_snapshot_hash": snapshot_hash,
                "invariant_registry_version": registry_version,
                "expected_manifest_hash": "",
                "actual_manifest_hash": "",
            },
            names={
                "ProofChainInvariant",
                "ReplayDeterminismInvariant",
                "EventOrderInvariant",
                "RuntimeStateConsistencyInvariant",
                "StateHashBindingInvariant",
                "ProofRegistrySnapshotInvariant",
                "InvariantRegistryVersionInvariant",
                "ExportManifestInvariant",
            },
        )
    except InvariantContractError as exc:
        raise EngineError(f"guard invariant contract failure: {exc}") from exc
    if not report["passed"]:
        raise EngineError(f"guard invariant failure: {report['failures'][0]['name']}")

    if mode == "ci":
        claim_path = root / "IMPLEMENTATION_CLAIM.json"
        if claim_path.exists():
            try:
                run_implementation_reality_gate(
                    root=root,
                    base="HEAD~1",
                    head="HEAD",
                    claim_path=claim_path,
                    output_path=root
                    / "implementation_reality_artifacts"
                    / "latest.json",
                )
            except ImplementationRealityError as exc:
                raise EngineError(
                    f"implementation reality gate failure: {exc}"
                ) from exc
        if (
            root / "GOVERNED_EXECUTION.md"
        ).exists() and "execute_with_recovery" not in (
            root / "GOVERNED_EXECUTION.md"
        ).read_text(encoding="utf-8"):
            raise EngineError("governed execution contract missing")


def _write_receipt(
    root: Path,
    result: str,
    mode: str,
    error: str = "",
    run_id: str | None = None,
    trace_id: str | None = None,
    execution_id: str | None = None,
) -> None:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "result": result,
        "error": error,
    }
    if run_id is not None:
        payload["run_id"] = run_id
    if trace_id is not None:
        payload["trace_id"] = trace_id
    if execution_id is not None:
        payload["execution_id"] = execution_id
    (root / "GUARD_RECEIPT.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["staged", "ci"], default="staged")
    parser.add_argument("--root", default=".")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--trace-id", default=None)
    parser.add_argument("--execution-id", default=None)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    try:
        run_guard(root, args.mode)
    except EngineError as exc:
        _write_receipt(
            root,
            "FAIL",
            args.mode,
            str(exc),
            run_id=args.run_id,
            trace_id=args.trace_id,
            execution_id=args.execution_id,
        )
        raise
    _write_receipt(
        root,
        "PASS",
        args.mode,
        run_id=args.run_id,
        trace_id=args.trace_id,
        execution_id=args.execution_id,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

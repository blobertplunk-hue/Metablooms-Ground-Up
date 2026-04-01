from __future__ import annotations

import argparse
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

from src.turn_execution_engine import (
    EngineError,
    _validate_event_order_integrity,
    _validate_event_sequence_integrity,
    canonical_events_hash,
    replay_state,
)
from src.validation_layer import load_registry_entries_resilient


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
    _validate_event_sequence_integrity(events)
    _validate_event_order_integrity(events)

    if runtime_state_path.exists():
        persisted = json.loads(runtime_state_path.read_text(encoding="utf-8"))
        if persisted != replay_state(events):
            raise EngineError("runtime_state mismatch")

    proofs = load_registry_entries_resilient(proof_path)
    if events and not proofs:
        raise EngineError("missing proof entry for events")
    trace_ids = [p.get("trace_id") for p in proofs if p.get("trace_id")]
    if len(trace_ids) != len(set(trace_ids)):
        raise EngineError("duplicate trace_id in proof registry")

    if proofs:
        for i in range(1, len(proofs)):
            if proofs[i].get("prev_hash") != proofs[i - 1].get("entry_hash"):
                raise EngineError("broken proof chain")
        for entry in proofs:
            canonical = json.dumps(
                {
                    "proof": {
                        "trace_id": entry.get("trace_id"),
                        "task_id": entry.get("task_id"),
                        "execution": entry.get("execution"),
                        "mutation_proof": entry.get("mutation_proof"),
                        "result": entry.get("result"),
                    },
                    "prev_hash": entry.get("prev_hash", ""),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            if (
                entry.get("entry_hash")
                != hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            ):
                raise EngineError("tampered proof entry hash")
        event_hash = canonical_events_hash(events)
        if not any(p.get("events_hash") == event_hash for p in proofs):
            raise EngineError("no proof entry matches canonical events hash")

    if mode == "ci":
        if (
            root / "GOVERNED_EXECUTION.md"
        ).exists() and "execute_with_recovery" not in (
            root / "GOVERNED_EXECUTION.md"
        ).read_text(encoding="utf-8"):
            raise EngineError("governed execution contract missing")


def _write_receipt(root: Path, result: str, mode: str, error: str = "") -> None:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "result": result,
        "error": error,
    }
    (root / "GUARD_RECEIPT.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["staged", "ci"], default="staged")
    parser.add_argument("--root", default=".")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    try:
        run_guard(root, args.mode)
    except EngineError as exc:
        _write_receipt(root, "FAIL", args.mode, str(exc))
        raise
    _write_receipt(root, "PASS", args.mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

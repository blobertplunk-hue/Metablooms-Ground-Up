import json
from pathlib import Path

from scripts.export_bundle import export_bundle
from scripts.verify_proof_chain import verify
from src.invariants import canonical_events_hash
from src.replay_utils import replay_state, state_hash
from src.turn_execution_engine import _execute_once_internal
from tests.test_turn_execution_engine import _paths, _write_controls


def test_end_to_end_external_audit_pipeline(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    events = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:00:00+00:00",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
        }
    ]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n",
        encoding="utf-8",
    )
    _execute_once_internal(_paths(tmp_path))

    export_dir = tmp_path / "export"
    export_bundle(tmp_path, export_dir)

    verification = verify(tmp_path)
    assert verification["result"] == "PASS"
    assert (
        json.loads((export_dir / "invariant_results.json").read_text())["result"]
        == "PASS"
    )

    exported_events = [
        json.loads(line)
        for line in (export_dir / "events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    replayed = replay_state(exported_events)
    assert canonical_events_hash(exported_events) == verification["events_hash"]
    assert state_hash(replayed) == verification["state_hash"]

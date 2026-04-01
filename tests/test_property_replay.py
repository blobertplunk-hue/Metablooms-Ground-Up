import pytest

from src.turn_execution_engine import (
    EngineError,
    _compute_replay_hash,
    _validate_event_order_integrity,
)
from src.validation_layer import append_registry_atomic, load_registry_entries_resilient


def test_property_replay_deterministic() -> None:
    for turns in ([1], [1, 2, 3], [3, 5, 8, 13], list(range(1, 20))):
        events = [
            {
                "event_id": f"e{i}",
                "type": "STAGE_ENQUEUED",
                "turn_id": t,
                "idempotency_key": f"k{i}",
                "payload": {"stage_id": f"s{i}", "bounded": True},
            }
            for i, t in enumerate(sorted(turns), start=1)
        ]
        assert _compute_replay_hash(events) == _compute_replay_hash(events)


def test_property_event_order_mutation_fails() -> None:
    events = [
        {"event_id": "e1", "turn_id": 1},
        {"event_id": "e2", "turn_id": 2},
    ]
    _validate_event_order_integrity(events)
    with pytest.raises(EngineError):
        _validate_event_order_integrity(list(reversed(events)))


def test_property_proof_chain_random_append(tmp_path) -> None:
    p = tmp_path / "PROOF_REGISTRY.jsonl"
    for i in range(10):
        append_registry_atomic(
            p,
            {
                "trace_id": f"t{i}",
                "task_id": "x",
                "execution": {},
                "mutation_proof": {},
                "result": "PASS",
            },
        )
    rows = load_registry_entries_resilient(p)
    assert len(rows) == 10

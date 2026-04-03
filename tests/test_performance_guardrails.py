import time

from src.invariants import run_invariants
from src.replay_utils import replay_state


def _events(n: int = 300) -> list[dict]:
    events = []
    for i in range(1, n + 1):
        events.append(
            {
                "event_id": f"e{i}",
                "type": "STAGE_ENQUEUED",
                "turn_id": i,
                "idempotency_key": f"enqueue:s{i}",
                "payload": {"stage_id": f"s{i}", "bounded": True, "mutates": False},
            }
        )
    return events


def test_invariant_execution_time_guardrail() -> None:
    start = time.perf_counter()
    report = run_invariants(
        {
            "proofs": [],
            "events_hash": "",
            "computed_events_hash": "",
            "event_order_invalid": False,
            "runtime_state_invalid": False,
        },
        names={
            "ProofChainInvariant",
            "ReplayDeterminismInvariant",
            "EventOrderInvariant",
            "RuntimeStateConsistencyInvariant",
        },
    )
    elapsed = time.perf_counter() - start
    assert report["passed"] is True
    assert elapsed < 0.2


def test_replay_execution_time_guardrail() -> None:
    events = _events()
    start = time.perf_counter()
    state = replay_state(events)
    elapsed = time.perf_counter() - start
    assert state["replayed_event_count"] == len(events)
    assert elapsed < 0.2

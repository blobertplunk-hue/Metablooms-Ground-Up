from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def state_hash(state: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(state).encode("utf-8")).hexdigest()


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

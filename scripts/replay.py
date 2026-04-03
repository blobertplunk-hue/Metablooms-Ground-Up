from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from scripts.verify_proof_chain import verify
from src.invariants import canonical_events_hash
from src.replay_utils import replay_state, state_hash


def _load_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--format", choices=["json", "text"], default="json")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    events = _load_events(root / "events.jsonl")
    state = replay_state(events)
    payload = {
        "events_hash": canonical_events_hash(events),
        "state_hash": state_hash(state),
        "event_count": len(events),
    }
    if args.verify:
        payload["verification"] = verify(root)
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            f"events={payload['event_count']} events_hash={payload['events_hash']} state_hash={payload['state_hash']}"
        )
    if args.verify and payload["verification"]["result"] != "PASS":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

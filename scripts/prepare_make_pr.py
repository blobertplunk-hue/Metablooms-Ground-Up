from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.implementation_reality_gate import ImplementationRealityError, run_gate


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--base", default="HEAD~1")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--claim", required=True)
    parser.add_argument(
        "--output",
        default="implementation_reality_artifacts/pr_payload.json",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    claim_path = (root / args.claim).resolve()
    claim = json.loads(claim_path.read_text(encoding="utf-8"))
    try:
        gate_payload = run_gate(root, args.base, args.head, claim_path, root / args.output)
    except ImplementationRealityError as exc:
        raise SystemExit(f"blocked_make_pr: {exc}") from exc

    summary = {
        "title": claim.get("title", ""),
        "body": claim.get("summary", ""),
        "classification": gate_payload["classification"],
        "claim_assessment": gate_payload["claim_assessment"],
        "tests_run": claim.get("tests_run", []),
        "result": "READY",
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

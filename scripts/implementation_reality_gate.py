from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.invariants import InvariantContractError, run_invariants
from src.review.claim_consistency import assess_claim_consistency
from src.review.diff_classifier import classify_diff


class ImplementationRealityError(RuntimeError):
    pass


FEATURE_TASK_TYPES = frozenset({"feature", "enforcement", "runtime", "verifier"})


def _load_claim(path: Path | None) -> dict:
    if path is None:
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def run_gate(
    root: Path,
    base: str,
    head: str,
    claim_path: Path | None,
    output_path: Path,
) -> dict:
    classification = classify_diff(root=root, base=base, head=head)
    claim = _load_claim(claim_path)

    assessment = assess_claim_consistency(
        classification,
        title=str(claim.get("title", "")),
        summary=str(claim.get("summary", "")),
        expected_changed_surfaces=[str(x) for x in claim.get("expected_changed_surfaces", [])],
        claimed_capability=str(claim.get("claimed_capability", "")),
    )

    task_type = str(claim.get("task_type", "maintenance"))
    if not claim and classification.no_op:
        raise ImplementationRealityError("no-op diff; make_pr must be blocked")

    invariant_context = {
        "implementation_reality": {
            "task_type": task_type,
            "classification": classification.to_dict(),
            "claim": claim,
            "claim_assessment": {
                "passed": assessment.passed,
                "failures": assessment.failures,
                "metadata": assessment.metadata,
            },
        }
    }

    try:
        report = run_invariants(
            invariant_context,
            names={"ImplementationRealityInvariant", "ClaimConsistencyInvariant"},
        )
    except InvariantContractError as exc:
        raise ImplementationRealityError(f"invariant contract failure: {exc}") from exc

    if task_type in FEATURE_TASK_TYPES and classification.no_op:
        raise ImplementationRealityError("feature task cannot run on no-op diff")

    payload = {
        "base": base,
        "head": head,
        "classification": classification.to_dict(),
        "classification_hash": classification.stable_hash,
        "claim": claim,
        "claim_assessment": {
            "passed": assessment.passed,
            "failures": assessment.failures,
            "metadata": assessment.metadata,
        },
        "invariant_report": {
            "passed": report["passed"],
            "failures": report["failures"],
            "checked": report["invariants_checked"],
        },
        "result": "PASS" if report["passed"] else "FAIL",
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if not report["passed"]:
        raise ImplementationRealityError(
            "implementation reality gate failure: " + str(report["failures"][0]["name"])
        )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--base", default="HEAD~1")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--claim", default="")
    parser.add_argument(
        "--output",
        default="implementation_reality_artifacts/latest.json",
    )
    args = parser.parse_args()
    root = Path(args.root).resolve()
    claim_path = Path(args.claim).resolve() if args.claim else None
    run_gate(root, args.base, args.head, claim_path, root / args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

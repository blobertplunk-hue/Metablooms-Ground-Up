from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from src.invariants import (
    INVARIANT_REGISTRY_VERSION,
    InvariantContractError,
    canonical_events_hash,
    run_invariants,
)
from src.mpp.mpp_canonical import MPPCanonicalError, canonical_mpp_hash
from src.bts.bts_canonical import BTSCanonicalError, canonical_bts_hash
from src.replay_utils import replay_state, state_hash
from src.optimality.optimality_model import OptimalityError, canonical_optimality_hash
from src.validation_layer import (
    load_registry_entries_resilient,
    proof_registry_snapshot_hash,
)

logger = logging.getLogger("mpp.verify")

TCB_MODULES = frozenset(
    {
        "src.invariants",
        "src.replay_utils",
        "scripts.verify_proof_chain",
        "src.validation_layer",
    }
)


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _is_version_compatible(version: str) -> bool:
    if not version:
        return True
    major = version.split(".", maxsplit=1)[0]
    expected_major = INVARIANT_REGISTRY_VERSION.split(".", maxsplit=1)[0]
    return major == expected_major


def _migrate_registry_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    migrated: list[dict[str, Any]] = []
    for entry in entries:
        item = dict(entry)
        if "invariant_registry_version" not in item:
            item["invariant_registry_version"] = "0.9.0"
        migrated.append(item)
    return migrated


def _load_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _load_manifest(root: Path) -> dict[str, Any] | None:
    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        return None
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _validate_release_signature(root: Path, manifest_hash: str) -> tuple[bool, str]:
    path = root / "release_signature.json"
    if not path.exists():
        return (True, "")
    payload = json.loads(path.read_text(encoding="utf-8"))
    bundle_hash = payload.get("bundle_hash", "")
    expected_manifest_hash = payload.get("manifest_hash", "")
    signature = payload.get("signature", "")
    algorithm = payload.get("algorithm", "")
    if expected_manifest_hash != manifest_hash:
        return (False, "manifest hash mismatch in signature")
    key = os.environ.get("RELEASE_SIGNING_KEY", "")
    if algorithm == "hmac-sha256":
        if not key:
            return (False, "missing RELEASE_SIGNING_KEY for signature validation")
        expected_sig = hmac.new(
            key.encode("utf-8"), bundle_hash.encode("utf-8"), hashlib.sha256
        ).hexdigest()
    else:
        expected_sig = hashlib.sha256(bundle_hash.encode("utf-8")).hexdigest()
    if signature != expected_sig:
        return (False, "release signature mismatch")
    return (True, "")


def _validate_manifest(root: Path, manifest: dict[str, Any] | None) -> tuple[str, str]:
    if manifest is None:
        return ("", "")
    files = manifest.get("files", [])
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode("utf-8")
    expected = manifest.get("manifest_hash", "")
    actual = _sha256_bytes(canonical)
    for item in files:
        rel = item.get("path")
        digest = item.get("sha256")
        if not isinstance(rel, str) or not isinstance(digest, str):
            return (expected, "invalid")
        target = root / rel
        if not target.exists():
            return (expected, "missing")
        if _sha256_bytes(target.read_bytes()) != digest:
            return (expected, "mismatch")
    return (expected, actual)


def verify(root: Path) -> dict[str, Any]:
    logger.info(json.dumps({"action": "verify_start", "root": str(root)}))
    events = _load_events(root / "events.jsonl")
    proof_path = root / "PROOF_REGISTRY.jsonl"
    if not proof_path.exists():
        alt = root / "proof_registry.jsonl"
        if alt.exists():
            proof_path = alt
    proofs = _migrate_registry_entries(load_registry_entries_resilient(proof_path))
    replayed = replay_state(events)
    computed_events_hash = canonical_events_hash(events)
    replayed_state_hash = state_hash(replayed)
    proof_state_hash = proofs[-1].get("state_hash_after", "") if proofs else ""
    snapshot_hash = proof_registry_snapshot_hash(proofs)
    snapshot_path = root / "proof_registry_snapshot.sha256"
    expected_snapshot_hash = (
        snapshot_path.read_text(encoding="utf-8").strip()
        if snapshot_path.exists()
        else snapshot_hash
    )
    entry_version = proofs[-1].get("invariant_registry_version", "") if proofs else ""
    expected_manifest_hash, actual_manifest_hash = _validate_manifest(
        root, _load_manifest(root)
    )
    mpp_task_id = proofs[-1].get("task_id", "") if proofs else ""
    proof_mpp_hash = proofs[-1].get("mpp_hash", "") if proofs else ""
    if not proof_mpp_hash:
        mpp_task_id = ""
    try:
        computed_mpp_hash = canonical_mpp_hash(root, mpp_task_id) if mpp_task_id else ""
        mpp_artifact_error = False
    except MPPCanonicalError:
        computed_mpp_hash = ""
        mpp_artifact_error = True
    bts_task_id = proofs[-1].get("task_id", "") if proofs else ""
    proof_bts_hash = proofs[-1].get("bts_hash", "") if proofs else ""
    if not proof_bts_hash:
        bts_task_id = ""
    try:
        computed_bts_hash = canonical_bts_hash(root, bts_task_id) if bts_task_id else ""
        bts_artifact_error = False
    except BTSCanonicalError:
        computed_bts_hash = ""
        bts_artifact_error = True
    optimality_task_id = proofs[-1].get("task_id", "") if proofs else ""
    proof_optimality_hash = proofs[-1].get("optimality_hash", "") if proofs else ""
    if not proof_optimality_hash:
        optimality_task_id = ""
    try:
        computed_optimality_hash = (
            canonical_optimality_hash(root, optimality_task_id)
            if optimality_task_id
            else ""
        )
        optimality_artifact_error = False
    except OptimalityError:
        computed_optimality_hash = ""
        optimality_artifact_error = True
    sig_ok, sig_error = _validate_release_signature(root, actual_manifest_hash)
    implementation_reality_path = (
        root / "implementation_reality_artifacts" / "latest.json"
    )
    implementation_reality: dict[str, Any] = {}
    if implementation_reality_path.exists():
        implementation_reality = json.loads(
            implementation_reality_path.read_text(encoding="utf-8")
        )
    if not sig_ok:
        return {
            "result": "FAIL",
            "error": sig_error,
            "events_hash": computed_events_hash,
            "state_hash": replayed_state_hash,
            "proof_registry_snapshot_hash": snapshot_hash,
            "invariant_registry_version": INVARIANT_REGISTRY_VERSION,
            "invariant_report": None,
        }
    if entry_version and not _is_version_compatible(entry_version):
        return {
            "result": "FAIL",
            "error": "invariant registry version incompatible",
            "events_hash": computed_events_hash,
            "state_hash": replayed_state_hash,
            "proof_registry_snapshot_hash": snapshot_hash,
            "invariant_registry_version": INVARIANT_REGISTRY_VERSION,
            "invariant_report": None,
        }
    try:
        report = run_invariants(
            {
                "proofs": proofs,
                "events_hash": computed_events_hash,
                "computed_events_hash": computed_events_hash,
                "event_order_invalid": False,
                "runtime_state_invalid": False,
                "proof_state_hash": proof_state_hash,
                "replayed_state_hash": replayed_state_hash,
                "expected_proof_snapshot_hash": expected_snapshot_hash,
                "actual_proof_snapshot_hash": snapshot_hash,
                "invariant_registry_version": entry_version,
                "expected_manifest_hash": expected_manifest_hash,
                "actual_manifest_hash": actual_manifest_hash,
                "mpp_task_id": mpp_task_id,
                "proof_mpp_hash": proof_mpp_hash,
                "computed_mpp_hash": computed_mpp_hash,
                "mpp_artifact_error": mpp_artifact_error,
                "bts_task_id": bts_task_id,
                "proof_bts_hash": proof_bts_hash,
                "computed_bts_hash": computed_bts_hash,
                "bts_artifact_error": bts_artifact_error,
                "optimality_task_id": optimality_task_id,
                "proof_optimality_hash": proof_optimality_hash,
                "computed_optimality_hash": computed_optimality_hash,
                "optimality_artifact_error": optimality_artifact_error,
                "root_path": str(root),
                "implementation_reality": {
                    "task_type": implementation_reality.get("claim", {}).get(
                        "task_type", "maintenance"
                    ),
                    "classification": implementation_reality.get("classification", {}),
                    "claim": implementation_reality.get("claim", {}),
                    "claim_assessment": implementation_reality.get(
                        "claim_assessment", {}
                    ),
                },
            },
            names={
                "ProofChainInvariant",
                "ReplayDeterminismInvariant",
                "RuntimeStateConsistencyInvariant",
                "StateHashBindingInvariant",
                "ProofRegistrySnapshotInvariant",
                "InvariantRegistryVersionInvariant",
                "ExportManifestInvariant",
                "MPPHashInvariant",
                "BTSIntegrityInvariant",
                "BTSCompletenessInvariant",
                "BTSJustificationInvariant",
                "DecisionConsistencyInvariant",
                "OptimalityInvariant",
                "TradeoffQuantificationInvariant",
                "OptionSalienceInvariant",
                "DecisionImprovementInvariant",
                "ImplementationRealityInvariant",
                "ClaimConsistencyInvariant",
            },
        )
    except InvariantContractError as exc:
        return {
            "result": "FAIL",
            "error": f"invariant contract error: {exc}",
            "events_hash": computed_events_hash,
            "state_hash": replayed_state_hash,
            "proof_registry_snapshot_hash": snapshot_hash,
            "invariant_registry_version": INVARIANT_REGISTRY_VERSION,
            "invariant_report": None,
        }
    result = {
        "result": "PASS" if report["passed"] else "FAIL",
        "events_hash": computed_events_hash,
        "state_hash": replayed_state_hash,
        "proof_registry_snapshot_hash": snapshot_hash,
        "expected_proof_snapshot_hash": expected_snapshot_hash,
        "proof_entries": len(proofs),
        "invariant_registry_version": INVARIANT_REGISTRY_VERSION,
        "invariant_report": {
            "invariants_checked": report["invariants_checked"],
            "failures": report["failures"],
            "passed": report["passed"],
        },
    }
    if not report["passed"]:
        logger.error(
            json.dumps(
                {
                    "action": "verify_fail",
                    "failures": result["invariant_report"]["failures"],
                }
            )
        )
    else:
        logger.info(json.dumps({"action": "verify_pass", "proof_entries": len(proofs)}))
    with (root / "audit_log.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(
            json.dumps(
                {
                    "timestamp": time.time(),
                    "action": "verify",
                    "result": result["result"],
                    "failures": result["invariant_report"]["failures"],
                },
                sort_keys=True,
            )
            + "\n"
        )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--report", default=None)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--max-iterations", type=int, default=0)
    parser.add_argument("--format", choices=["json", "text"], default="json")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.watch:
        proof_path = root / "PROOF_REGISTRY.jsonl"
        last_size = -1
        iterations = 0
        while True:
            size = proof_path.stat().st_size if proof_path.exists() else 0
            if size != last_size:
                result = verify(root)
                if args.format == "json":
                    print(json.dumps(result, sort_keys=True))
                else:
                    print(
                        f"[verify] result={result['result']} proofs={result.get('proof_entries', 0)}"
                    )
                if result["result"] != "PASS":
                    return 1
                last_size = size
            iterations += 1
            if args.max_iterations and iterations >= args.max_iterations:
                return 0
            time.sleep(args.interval)
    result = verify(root)
    payload = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.report:
        Path(args.report).write_text(payload, encoding="utf-8")
    else:
        if args.format == "json":
            print(payload, end="")
        else:
            print(
                f"verify result: {result['result']} | events_hash={result.get('events_hash','')} | state_hash={result.get('state_hash','')}"
            )
    return 0 if result["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

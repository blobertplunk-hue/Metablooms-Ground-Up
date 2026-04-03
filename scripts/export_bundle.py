from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import shutil
from pathlib import Path

from scripts.verify_proof_chain import verify
from src.validation_layer import (
    load_registry_entries_resilient,
    proof_registry_snapshot_hash,
)


def export_bundle(root: Path, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    for name in [
        "events.jsonl",
        "PROOF_REGISTRY.jsonl",
        "runtime_state.json",
        "INVARIANT_COVERAGE_MAP.md",
        "VERIFY_INSTRUCTIONS.md",
        "docs/FORMAL_GUARANTEES.md",
        "audit_log.jsonl",
        "VERSION",
        "CHANGELOG.md",
        "MPP_STAGE_PIPELINE.md",
        "MPP_STAGE_POLICY_SCHEMA.json",
        "MPP_STAGE_ARTIFACT_SCHEMA.json",
        "BTS_TRACE_SCHEMA.json",
        "OPTIMALITY_TRACE_SCHEMA.json",
        "IMPLEMENTATION_REALITY_SCHEMA.json",
    ]:
        src = root / name
        if src.exists():
            target_name = (
                "proof_registry.jsonl" if name == "PROOF_REGISTRY.jsonl" else name
            )
            target = output / target_name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, target)
    mpp_dir = root / "mpp_artifacts"
    if mpp_dir.exists():
        for src in sorted(mpp_dir.rglob("*.json")):
            rel = src.relative_to(root)
            target = output / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, target)
    bts_dir = root / "bts_artifacts"
    if bts_dir.exists():
        for src in sorted(bts_dir.rglob("*.json")):
            rel = src.relative_to(root)
            target = output / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, target)
    optimality_dir = root / "optimality_artifacts"
    if optimality_dir.exists():
        for src in sorted(optimality_dir.rglob("*.json")):
            rel = src.relative_to(root)
            target = output / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, target)
    implementation_reality_dir = root / "implementation_reality_artifacts"
    if implementation_reality_dir.exists():
        for src in sorted(implementation_reality_dir.rglob("*.json")):
            rel = src.relative_to(root)
            target = output / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, target)
    report = verify(root)
    snapshot_hash = proof_registry_snapshot_hash(
        load_registry_entries_resilient(root / "PROOF_REGISTRY.jsonl")
    )
    (output / "invariant_results.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output / "proof_registry_snapshot.sha256").write_text(
        snapshot_hash + "\n", encoding="utf-8"
    )
    manifest_files = []
    for p in sorted(output.rglob("*")):
        if p.is_file() and p.name not in {"manifest.json", "audit_log.jsonl"}:
            rel = str(p.relative_to(output))
            digest = hashlib.sha256(p.read_bytes()).hexdigest()
            manifest_files.append({"path": rel, "sha256": digest})
    manifest_hash = hashlib.sha256(
        json.dumps(manifest_files, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    manifest = {
        "files": manifest_files,
        "manifest_hash": manifest_hash,
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    bundle_hash = hashlib.sha256(
        json.dumps(
            {
                "manifest_hash": manifest_hash,
                "files": [item["sha256"] for item in manifest_files],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    key = os.environ.get("RELEASE_SIGNING_KEY", "")
    if key:
        signature = hmac.new(
            key.encode("utf-8"), bundle_hash.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        mode = "hmac-sha256"
    else:
        signature = hashlib.sha256(bundle_hash.encode("utf-8")).hexdigest()
        mode = "sha256-self"
    (output / "release_signature.json").write_text(
        json.dumps(
            {
                "manifest_hash": manifest_hash,
                "bundle_hash": bundle_hash,
                "signature": signature,
                "algorithm": mode,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--output", default="export")
    parser.add_argument("--format", choices=["json", "text"], default="json")
    args = parser.parse_args()
    out = Path(args.output).resolve()
    export_bundle(Path(args.root).resolve(), out)
    summary = {"result": "PASS", "output": str(out)}
    if args.format == "json":
        print(json.dumps(summary, sort_keys=True))
    else:
        print(f"export complete: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

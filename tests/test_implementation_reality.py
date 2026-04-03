from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from scripts.implementation_reality_gate import ImplementationRealityError, run_gate
from scripts.verify_proof_chain import TCB_MODULES
from src.invariants import run_invariants
from src.review.diff_classifier import classify_diff


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout


def _init_repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init")
    _git(root, "config", "user.email", "test@example.com")
    _git(root, "config", "user.name", "test")
    (root / "src").mkdir()
    (root / "tests").mkdir()
    (root / "docs").mkdir()
    (root / "src" / "sample.py").write_text("def f(x:int)->int:\n    return x+1\n")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "init")
    return root


def _write_claim(root: Path, **overrides: object) -> Path:
    claim = {
        "title": "Implement sample feature behavior",
        "summary": "Implement sample feature behavior in code and tests",
        "task_type": "feature",
        "claimed_capability": "sample feature",
        "expected_changed_surfaces": ["src/sample.py"],
    }
    claim.update(overrides)
    path = root / "IMPLEMENTATION_CLAIM.json"
    path.write_text(json.dumps(claim, sort_keys=True), encoding="utf-8")
    return path


def test_formatting_only_feature_claim_fails(tmp_path: Path) -> None:
    root = _init_repo(tmp_path)
    (root / "src" / "sample.py").write_text("def f(x: int) -> int:\n    return x + 1\n")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "format")
    claim = _write_claim(root)
    with pytest.raises(ImplementationRealityError):
        run_gate(root, "HEAD~1", "HEAD", claim, root / "out.json")


def test_docs_only_feature_claim_fails(tmp_path: Path) -> None:
    root = _init_repo(tmp_path)
    (root / "docs" / "note.md").write_text("# docs\n")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "docs")
    claim = _write_claim(root)
    with pytest.raises(ImplementationRealityError):
        run_gate(root, "HEAD~1", "HEAD", claim, root / "out.json")


def test_tests_only_feature_claim_fails(tmp_path: Path) -> None:
    root = _init_repo(tmp_path)
    (root / "tests" / "test_sample.py").write_text("def test_a():\n    assert True\n")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "tests")
    claim = _write_claim(root)
    with pytest.raises(ImplementationRealityError):
        run_gate(root, "HEAD~1", "HEAD", claim, root / "out.json")


def test_semantic_code_with_accurate_claim_passes(tmp_path: Path) -> None:
    root = _init_repo(tmp_path)
    (root / "src" / "sample.py").write_text("def f(x:int)->int:\n    return x+2\n")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "semantic")
    claim = _write_claim(root)
    payload = run_gate(root, "HEAD~1", "HEAD", claim, root / "out.json")
    assert payload["result"] == "PASS"


def test_overstated_verifier_claim_fails(tmp_path: Path) -> None:
    root = _init_repo(tmp_path)
    (root / "src" / "sample.py").write_text("def f(x:int)->int:\n    return x+3\n")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "semantic")
    claim = _write_claim(root, title="Implement verifier enforcement hardening")
    with pytest.raises(ImplementationRealityError):
        run_gate(root, "HEAD~1", "HEAD", claim, root / "out.json")


def test_noop_diff_blocks_make_pr_path(tmp_path: Path) -> None:
    root = _init_repo(tmp_path)
    claim = _write_claim(root)
    with pytest.raises(ImplementationRealityError, match="no-op"):
        run_gate(root, "HEAD", "HEAD", claim, root / "out.json")


def test_expected_surfaces_mismatch_fails(tmp_path: Path) -> None:
    root = _init_repo(tmp_path)
    (root / "src" / "sample.py").write_text("def f(x:int)->int:\n    return x+5\n")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "semantic")
    claim = _write_claim(root, expected_changed_surfaces=["src/not_touched.py"])
    with pytest.raises(ImplementationRealityError):
        run_gate(root, "HEAD~1", "HEAD", claim, root / "out.json")


def test_diff_classification_is_deterministic(tmp_path: Path) -> None:
    root = _init_repo(tmp_path)
    (root / "src" / "sample.py").write_text("def f(x:int)->int:\n    return x+7\n")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "semantic")
    first = classify_diff(root, "HEAD~1", "HEAD")
    second = classify_diff(root, "HEAD~1", "HEAD")
    assert first.to_dict() == second.to_dict()
    assert first.stable_hash == second.stable_hash


def test_verifier_independence_tcb_remains_isolated() -> None:
    assert "scripts.implementation_reality_gate" not in TCB_MODULES


def test_existing_invariant_surface_still_passes_for_non_claim_context() -> None:
    report = run_invariants(
        {
            "implementation_reality": {
                "task_type": "maintenance",
                "classification": {
                    "changed_files": ["docs/note.md"],
                    "formatting_only": False,
                    "docs_only": True,
                    "tests_only": False,
                    "semantic_code": False,
                    "schema_or_contract": False,
                    "runtime_enforcement": False,
                    "verifier_enforcement": False,
                    "export_audit_surface": False,
                    "no_op": False,
                },
                "claim_assessment": {"passed": True, "failures": [], "metadata": {}},
            }
        },
        names={"ImplementationRealityInvariant", "ClaimConsistencyInvariant"},
    )
    assert report["passed"]

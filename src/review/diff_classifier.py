from __future__ import annotations

import ast
import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


FEATURE_SURFACE_FILES = frozenset(
    {
        "src/turn_execution_engine.py",
        "src/recovery_lock_engine.py",
        "src/validation_layer.py",
        "src/invariants.py",
        "scripts/verify_proof_chain.py",
        "scripts/export_bundle.py",
        "scripts/mpp_guard.py",
    }
)


@dataclass(frozen=True)
class DiffClassification:
    changed_files: list[str]
    formatting_only: bool
    docs_only: bool
    tests_only: bool
    semantic_code: bool
    schema_or_contract: bool
    runtime_enforcement: bool
    verifier_enforcement: bool
    export_audit_surface: bool
    no_op: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "changed_files": self.changed_files,
            "formatting_only": self.formatting_only,
            "docs_only": self.docs_only,
            "tests_only": self.tests_only,
            "semantic_code": self.semantic_code,
            "schema_or_contract": self.schema_or_contract,
            "runtime_enforcement": self.runtime_enforcement,
            "verifier_enforcement": self.verifier_enforcement,
            "export_audit_surface": self.export_audit_surface,
            "no_op": self.no_op,
        }

    @property
    def stable_hash(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()


def _git(*args: str, root: Path) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _changed_files(root: Path, base: str, head: str) -> list[str]:
    output = _git("diff", "--name-only", f"{base}..{head}", root=root)
    return sorted([line.strip() for line in output.splitlines() if line.strip()])


def _is_doc(path: str) -> bool:
    p = Path(path)
    return p.suffix.lower() in {".md", ".rst", ".txt"} or p.parts[:1] == ("docs",)


def _is_test(path: str) -> bool:
    p = Path(path)
    return p.parts[:1] == ("tests",) or p.name.startswith("test_")


def _is_schema_or_contract(path: str) -> bool:
    p = Path(path)
    if p.suffix.lower() == ".json" and (
        p.name.endswith("_SCHEMA.json")
        or p.name in {"CURRENT_ROOT.json", "IMMUTABLE_CONFIG.json"}
    ):
        return True
    if p.name in {
        "MASTER_WORKFLOW_V2.md",
        "EVENT_SCHEMA.json",
        "REPLAY_RULES.md",
        "EXECUTION_GATE_SPEC.md",
        "ACCEPTANCE_TESTS.md",
        "PROOF_REGISTRY_SCHEMA.json",
        "VERIFY_INSTRUCTIONS.md",
    }:
        return True
    return False


def _blob(root: Path, rev: str, path: str) -> str:
    try:
        return _git("show", f"{rev}:{path}", root=root)
    except subprocess.CalledProcessError:
        return ""


def _python_semantic_equal(before: str, after: str) -> bool:
    try:
        before_ast = ast.dump(ast.parse(before), include_attributes=False)
        after_ast = ast.dump(ast.parse(after), include_attributes=False)
    except SyntaxError:
        return False
    return before_ast == after_ast


def _file_is_formatting_only(root: Path, base: str, head: str, path: str) -> bool:
    if Path(path).suffix != ".py":
        return False
    return _python_semantic_equal(_blob(root, base, path), _blob(root, head, path))


def classify_diff(root: Path, base: str, head: str) -> DiffClassification:
    files = _changed_files(root, base, head)
    if not files:
        return DiffClassification([], False, False, False, False, False, False, False, False, True)

    docs_only = all(_is_doc(p) for p in files)
    tests_only = all(_is_test(p) for p in files)
    schema_or_contract = any(_is_schema_or_contract(p) for p in files)

    runtime_enforcement = any(
        p
        in {
            "src/turn_execution_engine.py",
            "src/recovery_lock_engine.py",
            "src/validation_layer.py",
            "scripts/mpp_guard.py",
        }
        for p in files
    )
    verifier_enforcement = any(
        p in {"scripts/verify_proof_chain.py", "src/invariants.py"} for p in files
    )
    export_audit_surface = any(
        p in {"scripts/export_bundle.py", "VERIFY_INSTRUCTIONS.md", "docs/FORMAL_GUARANTEES.md"}
        for p in files
    )

    py_files = [p for p in files if Path(p).suffix == ".py"]
    formatting_only = bool(py_files) and all(
        _file_is_formatting_only(root, base, head, p) for p in py_files
    ) and all(Path(p).suffix == ".py" for p in files)

    semantic_code = any(
        (
            Path(p).suffix == ".py"
            and not _is_test(p)
            and not _file_is_formatting_only(root, base, head, p)
        )
        for p in files
    )

    return DiffClassification(
        changed_files=files,
        formatting_only=formatting_only,
        docs_only=docs_only,
        tests_only=tests_only,
        semantic_code=semantic_code,
        schema_or_contract=schema_or_contract,
        runtime_enforcement=runtime_enforcement,
        verifier_enforcement=verifier_enforcement,
        export_audit_surface=export_audit_surface,
        no_op=False,
    )

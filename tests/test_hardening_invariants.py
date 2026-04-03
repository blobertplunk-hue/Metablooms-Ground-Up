import json
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.invariants import run_invariants
from src.turn_execution_engine import (
    EngineError,
    EnginePaths,
    _compute_replay_hash,
    _execute_once_internal,
    _validate_event_order_integrity,
    _validate_output_consistency,
    canonical_events_hash,
    execute_once,
    execute_with_recovery,
)
from src.validation_layer import (
    ValidationError,
    append_registry_atomic,
    load_registry_entries_resilient,
)


BASE_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["event_id", "type", "ts", "turn_id", "idempotency_key", "payload"],
    "properties": {
        "event_id": {"type": "string"},
        "type": {"type": "string", "enum": ["STAGE_ENQUEUED", "STAGE_EXECUTED"]},
        "ts": {"type": "string"},
        "turn_id": {"type": "integer"},
        "idempotency_key": {"type": "string"},
        "state_hash_before": {"type": "string"},
        "state_hash_after": {"type": "string"},
        "payload": {
            "type": "object",
            "required": ["stage_id", "bounded"],
            "properties": {
                "stage_id": {"type": "string"},
                "bounded": {"type": "boolean"},
                "mutates": {"type": "boolean"},
            },
        },
    },
}


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_controls(root: Path) -> None:
    _write(root / "CURRENT_ROOT.json", {"canonical_root": str(root.resolve())})
    _write(root / "EVENT_SCHEMA.json", BASE_SCHEMA)
    (root / "AGENTS.md").write_text(
        "runtime_state.json\nevents.jsonl\n", encoding="utf-8"
    )
    (root / "MASTER_WORKFLOW_V2.md").write_text(
        "Load all control files\nReplay and validate events\nEnforce execution gates\n",
        encoding="utf-8",
    )
    (root / "REPLAY_RULES.md").write_text(
        "Replay must be deterministic\nstate_hash_before\nstate_hash_after\n",
        encoding="utf-8",
    )
    (root / "EXECUTION_GATE_SPEC.md").write_text(
        "schema_valid\nreplay_valid\nidempotency_valid\ncompensation_valid\nbounded_stage_available\n",
        encoding="utf-8",
    )
    (root / "ACCEPTANCE_TESTS.md").write_text(
        "Replay from `events.jsonl`\nExactly one bounded stage executed\nExecution is blocked\n",
        encoding="utf-8",
    )
    (root / "CDR_SPEC.md").write_text(
        "CDR-RAT\nCDR-ARCH\nCDR-CODE\nCDR-LINT\nTRADEOFFS\nFUTURE_GAPS\n",
        encoding="utf-8",
    )
    (root / "CDR_SECURITY.md").write_text(
        "INPUT_VALIDATION_REQUIRED\nOUTPUT_ENCODING_REQUIRED\nACCESS_CONTROL_REQUIRED\nSECRETS_HANDLING_REQUIRED\nSAFE_ERROR_HANDLING_REQUIRED\nSAFE_FILE_HANDLING_REQUIRED\n",
        encoding="utf-8",
    )
    (root / "CDR_VERIFICATION.md").write_text(
        "UNIT_TEST_REQUIRED\nINTEGRATION_TEST_REQUIRED\nACCEPTANCE_TEST_REQUIRED\nEXTERNAL_VALIDATION_REQUIRED\n",
        encoding="utf-8",
    )
    (root / "CDR_OBSERVABILITY.md").write_text(
        "STRUCTURED_LOGGING_REQUIRED\nSENSITIVE_DATA_REDACTION_REQUIRED\nNO_SECRET_LOGGING\nTRACEABILITY_REQUIRED\n",
        encoding="utf-8",
    )
    (root / "CDR_LIFECYCLE.md").write_text(
        "RATIONALE_MUST_UPDATE_WITH_CODE\nTRADEOFFS_MUST_BE_DOCUMENTED\nFUTURE_GAPS_REQUIRED\nVERSIONED_CHANGE_REQUIRED\n",
        encoding="utf-8",
    )
    (root / "MASTER_MPP_SCHEMA_v1.md").write_text(
        "Stage 11 = VALIDATION_ENGINE\nStage 12 = TRACE_VALIDATION\nStage 13 = COUNTERFACTUAL_TESTING\n",
        encoding="utf-8",
    )
    (root / "VALIDATION_LAYER_SPEC.md").write_text(
        "Stage 11 VALIDATION_ENGINE\nStage 12 TRACE_VALIDATION\nStage 13 COUNTERFACTUAL_TESTING\n",
        encoding="utf-8",
    )
    (root / "MPP_STAGE_PIPELINE.md").write_text(
        "Stages 1-10 enforced\n", encoding="utf-8"
    )
    (root / "MPP_STAGE_POLICY_SCHEMA.json").write_text(
        json.dumps(
            {
                "type": "object",
                "required": [
                    "task_id",
                    "requires_see",
                    "requires_multi_option",
                    "requires_refinement",
                ],
            }
        ),
        encoding="utf-8",
    )
    (root / "MPP_STAGE_ARTIFACT_SCHEMA.json").write_text(
        json.dumps({"type": "object", "required": ["stage", "task_id", "content"]}),
        encoding="utf-8",
    )
    (root / "INVARIANT_COVERAGE_MAP.md").write_text(
        "# Invariant Coverage Map\n\n| Critical property | Enforcing invariant |\n|---|---|\n"
        "| Mutation proof | `MutationProofInvariant` |\n"
        "| Trace consistency | `TraceConsistencyInvariant` |\n"
        "| Counterfactual distinguishability | `CounterfactualInvariant` |\n"
        "| Proof chain integrity | `ProofChainInvariant` |\n"
        "| Events hash determinism | `ReplayDeterminismInvariant` |\n"
        "| Event order integrity | `EventOrderInvariant` |\n"
        "| Runtime state consistency | `RuntimeStateConsistencyInvariant` |\n"
        "| State hash binding | `StateHashBindingInvariant` |\n"
        "| Proof snapshot consistency | `ProofRegistrySnapshotInvariant` |\n"
        "| Invariant registry version compatibility | `InvariantRegistryVersionInvariant` |\n"
        "| Export manifest integrity | `ExportManifestInvariant` |\n"
        "| MPP compliance | `MPPComplianceInvariant` |\n"
        "| Reasoning integrity | `MPPHashInvariant` |\n"
        "| BTS provenance integrity | `BTSIntegrityInvariant` |\n"
        "| BTS completeness | `BTSCompletenessInvariant` |\n"
        "| BTS justification | `BTSJustificationInvariant` |\n"
        "| Decision consistency | `DecisionConsistencyInvariant` |\n"
        "| Decision optimality | `OptimalityInvariant` |\n"
        "| Tradeoff quantification | `TradeoffQuantificationInvariant` |\n"
        "| Option salience | `OptionSalienceInvariant` |\n"
        "| Decision improvement | `DecisionImprovementInvariant` |\n| Implementation reality | `ImplementationRealityInvariant` |\n| Claim consistency | `ClaimConsistencyInvariant` |\n",
        encoding="utf-8",
    )
    coverage_hash = hashlib.sha256(
        (root / "INVARIANT_COVERAGE_MAP.md").read_text(encoding="utf-8").encode("utf-8")
    ).hexdigest()
    (root / "IMMUTABLE_CONFIG.json").write_text(
        json.dumps(
            {
                "invariant_registry_version": "1.0.0",
                "non_semantic_event_fields": ["ts"],
                "coverage_map_sha256": coverage_hash,
            }
        ),
        encoding="utf-8",
    )
    repo_root = Path(__file__).resolve().parents[1]
    for name in [
        "VALIDATION_RECEIPT_SCHEMA.json",
        "TRACE_VALIDATION_RECEIPT_SCHEMA.json",
        "COUNTERFACTUAL_TEST_REPORT_SCHEMA.json",
        "PROOF_REGISTRY_SCHEMA.json",
    ]:
        (root / name).write_text(
            (repo_root / name).read_text(encoding="utf-8"), encoding="utf-8"
        )


def _paths(root: Path) -> EnginePaths:
    return EnginePaths(
        root,
        root / "CURRENT_ROOT.json",
        root / "events.jsonl",
        root / "runtime_state.json",
        root / "EVENT_SCHEMA.json",
        root / "receipts",
        root / "MASTER_WORKFLOW_V2.md",
        root / "REPLAY_RULES.md",
        root / "EXECUTION_GATE_SPEC.md",
        root / "ACCEPTANCE_TESTS.md",
        root / "AGENTS.md",
        root / "CDR_SPEC.md",
        root / "CDR_SECURITY.md",
        root / "CDR_VERIFICATION.md",
        root / "CDR_OBSERVABILITY.md",
        root / "CDR_LIFECYCLE.md",
        root / "MASTER_MPP_SCHEMA_v1.md",
        root / "VALIDATION_LAYER_SPEC.md",
        root / "MPP_STAGE_PIPELINE.md",
        root / "MPP_STAGE_POLICY_SCHEMA.json",
        root / "MPP_STAGE_ARTIFACT_SCHEMA.json",
    )


def test_replay_hash_deterministic_and_order_sensitive() -> None:
    e1 = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "t",
        "turn_id": 1,
        "idempotency_key": "k1",
        "payload": {"stage_id": "s1", "bounded": True},
    }
    e2 = {
        "event_id": "e2",
        "type": "STAGE_ENQUEUED",
        "ts": "t",
        "turn_id": 2,
        "idempotency_key": "k2",
        "payload": {"stage_id": "s2", "bounded": True},
    }
    assert _compute_replay_hash([e1, e2]) == _compute_replay_hash([e1, e2])
    assert _compute_replay_hash([e1, e2]) != _compute_replay_hash([e2, e1])


def test_canonical_events_hash_ignores_key_order_and_whitespace() -> None:
    a = [
        {
            "event_id": "e1",
            "turn_id": 1,
            "payload": {"b": 2, "a": 1},
            "type": "STAGE_ENQUEUED",
            "idempotency_key": "k",
            "ts": "x",
        }
    ]
    b = [
        {
            "type": "STAGE_ENQUEUED",
            "idempotency_key": "k",
            "event_id": "e1",
            "turn_id": 1,
            "payload": {"a": 1, "b": 2},
            "ts": "y",
        }
    ]
    assert canonical_events_hash(a) == canonical_events_hash(b)


def test_canonical_events_hash_changes_on_semantic_mutation() -> None:
    a = [
        {
            "event_id": "e1",
            "turn_id": 1,
            "payload": {"a": 1},
            "type": "STAGE_ENQUEUED",
            "idempotency_key": "k",
        }
    ]
    b = [
        {
            "event_id": "e1",
            "turn_id": 1,
            "payload": {"a": 2},
            "type": "STAGE_ENQUEUED",
            "idempotency_key": "k",
        }
    ]
    assert canonical_events_hash(a) != canonical_events_hash(b)


def test_canonical_events_hash_stability_across_repeated_runs() -> None:
    events = [
        {
            "event_id": "e1",
            "turn_id": 1,
            "payload": {"a": 1, "nested": {"x": True}},
            "type": "STAGE_ENQUEUED",
            "idempotency_key": "k1",
            "ts": "ignored-1",
        },
        {
            "event_id": "e2",
            "turn_id": 2,
            "payload": {"a": 2},
            "type": "STAGE_EXECUTED",
            "idempotency_key": "k2",
            "ts": "ignored-2",
        },
    ]
    hashes = {canonical_events_hash(events) for _ in range(10)}
    assert len(hashes) == 1


def test_canonical_events_hash_order_sensitive() -> None:
    events = [
        {
            "event_id": "e1",
            "turn_id": 1,
            "payload": {"a": 1},
            "type": "STAGE_ENQUEUED",
            "idempotency_key": "k1",
        },
        {
            "event_id": "e2",
            "turn_id": 2,
            "payload": {"a": 2},
            "type": "STAGE_ENQUEUED",
            "idempotency_key": "k2",
        },
    ]
    assert canonical_events_hash(events) != canonical_events_hash(
        list(reversed(events))
    )


def test_canonical_events_hash_mutation_sensitive_for_semantic_fields() -> None:
    base = [
        {
            "event_id": "e1",
            "turn_id": 1,
            "payload": {"a": 1},
            "type": "STAGE_ENQUEUED",
            "idempotency_key": "k",
        }
    ]
    mutated = [
        {
            "event_id": "e1",
            "turn_id": 1,
            "payload": {"a": 1},
            "type": "STAGE_ENQUEUED",
            "idempotency_key": "changed",
        }
    ]
    assert canonical_events_hash(base) != canonical_events_hash(mutated)


def test_canonical_events_hash_ignores_explicitly_non_semantic_fields() -> None:
    base = [
        {
            "event_id": "e1",
            "turn_id": 1,
            "payload": {"a": 1},
            "type": "STAGE_ENQUEUED",
            "idempotency_key": "k",
            "ts": "2026-01-01T00:00:00+00:00",
        }
    ]
    changed_ts = [
        {
            "event_id": "e1",
            "turn_id": 1,
            "payload": {"a": 1},
            "type": "STAGE_ENQUEUED",
            "idempotency_key": "k",
            "ts": "2026-02-01T00:00:00+00:00",
        }
    ]
    assert canonical_events_hash(base) == canonical_events_hash(changed_ts)


def test_canonical_events_hash_normalizes_unicode_equivalents() -> None:
    nfc = "é"
    nfd = "e\u0301"
    base = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {"stage_id": nfc, "bounded": True, "mutates": False},
        }
    ]
    variant = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {"stage_id": nfd, "bounded": True, "mutates": False},
        }
    ]
    assert canonical_events_hash(base) == canonical_events_hash(variant)


def test_canonical_events_hash_stable_for_float_representation() -> None:
    a = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {
                "stage_id": "s1",
                "bounded": True,
                "mutates": False,
                "params": {"value": 1.0},
            },
        }
    ]
    b = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {
                "stage_id": "s1",
                "bounded": True,
                "mutates": False,
                "params": {"value": 1.0000000000000002},
            },
        }
    ]
    assert canonical_events_hash(a) != canonical_events_hash(b)


def test_canonical_events_hash_neutralizes_nested_order_tricks() -> None:
    a = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {
                "stage_id": "s1",
                "bounded": True,
                "mutates": False,
                "params": {"z": 1, "a": {"y": 2, "x": 1}},
            },
        }
    ]
    b = [
        {
            "idempotency_key": "enqueue:s1",
            "turn_id": 1,
            "type": "STAGE_ENQUEUED",
            "event_id": "e1",
            "payload": {
                "params": {"a": {"x": 1, "y": 2}, "z": 1},
                "mutates": False,
                "bounded": True,
                "stage_id": "s1",
            },
        }
    ]
    assert canonical_events_hash(a) == canonical_events_hash(b)


def test_proof_registry_chain_tamper_detected(tmp_path: Path) -> None:
    p = tmp_path / "PROOF_REGISTRY.jsonl"
    append_registry_atomic(p, {"trace_id": "t1"})
    append_registry_atomic(p, {"trace_id": "t2"})
    rows = [
        json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x.strip()
    ]
    rows[0]["trace_id"] = "mutated"
    p.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="tampered"):
        append_registry_atomic(p, {"trace_id": "t3"})


def test_proof_chain_continuity_full_traversal() -> None:
    proofs_path = Path("PROOF_REGISTRY.jsonl")
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as td:
        proofs_path = Path(td) / "PROOF_REGISTRY.jsonl"
        append_registry_atomic(proofs_path, {"trace_id": "t1", "events_hash": "eh1"})
        append_registry_atomic(proofs_path, {"trace_id": "t2", "events_hash": "eh2"})
        append_registry_atomic(proofs_path, {"trace_id": "t3", "events_hash": "eh3"})
        proofs = load_registry_entries_resilient(proofs_path)
        report = run_invariants(
            {"proofs": proofs, "events_hash": proofs[-1]["events_hash"]},
            names={"ProofChainInvariant"},
        )
        assert report["passed"]


def test_proof_chain_fails_on_middle_entry_tamper() -> None:
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as td:
        proofs_path = Path(td) / "PROOF_REGISTRY.jsonl"
        append_registry_atomic(proofs_path, {"trace_id": "t1", "events_hash": "eh1"})
        append_registry_atomic(proofs_path, {"trace_id": "t2", "events_hash": "eh2"})
        append_registry_atomic(proofs_path, {"trace_id": "t3", "events_hash": "eh3"})
        proofs = load_registry_entries_resilient(proofs_path)
        proofs[1]["task_id"] = "tampered"
        proofs_path.write_text(
            "\n".join(json.dumps(p, sort_keys=True) for p in proofs) + "\n",
            encoding="utf-8",
        )
        report = run_invariants(
            {
                "proofs": load_registry_entries_resilient(proofs_path),
                "events_hash": "eh3",
            },
            names={"ProofChainInvariant"},
        )
        assert not report["passed"]


def test_proof_chain_fails_on_truncation() -> None:
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as td:
        proofs_path = Path(td) / "PROOF_REGISTRY.jsonl"
        append_registry_atomic(proofs_path, {"trace_id": "t1", "events_hash": "eh1"})
        append_registry_atomic(proofs_path, {"trace_id": "t2", "events_hash": "eh2"})
        append_registry_atomic(proofs_path, {"trace_id": "t3", "events_hash": "eh3"})
        proofs = load_registry_entries_resilient(proofs_path)
        truncated = [proofs[0], proofs[2]]
        proofs_path.write_text(
            "\n".join(json.dumps(p, sort_keys=True) for p in truncated) + "\n",
            encoding="utf-8",
        )
        report = run_invariants(
            {
                "proofs": load_registry_entries_resilient(proofs_path),
                "events_hash": "eh3",
            },
            names={"ProofChainInvariant"},
        )
        assert not report["passed"]


def test_proof_chain_fails_on_reordering() -> None:
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as td:
        proofs_path = Path(td) / "PROOF_REGISTRY.jsonl"
        append_registry_atomic(proofs_path, {"trace_id": "t1", "events_hash": "eh1"})
        append_registry_atomic(proofs_path, {"trace_id": "t2", "events_hash": "eh2"})
        append_registry_atomic(proofs_path, {"trace_id": "t3", "events_hash": "eh3"})
        proofs = load_registry_entries_resilient(proofs_path)
        reordered = [proofs[1], proofs[0], proofs[2]]
        proofs_path.write_text(
            "\n".join(json.dumps(p, sort_keys=True) for p in reordered) + "\n",
            encoding="utf-8",
        )
        report = run_invariants(
            {
                "proofs": load_registry_entries_resilient(proofs_path),
                "events_hash": "eh3",
            },
            names={"ProofChainInvariant"},
        )
        assert not report["passed"]


def test_direct_execute_blocked() -> None:
    with pytest.raises(EngineError, match="internal-only"):
        execute_once(None)  # type: ignore[arg-type]


def test_self_test_fails_on_corrupt_proof_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    events = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:00:00+00:00",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
        }
    ]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )
    _execute_once_internal(_paths(tmp_path))
    append_registry_atomic(tmp_path / "PROOF_REGISTRY.jsonl", {"trace_id": "z1"})
    rows = [
        json.loads(x)
        for x in (tmp_path / "PROOF_REGISTRY.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if x.strip()
    ]
    rows[-1]["prev_hash"] = "bad"
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
    )

    monkeypatch.setattr(
        "src.turn_execution_engine.subprocess.run",
        lambda *a, **k: SimpleNamespace(returncode=0, stdout="ok", stderr=""),
    )
    with pytest.raises(EngineError, match="MPP self-test failed"):
        execute_with_recovery(
            _paths(tmp_path),
            task_id="t",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
            changed_files=["src/turn_execution_engine.py"],
        )


def test_proof_entry_events_hash_binding_enforced(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    events = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:00:00+00:00",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
        }
    ]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )
    _execute_once_internal(_paths(tmp_path))
    rows = [
        json.loads(x)
        for x in (tmp_path / "PROOF_REGISTRY.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if x.strip()
    ]
    rows[-1]["events_hash"] = "bad-hash"
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
    )
    with pytest.raises(
        EngineError, match="binding missing|events_hash mismatch|MPP guard failed"
    ):
        execute_with_recovery(
            _paths(tmp_path),
            task_id="t",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
        )


def test_proof_binding_conflict_fails(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    events = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:00:00+00:00",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
        },
        {
            "event_id": "e2",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:00:30+00:00",
            "turn_id": 2,
            "idempotency_key": "enqueue:s2",
            "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
        },
    ]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )
    _execute_once_internal(_paths(tmp_path))
    lines = [
        line
        for line in (tmp_path / "PROOF_REGISTRY.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    lines.append(lines[-1])
    (tmp_path / "PROOF_REGISTRY.jsonl").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    with pytest.raises(
        EngineError,
        match="duplicate trace_id|binding conflict|MPP self-test failed|MPP guard failed",
    ):
        execute_with_recovery(
            _paths(tmp_path),
            task_id="t",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
        )


def test_output_consistency_keys_by_execution_identity() -> None:
    h1 = hashlib.sha256(
        json.dumps({"v": 1}, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    h2 = hashlib.sha256(
        json.dumps({"v": 2}, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    events = [
        {
            "event_id": "e1",
            "type": "STAGE_EXECUTED",
            "turn_id": 1,
            "idempotency_key": "exec-1",
            "payload": {"stage_id": "s1", "output": {"v": 1}, "output_hash": h1},
        },
        {
            "event_id": "e2",
            "type": "STAGE_EXECUTED",
            "turn_id": 2,
            "idempotency_key": "exec-2",
            "payload": {"stage_id": "s1", "output": {"v": 2}, "output_hash": h2},
        },
    ]
    _validate_output_consistency(events)


def test_output_consistency_same_identity_different_output_fails() -> None:
    h1 = hashlib.sha256(
        json.dumps({"v": 1}, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    h2 = hashlib.sha256(
        json.dumps({"v": 2}, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    events = [
        {
            "event_id": "e1",
            "type": "STAGE_EXECUTED",
            "turn_id": 1,
            "idempotency_key": "exec-1",
            "payload": {"stage_id": "s1", "output": {"v": 1}, "output_hash": h1},
        },
        {
            "event_id": "e2",
            "type": "STAGE_EXECUTED",
            "turn_id": 2,
            "idempotency_key": "exec-1",
            "payload": {"stage_id": "s1", "output": {"v": 2}, "output_hash": h2},
        },
    ]
    with pytest.raises(EngineError, match="output mismatch"):
        _validate_output_consistency(events)


def test_event_order_integrity_reordered_events_fail() -> None:
    events = [
        {"event_id": "b", "turn_id": 2},
        {"event_id": "a", "turn_id": 1},
    ]
    with pytest.raises(EngineError, match="file order drift"):
        _validate_event_order_integrity(events)

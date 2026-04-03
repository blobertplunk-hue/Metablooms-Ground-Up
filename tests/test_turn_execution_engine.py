import json
import hashlib
from pathlib import Path

import pytest

from src import turn_execution_engine as engine
from src.turn_execution_engine import EngineError, EnginePaths, _execute_once_internal


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
                "status": {"type": "string"},
                "params": {"type": "object"},
                "compensation": {"type": "object"},
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
    _write(
        root / "MPP_STAGE_POLICY_SCHEMA.json",
        {
            "type": "object",
            "required": [
                "task_id",
                "requires_see",
                "requires_multi_option",
                "requires_refinement",
            ],
        },
    )
    _write(
        root / "MPP_STAGE_ARTIFACT_SCHEMA.json",
        {"type": "object", "required": ["stage", "task_id", "content"]},
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
    _write(
        root / "IMMUTABLE_CONFIG.json",
        {
            "invariant_registry_version": "1.0.0",
            "non_semantic_event_fields": ["ts"],
            "coverage_map_sha256": coverage_hash,
        },
    )
    (root / "VALIDATION_RECEIPT_SCHEMA.json").write_text(
        '{"required":["trace_id","task_id","stage_id","timestamp","inputs_checked","artifacts_checked","rules_checked","mutation_proof","result","failures","warnings"]}',
        encoding="utf-8",
    )
    (root / "TRACE_VALIDATION_RECEIPT_SCHEMA.json").write_text(
        '{"required":["trace_id","task_id","stage_id","timestamp","execution_proof_ref","result","failures"]}',
        encoding="utf-8",
    )
    (root / "COUNTERFACTUAL_TEST_REPORT_SCHEMA.json").write_text(
        '{"required":["scenarios_tested","expected_failures","observed_failures","escaped_failures","result"]}',
        encoding="utf-8",
    )
    (root / "PROOF_REGISTRY_SCHEMA.json").write_text(
        '{"required":["trace_id","task_id","timestamp","execution","validation_receipt_ref","trace_receipt_ref","counterfactual_report_ref","mutation_proof","mpp_hash","bts_hash","optimality_hash","result"]}',
        encoding="utf-8",
    )


def _paths(root: Path) -> EnginePaths:
    return EnginePaths(
        root=root,
        current_root=root / "CURRENT_ROOT.json",
        events=root / "events.jsonl",
        runtime_state=root / "runtime_state.json",
        schema=root / "EVENT_SCHEMA.json",
        receipts_dir=root / "receipts",
        workflow_spec=root / "MASTER_WORKFLOW_V2.md",
        replay_rules=root / "REPLAY_RULES.md",
        gate_spec=root / "EXECUTION_GATE_SPEC.md",
        acceptance_tests=root / "ACCEPTANCE_TESTS.md",
        agents_instructions=root / "AGENTS.md",
        cdr_spec=root / "CDR_SPEC.md",
        cdr_security=root / "CDR_SECURITY.md",
        cdr_verification=root / "CDR_VERIFICATION.md",
        cdr_observability=root / "CDR_OBSERVABILITY.md",
        cdr_lifecycle=root / "CDR_LIFECYCLE.md",
        mpp_schema=root / "MASTER_MPP_SCHEMA_v1.md",
        validation_layer_spec=root / "VALIDATION_LAYER_SPEC.md",
        mpp_stage_pipeline=root / "MPP_STAGE_PIPELINE.md",
        mpp_stage_policy_schema=root / "MPP_STAGE_POLICY_SCHEMA.json",
        mpp_stage_artifact_schema=root / "MPP_STAGE_ARTIFACT_SCHEMA.json",
    )


def test_executes_exactly_one_bounded_stage(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    events = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:00:00+00:00",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {
                "stage_id": "s1",
                "bounded": True,
                "mutates": True,
                "compensation": {"strategy": "rollback"},
            },
        },
        {
            "event_id": "e2",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:01:00+00:00",
            "turn_id": 2,
            "idempotency_key": "enqueue:s2",
            "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
        },
    ]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )

    receipt = _execute_once_internal(_paths(tmp_path))
    assert receipt["executed_stage_id"] == "s1"
    assert receipt["gates"]["acceptance_tests_passed"] is True

    lines = (tmp_path / "events.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 3
    appended = json.loads(lines[-1])
    assert appended["type"] == "STAGE_EXECUTED"
    assert appended["state_hash_before"]
    assert appended["state_hash_after"]


def test_invariant_coverage_map_runtime_enforced(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    (tmp_path / "INVARIANT_COVERAGE_MAP.md").write_text(
        "# Invariant Coverage Map\n\n| Critical property | Enforcing invariant |\n|---|---|\n"
        "| A | `MutationProofInvariant` |\n"
        "| B | `MutationProofInvariant` |\n",
        encoding="utf-8",
    )
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
    with pytest.raises(EngineError, match="coverage map has duplicate invariant"):
        _execute_once_internal(_paths(tmp_path))


def test_invariant_coverage_map_unknown_invariant_fails(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    (tmp_path / "INVARIANT_COVERAGE_MAP.md").write_text(
        "# Invariant Coverage Map\n\n| Critical property | Enforcing invariant |\n|---|---|\n"
        "| A | `UnknownInvariant` |\n",
        encoding="utf-8",
    )
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
    with pytest.raises(EngineError, match="unknown invariants"):
        _execute_once_internal(_paths(tmp_path))


def test_immutable_config_mutation_fails(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    immutable = json.loads(
        (tmp_path / "IMMUTABLE_CONFIG.json").read_text(encoding="utf-8")
    )
    immutable["coverage_map_sha256"] = "tampered"
    (tmp_path / "IMMUTABLE_CONFIG.json").write_text(
        json.dumps(immutable), encoding="utf-8"
    )
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
    with pytest.raises(EngineError, match="Immutable config mismatch"):
        _execute_once_internal(_paths(tmp_path))


def test_runtime_tamper_snapshot_check_fails_closed(tmp_path: Path) -> None:
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
    (tmp_path / "proof_registry_snapshot.sha256").write_text(
        "tampered\n", encoding="utf-8"
    )
    with pytest.raises(EngineError, match="tamper check failed"):
        engine.execute_with_recovery(
            _paths(tmp_path),
            task_id="tamper",
            failure_class="SOFT_FAILURE",
            retry_class="RETRYABLE",
        )


def test_blocks_on_replay_hash_mismatch(tmp_path: Path) -> None:
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
            "type": "STAGE_EXECUTED",
            "ts": "2026-01-01T00:01:00+00:00",
            "turn_id": 2,
            "idempotency_key": "turn-exec:s1",
            "state_hash_before": "bad-before",
            "state_hash_after": "bad-after",
            "payload": {
                "stage_id": "s1",
                "bounded": True,
                "mutates": False,
                "status": "ok",
                "output": {"stage_id": "s1", "status": "ok"},
                "output_hash": engine.hashlib.sha256(
                    engine._canonical_json({"stage_id": "s1", "status": "ok"}).encode(
                        "utf-8"
                    )
                ).hexdigest(),
            },
        },
    ]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )

    with pytest.raises(EngineError, match="state_hash"):
        _execute_once_internal(_paths(tmp_path))


def test_blocks_on_idempotency_collision(tmp_path: Path) -> None:
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
            "ts": "2026-01-01T00:01:00+00:00",
            "turn_id": 2,
            "idempotency_key": "turn-exec:s1",
            "payload": {"stage_id": "sX", "bounded": False, "mutates": False},
        },
    ]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )

    with pytest.raises(EngineError, match="Idempotency key"):
        _execute_once_internal(_paths(tmp_path))


def test_blocks_mutating_stage_without_compensation(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": True},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")

    with pytest.raises(EngineError, match="compensation"):
        _execute_once_internal(_paths(tmp_path))


def test_blocks_on_schema_failure(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    invalid_event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1"},
    }
    (tmp_path / "events.jsonl").write_text(
        json.dumps(invalid_event) + "\n", encoding="utf-8"
    )

    with pytest.raises(EngineError, match="Payload missing required field: bounded"):
        _execute_once_internal(_paths(tmp_path))


def test_blocks_on_event_order_corruption(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    events = [
        {
            "event_id": "e1",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:00:00+00:00",
            "turn_id": 2,
            "idempotency_key": "enqueue:s1",
            "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
        },
        {
            "event_id": "e2",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:01:00+00:00",
            "turn_id": 1,
            "idempotency_key": "enqueue:s2",
            "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
        },
    ]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )

    with pytest.raises(EngineError, match="turn_id must strictly increase"):
        _execute_once_internal(_paths(tmp_path))


def test_blocks_on_stage_sequence_mismatch(tmp_path: Path) -> None:
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
            "ts": "2026-01-01T00:01:00+00:00",
            "turn_id": 2,
            "idempotency_key": "enqueue:s2",
            "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
        },
        {
            "event_id": "e3",
            "type": "STAGE_EXECUTED",
            "ts": "2026-01-01T00:02:00+00:00",
            "turn_id": 3,
            "idempotency_key": "turn-exec:s2",
            "state_hash_before": "x",
            "state_hash_after": "y",
            "payload": {
                "stage_id": "s2",
                "bounded": True,
                "mutates": False,
                "status": "ok",
                "output": {"stage_id": "s2", "status": "ok"},
                "output_hash": engine.hashlib.sha256(
                    engine._canonical_json({"stage_id": "s2", "status": "ok"}).encode(
                        "utf-8"
                    )
                ).hexdigest(),
            },
        },
    ]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )

    with pytest.raises(EngineError, match="stage sequence mismatch"):
        _execute_once_internal(_paths(tmp_path))


def test_partial_event_write_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    seed = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    events_path = tmp_path / "events.jsonl"
    events_path.write_text(json.dumps(seed) + "\n", encoding="utf-8")
    original_events = [
        json.loads(line)
        for line in events_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    original_atomic = engine._atomic_write_text

    def flaky(path: Path, content: str) -> None:
        original_atomic(path, content)
        if path.name == "runtime_state.json":
            raise OSError("simulated disk error")

    monkeypatch.setattr(engine, "_atomic_write_text", flaky)

    with pytest.raises(EngineError, match="Write transaction failed"):
        _execute_once_internal(_paths(tmp_path))

    after_events = [
        json.loads(line)
        for line in events_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert after_events == original_events


def test_partial_event_append_corruption_is_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    seed = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    events_path = tmp_path / "events.jsonl"
    events_path.write_text(json.dumps(seed) + "\n", encoding="utf-8")
    original = [
        json.loads(line)
        for line in events_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    original_replace = engine.os.replace

    failed_once = {"v": False}

    def explode(src: Path, dst: Path) -> None:
        if str(dst).endswith("events.jsonl") and not failed_once["v"]:
            failed_once["v"] = True
            raise OSError("simulated rename failure")
        original_replace(src, dst)

    monkeypatch.setattr(engine.os, "replace", explode)

    with pytest.raises(EngineError, match="Write transaction failed"):
        _execute_once_internal(_paths(tmp_path))

    after = [
        json.loads(line)
        for line in events_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert after == original


def test_allows_distinct_execution_identities_with_different_outputs(
    tmp_path: Path,
) -> None:
    _write_controls(tmp_path)
    e1 = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    e1b = {
        "event_id": "e1b",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:30+00:00",
        "turn_id": 2,
        "idempotency_key": "enqueue:s1:2",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    e2 = {
        "event_id": "e2",
        "type": "STAGE_EXECUTED",
        "ts": "2026-01-01T00:01:00+00:00",
        "turn_id": 3,
        "idempotency_key": "turn-exec:s1#1",
        "payload": {
            "stage_id": "s1",
            "bounded": True,
            "mutates": False,
            "status": "ok",
            "output": {"v": 1},
        },
    }
    e2["payload"]["output_hash"] = engine.hashlib.sha256(
        engine._canonical_json({"v": 1}).encode("utf-8")
    ).hexdigest()
    e2["state_hash_before"] = engine._state_hash(engine.replay_state([e1, e1b]))
    e2["state_hash_after"] = engine._state_hash(engine.replay_state([e1, e1b, e2]))
    e3 = {
        "event_id": "e3",
        "type": "STAGE_EXECUTED",
        "ts": "2026-01-01T00:02:00+00:00",
        "turn_id": 4,
        "idempotency_key": "turn-exec:s1#2",
        "payload": {
            "stage_id": "s1",
            "bounded": True,
            "mutates": False,
            "status": "ok",
            "output": {"v": 2},
        },
    }
    e3["payload"]["output_hash"] = engine.hashlib.sha256(
        engine._canonical_json({"v": 2}).encode("utf-8")
    ).hexdigest()
    e3["state_hash_before"] = engine._state_hash(engine.replay_state([e1, e1b, e2]))
    e3["state_hash_after"] = engine._state_hash(engine.replay_state([e1, e1b, e2, e3]))
    events = [e1, e1b, e2, e3]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )

    receipt = _execute_once_internal(_paths(tmp_path))
    assert receipt["gates"]["replay_valid"] is True


def test_runtime_state_mismatch_fails_closed(tmp_path: Path) -> None:
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
        json.dumps(events[0]) + "\n", encoding="utf-8"
    )
    # Poison runtime_state to prove replay-only reconstruction.
    (tmp_path / "runtime_state.json").write_text(
        json.dumps(
            {"pending_stages": [{"stage_id": "WRONG"}], "completed_stage_ids": ["s1"]}
        ),
        encoding="utf-8",
    )

    with pytest.raises(EngineError, match="Write transaction failed"):
        _execute_once_internal(_paths(tmp_path))


def test_blocks_when_executed_event_missing_output_hash(tmp_path: Path) -> None:
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
            "type": "STAGE_EXECUTED",
            "ts": "2026-01-01T00:01:00+00:00",
            "turn_id": 2,
            "idempotency_key": "turn-exec:s1",
            "state_hash_before": "x",
            "state_hash_after": "y",
            "payload": {
                "stage_id": "s1",
                "bounded": True,
                "mutates": False,
                "status": "ok",
                "output": {"v": 1},
            },
        },
    ]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )
    with pytest.raises(EngineError, match="requires payload.output_hash"):
        _execute_once_internal(_paths(tmp_path))


def test_blocks_when_output_hash_mismatch(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    e1 = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    e2 = {
        "event_id": "e2",
        "type": "STAGE_EXECUTED",
        "ts": "2026-01-01T00:01:00+00:00",
        "turn_id": 2,
        "idempotency_key": "turn-exec:s1",
        "payload": {
            "stage_id": "s1",
            "bounded": True,
            "mutates": False,
            "status": "ok",
            "output": {"v": 1},
            "output_hash": "not-a-real-hash",
        },
    }
    e2["state_hash_before"] = engine._state_hash(engine.replay_state([e1]))
    e2["state_hash_after"] = engine._state_hash(engine.replay_state([e1, e2]))
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in [e1, e2]) + "\n", encoding="utf-8"
    )
    with pytest.raises(EngineError, match="output_hash mismatch"):
        _execute_once_internal(_paths(tmp_path))


def test_control_file_present_but_rule_missing_fails(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    # File exists, but required gate rule is removed: engine must fail.
    (tmp_path / "EXECUTION_GATE_SPEC.md").write_text(
        "schema_valid\\nreplay_valid\\nidempotency_valid\\n", encoding="utf-8"
    )
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    with pytest.raises(EngineError, match="Execution gate spec missing required rule"):
        _execute_once_internal(_paths(tmp_path))


def test_blocks_on_duplicate_event_id(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    events = [
        {
            "event_id": "dup",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:00:00+00:00",
            "turn_id": 1,
            "idempotency_key": "enqueue:s1",
            "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
        },
        {
            "event_id": "dup",
            "type": "STAGE_ENQUEUED",
            "ts": "2026-01-01T00:01:00+00:00",
            "turn_id": 2,
            "idempotency_key": "enqueue:s2",
            "payload": {"stage_id": "s2", "bounded": True, "mutates": False},
        },
    ]
    (tmp_path / "events.jsonl").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
    )
    with pytest.raises(EngineError, match="duplicate event_id"):
        _execute_once_internal(_paths(tmp_path))


def test_blocks_on_replay_completeness_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    monkeypatch.setattr(engine, "_consume_events_exactly_once", lambda events: [])
    with pytest.raises(EngineError, match="Replay completeness failed"):
        _execute_once_internal(_paths(tmp_path))


def test_blocks_path_traversal_outside_canonical_root(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    bad_paths = _paths(tmp_path)
    bad_paths = EnginePaths(
        root=bad_paths.root,
        current_root=bad_paths.current_root,
        events=Path(tmp_path / "subdir/../events.jsonl"),
        runtime_state=Path(tmp_path / "../escaped_runtime_state.json"),
        schema=bad_paths.schema,
        receipts_dir=bad_paths.receipts_dir,
        workflow_spec=bad_paths.workflow_spec,
        replay_rules=bad_paths.replay_rules,
        gate_spec=bad_paths.gate_spec,
        acceptance_tests=bad_paths.acceptance_tests,
        agents_instructions=bad_paths.agents_instructions,
        cdr_spec=bad_paths.cdr_spec,
        cdr_security=bad_paths.cdr_security,
        cdr_verification=bad_paths.cdr_verification,
        cdr_observability=bad_paths.cdr_observability,
        cdr_lifecycle=bad_paths.cdr_lifecycle,
        mpp_schema=bad_paths.mpp_schema,
        validation_layer_spec=bad_paths.validation_layer_spec,
        mpp_stage_pipeline=bad_paths.mpp_stage_pipeline,
        mpp_stage_policy_schema=bad_paths.mpp_stage_policy_schema,
        mpp_stage_artifact_schema=bad_paths.mpp_stage_artifact_schema,
    )
    with pytest.raises(EngineError, match="outside canonical root"):
        _execute_once_internal(bad_paths)


def test_workflow_marker_violation_fails(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    (tmp_path / "MASTER_WORKFLOW_V2.md").write_text(
        "invalid workflow", encoding="utf-8"
    )
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    with pytest.raises(EngineError, match="MASTER_WORKFLOW_V2.md"):
        _execute_once_internal(_paths(tmp_path))


def test_replay_rule_marker_violation_fails(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    (tmp_path / "REPLAY_RULES.md").write_text("Replay source only", encoding="utf-8")
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    with pytest.raises(EngineError, match="REPLAY_RULES.md"):
        _execute_once_internal(_paths(tmp_path))


def test_acceptance_marker_violation_fails_and_no_append(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    (tmp_path / "ACCEPTANCE_TESTS.md").write_text(
        "acceptance doc without required markers", encoding="utf-8"
    )
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    events_path = tmp_path / "events.jsonl"
    events_path.write_text(json.dumps(event) + "\n", encoding="utf-8")
    before = events_path.read_text(encoding="utf-8")
    with pytest.raises(EngineError, match="ACCEPTANCE_TESTS.md"):
        _execute_once_internal(_paths(tmp_path))
    assert events_path.read_text(encoding="utf-8") == before


def test_stage2_full_system_validation(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    # One bounded mutating stage with compensation to exercise core gates.
    e1 = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {
            "stage_id": "s1",
            "bounded": True,
            "mutates": True,
            "compensation": {"strategy": "rollback"},
        },
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(e1) + "\n", encoding="utf-8")

    receipt = _execute_once_internal(_paths(tmp_path))
    assert receipt["ok"] is True
    assert receipt["executed_stage_id"] == "s1"
    assert receipt["gates"]["schema_valid"] is True
    assert receipt["gates"]["replay_valid"] is True
    assert receipt["gates"]["acceptance_tests_passed"] is True

    # Validate appended execution event invariants end-to-end.
    lines = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(lines) == 2
    executed = lines[-1]
    assert executed["type"] == "STAGE_EXECUTED"
    assert (
        executed["payload"]["output_hash"]
        == engine.hashlib.sha256(
            engine._canonical_json(executed["payload"]["output"]).encode("utf-8")
        ).hexdigest()
    )

    # runtime_state is replay-derived (source of truth = events.jsonl).
    runtime_state = json.loads(
        (tmp_path / "runtime_state.json").read_text(encoding="utf-8")
    )
    assert runtime_state["replayed_event_count"] == 2


@pytest.mark.parametrize(
    ("file_name", "content", "error_match"),
    [
        (
            "CDR_SECURITY.md",
            "INPUT_VALIDATION_REQUIRED\n",
            "CDR_SECURITY.md missing required marker",
        ),
        (
            "CDR_VERIFICATION.md",
            "UNIT_TEST_REQUIRED\n",
            "CDR_VERIFICATION.md missing required marker",
        ),
        (
            "CDR_OBSERVABILITY.md",
            "STRUCTURED_LOGGING_REQUIRED\n",
            "CDR_OBSERVABILITY.md missing required marker",
        ),
        (
            "CDR_LIFECYCLE.md",
            "RATIONALE_MUST_UPDATE_WITH_CODE\n",
            "CDR_LIFECYCLE.md missing required marker",
        ),
    ],
)
def test_cdr_marker_missing_fails_closed(
    tmp_path: Path, file_name: str, content: str, error_match: str
) -> None:
    _write_controls(tmp_path)
    (tmp_path / file_name).write_text(content, encoding="utf-8")
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    events_path = tmp_path / "events.jsonl"
    events_path.write_text(json.dumps(event) + "\n", encoding="utf-8")
    before = events_path.read_text(encoding="utf-8")
    with pytest.raises(EngineError, match=error_match):
        _execute_once_internal(_paths(tmp_path))
    assert events_path.read_text(encoding="utf-8") == before


def test_cdr_security_hardcoded_secret_detection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    original_read = engine._read_text

    def fake_read(path: Path) -> str:
        if str(path).endswith("src/turn_execution_engine.py"):
            return 'api_key = \'hardcoded-secret\'\\n_validate_event_schema\\n_assert_within_root\\n"gates" "event_count"\\nRATIONALE:'
        return original_read(path)

    monkeypatch.setattr(engine, "_read_text", fake_read)
    with pytest.raises(EngineError, match="hardcoded secret pattern"):
        _execute_once_internal(_paths(tmp_path))


def test_cdr_security_unsafe_error_handling_detection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    original_read = engine._read_text

    def fake_read(path: Path) -> str:
        if str(path).endswith("src/turn_execution_engine.py"):
            return 'raise EngineError(f\'Write transaction failed: {exc}\')\\n_validate_event_schema\\n_assert_within_root\\n"gates" "event_count"\\nRATIONALE:'
        return original_read(path)

    monkeypatch.setattr(engine, "_read_text", fake_read)
    with pytest.raises(EngineError, match="unsafe error exposure"):
        _execute_once_internal(_paths(tmp_path))


def test_cdr_verification_missing_integration_test_detection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    original_read = engine._read_text

    def fake_read(path: Path) -> str:
        if str(path).endswith("tests/test_turn_execution_engine.py"):
            return "def test_unit_only():\\n    pass\\n"
        return original_read(path)

    monkeypatch.setattr(engine, "_read_text", fake_read)
    with pytest.raises(EngineError, match="integration-style test missing"):
        _execute_once_internal(_paths(tmp_path))


def test_cdr_observability_missing_structured_logging_detection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    original_read = engine._read_text

    def fake_read(path: Path) -> str:
        if str(path).endswith("src/turn_execution_engine.py"):
            return "RATIONALE:\\n_validate_event_schema\\n_assert_within_root"
        return original_read(path)

    monkeypatch.setattr(engine, "_read_text", fake_read)
    with pytest.raises(EngineError, match="structured state-change logging missing"):
        _execute_once_internal(_paths(tmp_path))


def test_cdr_lifecycle_missing_tradeoffs_future_gaps_detection(tmp_path: Path) -> None:
    _write_controls(tmp_path)
    (tmp_path / "CDR_SPEC.md").write_text(
        "CDR-RAT\nCDR-ARCH\nCDR-CODE\nCDR-LINT\n", encoding="utf-8"
    )
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    with pytest.raises(EngineError, match="CDR_SPEC.md missing required marker"):
        _execute_once_internal(_paths(tmp_path))


def test_cdr_lifecycle_missing_rationale_headers_detection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_controls(tmp_path)
    event = {
        "event_id": "e1",
        "type": "STAGE_ENQUEUED",
        "ts": "2026-01-01T00:00:00+00:00",
        "turn_id": 1,
        "idempotency_key": "enqueue:s1",
        "payload": {"stage_id": "s1", "bounded": True, "mutates": False},
    }
    (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    original_read = engine._read_text

    def fake_read(path: Path) -> str:
        if str(path).endswith("src/turn_execution_engine.py"):
            return '"gates" "event_count" _validate_event_schema _assert_within_root'
        return original_read(path)

    monkeypatch.setattr(engine, "_read_text", fake_read)
    with pytest.raises(EngineError, match="rationale headers missing in code"):
        _execute_once_internal(_paths(tmp_path))

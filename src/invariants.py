from __future__ import annotations

import hashlib
import json
import unicodedata
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import (
    Draft202012Validator,
    ValidationError as JsonSchemaValidationError,
    validate,
)
from src.mpp.mpp_canonical import MPPCanonicalError, canonical_mpp_hash
from src.bts.bts_canonical import (
    BTSCanonicalError,
    canonical_bts_hash,
    canonical_bts_payload,
)
from src.optimality.optimality_model import (
    OptimalityError,
    canonical_optimality_hash,
    canonical_optimality_payload,
)
from src.review.claim_consistency import assess_claim_consistency
from src.review.diff_classifier import DiffClassification


@dataclass(frozen=True)
class ValidationResult:
    name: str
    passed: bool
    failures: list[str]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class Invariant:
    name: str
    stage: str
    severity: str
    requires: list[str]

    def validate(self, context: dict[str, Any]) -> ValidationResult:  # pragma: no cover
        raise NotImplementedError


class InvariantContractError(RuntimeError):
    pass


NON_SEMANTIC_EVENT_FIELDS = frozenset({"ts"})
INVARIANT_REGISTRY_VERSION = "1.0.0"
FAILURE_CLASS_ENUM = frozenset({"HARD_FAILURE", "SOFT_FAILURE"})
RETRY_CLASS_ENUM = frozenset({"NON_RETRYABLE", "RETRYABLE"})
ALLOWED_RULES = frozenset(
    {
        "mutation_proof_required",
        "mutation_proof_schema",
        "delta_mismatch",
        "no_silent_success",
        "pre_hash_mismatch",
        "post_hash_mismatch",
        "artifact_presence",
        "execution_proof_required",
        "cross_artifact_consistency",
        "causal_binding_missing",
        "counterfactual_indistinguishable",
        "broken_proof_chain",
        "duplicate_trace_id",
        "events_hash_binding_missing",
        "tampered_proof_entry_hash",
        "events_hash_mismatch",
        "event_order_invalid",
        "runtime_state_mismatch",
        "proof_snapshot_mismatch",
        "invariant_registry_version_mismatch",
        "export_manifest_mismatch",
        "mpp_compliance_failed",
        "mpp_hash_missing",
        "mpp_hash_mismatch",
        "mpp_artifact_invalid",
        "bts_hash_missing",
        "bts_hash_mismatch",
        "bts_artifact_invalid",
        "bts_insufficient_diversity",
        "bts_missing_justification",
        "bts_decision_inconsistent",
        "optimality_hash_missing",
        "optimality_hash_mismatch",
        "optimality_artifact_invalid",
        "suboptimal_decision_detected",
        "tradeoff_not_quantified",
        "option_salience_failure",
        "decision_improvement_failure",
        "implementation_non_substantive",
        "implementation_missing_test_evidence",
        "implementation_surface_mismatch",
        "claim_inconsistent_with_diff",
        "claim_overstates_changes",
    }
)
STRUCTURED_FAILURE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["rule", "failure_class", "retry_class"],
    "properties": {
        "rule": {"type": "string", "enum": sorted(ALLOWED_RULES)},
        "failure_class": {"type": "string", "enum": sorted(FAILURE_CLASS_ENUM)},
        "retry_class": {"type": "string", "enum": sorted(RETRY_CLASS_ENUM)},
    },
}


def _structured(rule: str) -> dict[str, str]:
    return {
        "rule": rule,
        "failure_class": "HARD_FAILURE",
        "retry_class": "NON_RETRYABLE",
    }


class MutationProofInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("MutationProofInvariant", "11", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        mutation_proof = context.get("mutation_proof")
        pre_hash = context.get("pre_hash")
        post_hash = context.get("post_hash")
        artifacts_present = context.get("artifacts_present", [])
        failures: list[str] = []
        structured: list[dict[str, str]] = []

        if mutation_proof is None:
            failures.append("mutation_proof_required")
            structured.append(_structured("mutation_proof_required"))
        else:
            required = {"target_id", "delta_observed", "pre_hash", "post_hash"}
            if not required.issubset(mutation_proof):
                failures.append("mutation_proof_schema")
                structured.append(_structured("mutation_proof_schema"))

        if mutation_proof:
            derived_delta = mutation_proof.get("pre_hash") != mutation_proof.get(
                "post_hash"
            )
            declared_delta = bool(mutation_proof.get("delta_observed"))
            if declared_delta != derived_delta:
                failures.append("delta_mismatch")
                structured.append(_structured("delta_mismatch"))
            if not derived_delta:
                failures.append("no_silent_success")
                structured.append(_structured("no_silent_success"))
            if pre_hash and mutation_proof.get("pre_hash") != pre_hash:
                failures.append("pre_hash_mismatch")
                structured.append(_structured("pre_hash_mismatch"))
            if post_hash and mutation_proof.get("post_hash") != post_hash:
                failures.append("post_hash_mismatch")
                structured.append(_structured("post_hash_mismatch"))

        for required_artifact in ["events.jsonl", "runtime_state.json"]:
            if required_artifact not in artifacts_present:
                failures.append("artifact_presence")
                structured.append(_structured("artifact_presence"))
                break

        return ValidationResult(
            self.name,
            passed=not failures,
            failures=failures,
            metadata={
                "structured_failures": structured,
                "required_artifacts": ["events.jsonl", "runtime_state.json"],
            },
        )


class TraceConsistencyInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__(
            "TraceConsistencyInvariant", "12", "HARD", ["MutationProofInvariant"]
        )

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        execution_claimed = bool(context.get("execution_claimed", True))
        execution_events = context.get("execution_events", [])
        mutation_proof = context.get("mutation_proof")
        mutated_artifact = context.get("mutated_artifact")
        failures: list[str] = []
        structured: list[dict[str, str]] = []

        if execution_claimed and not execution_events:
            failures.append("execution_proof_required")
            structured.append(_structured("execution_proof_required"))
        if execution_claimed and mutation_proof and execution_events:
            if mutation_proof.get("target_id") not in [
                e.get("stage_id") for e in execution_events
            ]:
                failures.append("cross_artifact_consistency")
                structured.append(_structured("cross_artifact_consistency"))
            bound = [
                e
                for e in execution_events
                if e.get("target_id") == mutation_proof.get("target_id")
                and e.get("artifact_id") == mutated_artifact
            ]
            if not bound:
                failures.append("causal_binding_missing")
                structured.append(_structured("causal_binding_missing"))

        return ValidationResult(
            self.name,
            passed=not failures,
            failures=failures,
            metadata={"structured_failures": structured},
        )


class CounterfactualInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__(
            "CounterfactualInvariant", "13", "HARD", ["MutationProofInvariant"]
        )

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        pre_hash = context.get("pre_hash")
        post_hash = context.get("post_hash")
        failures: list[str] = []
        structured: list[dict[str, str]] = []
        distinguishing_signals: list[dict[str, str]] = []

        if pre_hash and post_hash and pre_hash != post_hash:
            distinguishing_signals.append(
                {
                    "signal_type": "hash_transition",
                    "pre_hash": pre_hash,
                    "post_hash": post_hash,
                }
            )
        if not distinguishing_signals:
            failures.append("counterfactual_indistinguishable")
            structured.append(_structured("counterfactual_indistinguishable"))

        return ValidationResult(
            self.name,
            passed=not failures,
            failures=failures,
            metadata={
                "structured_failures": structured,
                "distinguishing_signals": distinguishing_signals,
            },
        )


class ProofChainInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("ProofChainInvariant", "18", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        proofs = context.get("proofs", [])
        failures: list[str] = []
        if proofs and proofs[0].get("prev_hash", "") not in ("", None):
            failures.append("broken_proof_chain")
        for i in range(1, len(proofs)):
            prior_hash = proofs[i - 1].get(
                "current_hash", proofs[i - 1].get("entry_hash")
            )
            if proofs[i].get("prev_hash") != prior_hash:
                failures.append("broken_proof_chain")
                break
        trace_ids = [p.get("trace_id") for p in proofs if p.get("trace_id")]
        if len(trace_ids) != len(set(trace_ids)):
            failures.append("duplicate_trace_id")
        expected_events_hash = context.get("events_hash")
        if (
            proofs
            and expected_events_hash
            and not any(p.get("events_hash") == expected_events_hash for p in proofs)
        ):
            failures.append("events_hash_binding_missing")
        for entry in proofs:
            proof_payload = {
                "trace_id": entry.get("trace_id"),
                "invariant_registry_version": entry.get("invariant_registry_version"),
                "task_id": entry.get("task_id"),
                "execution": entry.get("execution"),
                "mutation_proof": entry.get("mutation_proof"),
                "state_hash_after": entry.get("state_hash_after"),
                "result": entry.get("result"),
            }
            if "mpp_hash" in entry:
                proof_payload["mpp_hash"] = entry.get("mpp_hash", "")
            if "bts_hash" in entry:
                proof_payload["bts_hash"] = entry.get("bts_hash", "")
            if "optimality_hash" in entry:
                proof_payload["optimality_hash"] = entry.get("optimality_hash", "")
            canonical = json.dumps(
                {
                    "proof": proof_payload,
                    "prev_hash": entry.get("prev_hash", ""),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            stored_hash = entry.get("current_hash", entry.get("entry_hash"))
            if (
                stored_hash != hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                or entry.get("entry_hash") != stored_hash
            ):
                failures.append("tampered_proof_entry_hash")
                break
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class ReplayDeterminismInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("ReplayDeterminismInvariant", "18", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        expected = context.get("events_hash")
        actual = context.get("computed_events_hash")
        failures: list[str] = []
        if expected and actual and expected != actual:
            failures.append("events_hash_mismatch")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class EventOrderInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("EventOrderInvariant", "18", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        failures = []
        if context.get("event_order_invalid"):
            failures.append("event_order_invalid")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class RuntimeStateConsistencyInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("RuntimeStateConsistencyInvariant", "18", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        failures = []
        if context.get("runtime_state_invalid"):
            failures.append("runtime_state_mismatch")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class StateHashBindingInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("StateHashBindingInvariant", "18", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        failures = []
        expected = context.get("proof_state_hash")
        replayed = context.get("replayed_state_hash")
        if expected and replayed and expected != replayed:
            failures.append("runtime_state_mismatch")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class ProofRegistrySnapshotInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("ProofRegistrySnapshotInvariant", "18", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        failures = []
        expected = context.get("expected_proof_snapshot_hash")
        actual = context.get("actual_proof_snapshot_hash")
        if expected and actual and expected != actual:
            failures.append("proof_snapshot_mismatch")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class InvariantRegistryVersionInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("InvariantRegistryVersionInvariant", "18", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        failures = []
        entry_version = context.get("invariant_registry_version")
        if entry_version and entry_version != INVARIANT_REGISTRY_VERSION:
            failures.append("invariant_registry_version_mismatch")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class ExportManifestInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("ExportManifestInvariant", "18", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        failures = []
        expected = context.get("expected_manifest_hash")
        actual = context.get("actual_manifest_hash")
        if expected and actual and expected != actual:
            failures.append("export_manifest_mismatch")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class MPPComplianceInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("MPPComplianceInvariant", "10", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        failures = []
        mpp = context.get("mpp_compliance")
        if mpp is not None and not bool(mpp.get("compliant")):
            failures.append("mpp_compliance_failed")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class MPPHashInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("MPPHashInvariant", "10", "HARD", ["MPPComplianceInvariant"])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        failures: list[str] = []
        mpp_task_id = context.get("mpp_task_id")
        stored = context.get("proof_mpp_hash", "")
        root = context.get("root_path")
        if not mpp_task_id:
            return ValidationResult(self.name, passed=True, failures=[], metadata={})
        if not stored:
            failures.append("mpp_hash_missing")
            return ValidationResult(
                self.name, passed=False, failures=failures, metadata={}
            )
        if not isinstance(root, str) or not root:
            failures.append("mpp_artifact_invalid")
            return ValidationResult(
                self.name, passed=False, failures=failures, metadata={}
            )
        try:
            computed = canonical_mpp_hash(Path(root), str(mpp_task_id))
        except MPPCanonicalError:
            failures.append("mpp_artifact_invalid")
            return ValidationResult(
                self.name, passed=False, failures=failures, metadata={}
            )
        if computed != stored:
            failures.append("mpp_hash_mismatch")
        return ValidationResult(
            self.name,
            passed=not failures,
            failures=failures,
            metadata={"computed_mpp_hash": computed if not failures else ""},
        )


class BTSIntegrityInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("BTSIntegrityInvariant", "10", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        bts_task_id = context.get("bts_task_id")
        stored = context.get("proof_bts_hash", "")
        root = context.get("root_path")
        failures: list[str] = []
        if not bts_task_id:
            return ValidationResult(self.name, passed=True, failures=[], metadata={})
        if not stored:
            failures.append("bts_hash_missing")
            return ValidationResult(
                self.name, passed=False, failures=failures, metadata={}
            )
        if not isinstance(root, str) or not root:
            failures.append("bts_artifact_invalid")
            return ValidationResult(
                self.name, passed=False, failures=failures, metadata={}
            )
        try:
            computed = canonical_bts_hash(Path(root), str(bts_task_id))
        except BTSCanonicalError:
            failures.append("bts_artifact_invalid")
            return ValidationResult(
                self.name, passed=False, failures=failures, metadata={}
            )
        if computed != stored:
            failures.append("bts_hash_mismatch")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class BTSCompletenessInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__(
            "BTSCompletenessInvariant", "10", "HARD", ["BTSIntegrityInvariant"]
        )

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        bts_task_id = context.get("bts_task_id")
        root = context.get("root_path")
        if not bts_task_id:
            return ValidationResult(self.name, passed=True, failures=[], metadata={})
        failures: list[str] = []
        try:
            payload = canonical_bts_payload(Path(str(root)), str(bts_task_id))
            options = payload["trace"]["options"]
            signatures = {
                (
                    str(i.get("option_id", "")),
                    str(i.get("approach_key", "")),
                    tuple(sorted(str(m) for m in i.get("touched_modules", []))),
                )
                for i in options
                if isinstance(i, dict)
            }
            if len(signatures) < 5:
                failures.append("bts_insufficient_diversity")
        except Exception:
            failures.append("bts_artifact_invalid")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class BTSJustificationInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__(
            "BTSJustificationInvariant", "10", "HARD", ["BTSIntegrityInvariant"]
        )

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        bts_task_id = context.get("bts_task_id")
        root = context.get("root_path")
        if not bts_task_id:
            return ValidationResult(self.name, passed=True, failures=[], metadata={})
        failures: list[str] = []
        try:
            payload = canonical_bts_payload(Path(str(root)), str(bts_task_id))
            rejected = payload["trace"]["rejected_options"]
            if not all(
                item.get("reason") and item.get("criteria_links") for item in rejected
            ):
                failures.append("bts_missing_justification")
        except Exception:
            failures.append("bts_artifact_invalid")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class DecisionConsistencyInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__(
            "DecisionConsistencyInvariant", "10", "HARD", ["BTSIntegrityInvariant"]
        )

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        bts_task_id = context.get("bts_task_id")
        root = context.get("root_path")
        if not bts_task_id:
            return ValidationResult(self.name, passed=True, failures=[], metadata={})
        failures: list[str] = []
        try:
            payload = canonical_bts_payload(Path(str(root)), str(bts_task_id))
            trace = payload["trace"]
            option_ids = {str(item.get("option_id")) for item in trace["options"]}
            chosen = str(trace["chosen_option"])
            if chosen not in option_ids:
                failures.append("bts_decision_inconsistent")
            scores = trace["evaluation_scores"]
            if str(chosen) not in {str(k) for k in scores.keys()}:
                failures.append("bts_decision_inconsistent")
        except Exception:
            failures.append("bts_artifact_invalid")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class OptimalityInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("OptimalityInvariant", "10", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        task_id = context.get("optimality_task_id")
        stored = context.get("proof_optimality_hash", "")
        root = context.get("root_path")
        if not task_id:
            return ValidationResult(self.name, passed=True, failures=[], metadata={})
        failures: list[str] = []
        if not stored:
            failures.append("optimality_hash_missing")
            return ValidationResult(
                self.name, passed=False, failures=failures, metadata={}
            )
        try:
            computed = canonical_optimality_hash(Path(str(root)), str(task_id))
            payload = canonical_optimality_payload(Path(str(root)), str(task_id))
            if (
                payload["chosen_option"] != payload["best_option"]
                or payload["regret"] > 0.0
            ):
                failures.append("suboptimal_decision_detected")
        except (OptimalityError, ValueError):
            failures.append("optimality_artifact_invalid")
            return ValidationResult(
                self.name, passed=False, failures=failures, metadata={}
            )
        if computed != stored:
            failures.append("optimality_hash_mismatch")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class TradeoffQuantificationInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("TradeoffQuantificationInvariant", "10", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        task_id = context.get("optimality_task_id")
        root = context.get("root_path")
        if not task_id:
            return ValidationResult(self.name, passed=True, failures=[], metadata={})
        failures: list[str] = []
        try:
            trace = json.loads(
                (
                    Path(str(root))
                    / "optimality_artifacts"
                    / str(task_id)
                    / "optimality_trace.json"
                ).read_text(encoding="utf-8")
            )
            for option in trace.get("options", []):
                trade = option.get("tradeoff", {})
                for key in ["cost", "benefit", "risk", "uncertainty"]:
                    if not isinstance(trade.get(key), (int, float)):
                        failures.append("tradeoff_not_quantified")
                        break
        except Exception:
            failures.append("optimality_artifact_invalid")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class OptionSalienceInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("OptionSalienceInvariant", "10", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        task_id = context.get("optimality_task_id")
        root = context.get("root_path")
        if not task_id:
            return ValidationResult(self.name, passed=True, failures=[], metadata={})
        failures: list[str] = []
        try:
            trace = json.loads(
                (
                    Path(str(root))
                    / "optimality_artifacts"
                    / str(task_id)
                    / "optimality_trace.json"
                ).read_text(encoding="utf-8")
            )
            signatures = {
                (
                    str(option.get("approach_key", "")),
                    tuple(sorted(str(m) for m in option.get("touched_modules", []))),
                )
                for option in trace.get("options", [])
            }
            if len(signatures) < 5:
                failures.append("option_salience_failure")
        except Exception:
            failures.append("optimality_artifact_invalid")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class DecisionImprovementInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("DecisionImprovementInvariant", "10", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        task_id = context.get("optimality_task_id")
        root = context.get("root_path")
        if not task_id:
            return ValidationResult(self.name, passed=True, failures=[], metadata={})
        history_path = (
            Path(str(root)) / "optimality_artifacts" / "decision_history.jsonl"
        )
        if not history_path.exists():
            return ValidationResult(self.name, passed=True, failures=[], metadata={})
        regrets: list[float] = []
        for line in history_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            item = json.loads(line)
            if item.get("task_id") == task_id:
                regrets.append(float(item.get("regret", 0.0)))
        failures: list[str] = []
        if len(regrets) >= 2 and regrets[-1] > regrets[0]:
            failures.append("decision_improvement_failure")
        return ValidationResult(
            self.name, passed=not failures, failures=failures, metadata={}
        )


class ImplementationRealityInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__("ImplementationRealityInvariant", "10", "HARD", [])

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        payload = context.get("implementation_reality", {})
        classification = payload.get("classification", {})
        task_type = str(payload.get("task_type", "maintenance"))
        failures: list[str] = []
        structured: list[dict[str, str]] = []
        feature_like = task_type in {"feature", "enforcement", "runtime", "verifier"}

        substantive = bool(
            classification.get("semantic_code")
            or classification.get("schema_or_contract")
            or classification.get("runtime_enforcement")
            or classification.get("verifier_enforcement")
            or classification.get("export_audit_surface")
        )
        if feature_like and not substantive:
            failures.append("implementation_non_substantive")
            structured.append(_structured("implementation_non_substantive"))
        if feature_like and bool(classification.get("tests_only")):
            failures.append("implementation_missing_test_evidence")
            structured.append(_structured("implementation_missing_test_evidence"))

        return ValidationResult(
            self.name,
            passed=not failures,
            failures=failures,
            metadata={"structured_failures": structured},
        )


class ClaimConsistencyInvariant(Invariant):
    def __init__(self) -> None:
        super().__init__(
            "ClaimConsistencyInvariant",
            "10",
            "HARD",
            ["ImplementationRealityInvariant"],
        )

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        payload = context.get("implementation_reality", {})
        classification = payload.get("classification", {})
        claim = payload.get("claim", {})
        pre_assessed = payload.get("claim_assessment", {})
        failures: list[str] = []
        structured: list[dict[str, str]] = []
        if claim:
            assessed = assess_claim_consistency(
                classification=DiffClassification(
                    changed_files=list(classification.get("changed_files", [])),
                    formatting_only=bool(classification.get("formatting_only", False)),
                    docs_only=bool(classification.get("docs_only", False)),
                    tests_only=bool(classification.get("tests_only", False)),
                    semantic_code=bool(classification.get("semantic_code", False)),
                    schema_or_contract=bool(
                        classification.get("schema_or_contract", False)
                    ),
                    runtime_enforcement=bool(
                        classification.get("runtime_enforcement", False)
                    ),
                    verifier_enforcement=bool(
                        classification.get("verifier_enforcement", False)
                    ),
                    export_audit_surface=bool(
                        classification.get("export_audit_surface", False)
                    ),
                    no_op=bool(classification.get("no_op", False)),
                ),
                title=str(claim.get("title", "")),
                summary=str(claim.get("summary", "")),
                expected_changed_surfaces=list(
                    claim.get("expected_changed_surfaces", [])
                ),
                claimed_capability=str(claim.get("claimed_capability", "")),
            )
            if not assessed.passed:
                failures.append("claim_inconsistent_with_diff")
                structured.append(_structured("claim_inconsistent_with_diff"))
                if any(
                    x in assessed.failures
                    for x in {
                        "claim_overstates_non_substantive_diff",
                        "claim_overstates_test_only_diff",
                        "feature_claim_requires_semantic_delta",
                    }
                ):
                    failures.append("claim_overstates_changes")
                    structured.append(_structured("claim_overstates_changes"))
        elif pre_assessed and not bool(pre_assessed.get("passed", False)):
            failures.append("claim_inconsistent_with_diff")
            structured.append(_structured("claim_inconsistent_with_diff"))

        return ValidationResult(
            self.name,
            passed=not failures,
            failures=failures,
            metadata={"structured_failures": structured},
        )


INVARIANT_REGISTRY: list[Invariant] = [
    MutationProofInvariant(),
    TraceConsistencyInvariant(),
    CounterfactualInvariant(),
    ProofChainInvariant(),
    ReplayDeterminismInvariant(),
    EventOrderInvariant(),
    RuntimeStateConsistencyInvariant(),
    StateHashBindingInvariant(),
    ProofRegistrySnapshotInvariant(),
    InvariantRegistryVersionInvariant(),
    ExportManifestInvariant(),
    MPPComplianceInvariant(),
    MPPHashInvariant(),
    BTSIntegrityInvariant(),
    BTSCompletenessInvariant(),
    BTSJustificationInvariant(),
    DecisionConsistencyInvariant(),
    OptimalityInvariant(),
    TradeoffQuantificationInvariant(),
    OptionSalienceInvariant(),
    DecisionImprovementInvariant(),
    ImplementationRealityInvariant(),
    ClaimConsistencyInvariant(),
]


def _normalize_canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _normalize_canonical(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [_normalize_canonical(item) for item in value]
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, float):
        return float(f"{value:.17g}")
    return value


def canonical_events_hash(events: list[dict[str, Any]]) -> str:
    normalized: list[dict[str, Any]] = []
    for event in events:
        filtered = {
            key: value
            for key, value in event.items()
            if key not in NON_SEMANTIC_EVENT_FIELDS
        }
        normalized.append(_normalize_canonical(filtered))
    canonical_stream = "\n".join(
        json.dumps(event, sort_keys=True, separators=(",", ":")) for event in normalized
    )
    return hashlib.sha256(canonical_stream.encode("utf-8")).hexdigest()


def _assert_structured_failure_contract(results: list[ValidationResult]) -> None:
    Draft202012Validator.check_schema(STRUCTURED_FAILURE_SCHEMA)
    validator = Draft202012Validator(STRUCTURED_FAILURE_SCHEMA)
    for result in results:
        for failure in result.metadata.get("structured_failures", []):
            try:
                validate(instance=failure, schema=STRUCTURED_FAILURE_SCHEMA)
            except JsonSchemaValidationError as exc:
                raise InvariantContractError(
                    f"invalid failure payload for {result.name}: {exc.message}"
                ) from exc
            if set(failure.keys()) != {"rule", "failure_class", "retry_class"}:
                raise InvariantContractError(
                    f"unexpected failure payload keys for {result.name}"
                )
            errors = list(validator.iter_errors(failure))
            if errors:
                raise InvariantContractError(
                    f"invalid failure payload for {result.name}: {errors[0].message}"
                )


def run_invariants(
    context: dict[str, Any], names: set[str] | None = None
) -> dict[str, Any]:
    before = deepcopy(context)
    selected = [inv for inv in INVARIANT_REGISTRY if names is None or inv.name in names]
    results = [inv.validate(context) for inv in selected]
    if context != before:
        raise InvariantContractError("invariant evaluation mutated context")
    _assert_structured_failure_contract(results)
    failures = [r for r in results if not r.passed]
    return {
        "invariants_checked": [r.name for r in results],
        "failures": [
            {
                "name": r.name,
                "failures": r.failures,
                "metadata": r.metadata,
            }
            for r in failures
        ],
        "passed": not failures,
        "results": results,
    }

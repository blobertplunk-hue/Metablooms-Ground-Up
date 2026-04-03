from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from src.review.diff_classifier import DiffClassification

FEATURE_VERB_PATTERN = re.compile(r"\b(add|implement|build|harden|introduce|deliver|create)\b", re.IGNORECASE)


@dataclass(frozen=True)
class ClaimAssessment:
    passed: bool
    failures: list[str]
    metadata: dict[str, Any]



def _has_feature_claim(title: str, summary: str) -> bool:
    return bool(FEATURE_VERB_PATTERN.search((title + " " + summary).strip()))


def assess_claim_consistency(
    classification: DiffClassification,
    title: str,
    summary: str,
    expected_changed_surfaces: list[str] | None = None,
    claimed_capability: str = "",
) -> ClaimAssessment:
    failures: list[str] = []
    expected = sorted(set(expected_changed_surfaces or []))
    actual = sorted(classification.changed_files)
    text = (title + "\n" + summary).lower()

    feature_claim = _has_feature_claim(title, summary)
    substantive = classification.semantic_code or classification.schema_or_contract

    if classification.no_op:
        failures.append("claim_noop_diff")
    if feature_claim and (classification.formatting_only or classification.docs_only):
        failures.append("claim_overstates_non_substantive_diff")
    if feature_claim and classification.tests_only:
        failures.append("claim_overstates_test_only_diff")
    if feature_claim and not substantive:
        failures.append("feature_claim_requires_semantic_delta")
    if "verifier" in text and not classification.verifier_enforcement:
        failures.append("claim_surface_mismatch_verifier")
    if any(k in text for k in ["runtime", "engine", "enforcement"]) and not classification.runtime_enforcement:
        failures.append("claim_surface_mismatch_runtime")
    if any(k in text for k in ["schema", "contract", "proof field"]) and not classification.schema_or_contract:
        failures.append("claim_surface_mismatch_contract")

    if expected and not set(expected).issubset(set(actual)):
        failures.append("expected_surfaces_not_changed")

    if claimed_capability and feature_claim and claimed_capability.lower() not in text:
        failures.append("claimed_capability_not_present_in_claim_text")

    return ClaimAssessment(
        passed=not failures,
        failures=failures,
        metadata={
            "feature_claim": feature_claim,
            "expected_changed_surfaces": expected,
            "actual_changed_surfaces": actual,
            "claimed_capability": claimed_capability,
        },
    )

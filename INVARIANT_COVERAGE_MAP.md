# Invariant Coverage Map

This map is the canonical audit index from critical integrity property to exactly one enforcing invariant.

| Critical property | Enforcing invariant |
|---|---|
| Mutation proof required + schema + delta consistency + artifact presence | `MutationProofInvariant` |
| Execution claim must have evidence + cross-artifact causal binding | `TraceConsistencyInvariant` |
| Counterfactual distinguishability | `CounterfactualInvariant` |
| Proof registry append-only chain, hash linkage, entry-hash integrity, duplicate trace prevention | `ProofChainInvariant` |
| Canonical events hash binding (proof vs computed) | `ReplayDeterminismInvariant` |
| Event sequence/order integrity surfaced to registry | `EventOrderInvariant` |
| Persisted runtime state equals replay-derived state | `RuntimeStateConsistencyInvariant` |
| Proof-bound replayed state hash equality (`state_hash_after`) | `StateHashBindingInvariant` |
| Proof-registry rolling snapshot hash consistency | `ProofRegistrySnapshotInvariant` |
| Proof entry invariant registry compatibility | `InvariantRegistryVersionInvariant` |
| Export manifest integrity (when present) | `ExportManifestInvariant` |
| MPP stages 1–10 compliance artifacts are complete and policy-consistent when required | `MPPComplianceInvariant` |
| Reasoning integrity (canonical MPP artifacts hashed + proof-bound) | `MPPHashInvariant` |
| Reasoning provenance integrity (canonical BTS trace hashed + proof-bound) | `BTSIntegrityInvariant` |
| Decision space exploration completeness and diversity | `BTSCompletenessInvariant` |
| Rejected-option and criteria-linked decision justification quality | `BTSJustificationInvariant` |
| Decision consistency between options, scores, and selected outcome | `DecisionConsistencyInvariant` |
| Decision optimality (chosen option must be argmax with bounded regret) | `OptimalityInvariant` |
| Quantified tradeoffs (cost/benefit/risk/uncertainty) | `TradeoffQuantificationInvariant` |
| Option salience (non-trivial strategic diversity) | `OptionSalienceInvariant` |
| Decision improvement trend over time | `DecisionImprovementInvariant` |
| Feature-implementation tasks require substantive non-no-op deltas | `ImplementationRealityInvariant` |
| Claim text must not overstate or mismatch changed surfaces | `ClaimConsistencyInvariant` |

## Non-overlap policy

- Each critical property above MUST be enforced by exactly one invariant in `src/invariants.py`.
- Adding a new critical property requires adding one new row to this map.
- Duplicating enforcement across invariants is disallowed unless the map is updated with an explicit rationale.

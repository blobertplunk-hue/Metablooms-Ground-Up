# Coding Done Right (CDR) v2.1 Specification

## 1. Purpose
CDR v2.1 is the build-governance layer for Stage 2. It hardens build-time controls without changing Stage 2 execution semantics.

## 2. Pre-Code Governance
### 2.1 Design Justification
Changes must be readable by an external engineer and explain why enforcement is required before runtime mutation.

### 2.2 Constraint Mapping
Build controls must map to prior phase constraints:
- deterministic replay
- fail-closed write model
- canonical-root isolation
- schema-first validation

## 3. Build Stages
1. **CDR-RAT**: rationale and constraints are documented.
2. **CDR-ARCH**: structure and control surfaces are defined (no business logic changes).
3. **CDR-CODE**: implementation under fail-closed governance.
4. **CDR-LINT**: self-check loop verifies CDR markers and structural rules.

## 4. Seven Pillars
1. Dependency Isolation
2. Rationale Headers
3. Fail-Closed Error Handling
4. Schema Enforcement
5. Auditability
6. Minimalist Logic
7. Recovery Path

## 5. Enforcement Boundary
CDR is enforced as semantic markers + structural checks executed before writes.

## 6. Control Files
- `CDR_SECURITY.md`
- `CDR_VERIFICATION.md`
- `CDR_OBSERVABILITY.md`
- `CDR_LIFECYCLE.md`

## 7. Runtime Compatibility
CDR must not weaken deterministic replay or fail-closed guarantees.

## 8. External Audit
### TRADEOFFS
- Marker-based checks prioritize deterministic enforcement over full natural-language parsing.
- Structural static checks are lightweight and intentionally conservative.

### FUTURE_GAPS
- Optional AST-based static analysis for richer security/observability checks.
- Optional policy-as-code integration for external compliance tooling.

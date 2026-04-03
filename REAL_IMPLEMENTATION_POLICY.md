# Real Implementation Policy

## Classification

### A) Non-substantive
- **formatting_only**: whitespace/line wrapping/import formatting with no Python AST semantic delta.
- **docs_only**: only markdown/text/docs paths changed.
- **tests_only**: only `tests/**` or `test_*.py` changed.
- **no_op**: no changed files.

### B) Substantive implementation
Any diff that includes one or more of:
- `semantic_code`: behavior-affecting code delta.
- `schema_or_contract`: schema/contract changes (`*_SCHEMA.json`, proof/workflow contracts).
- `runtime_enforcement`: runtime fail-closed enforcement surface changed.
- `verifier_enforcement`: independent verifier or invariant enforcement changed.
- `export_audit_surface`: export/audit semantics changed.

## Feature-claim threshold
A claim using verbs like **add/implement/build/harden/introduce/deliver/create** must include:
1. at least one substantive classification (`semantic_code` or `schema_or_contract`), and
2. matching changed surfaces for the claimed capability, and
3. corresponding test evidence for feature tasks.

## Fail-closed rules
- feature claim + formatting/docs/test-only diff => FAIL.
- no-op diff => PR generation blocked.
- claim text that references verifier/runtime/schema without touching those surfaces => FAIL.
- expected changed surfaces (from claim artifact) not present in actual diff => FAIL.

## Operational messaging rules
- formatting-only changes must be labeled formatting-only.
- tests-only changes must be labeled as test/audit/coverage work.
- feature claims are rejected unless semantic evidence exists in the diff.

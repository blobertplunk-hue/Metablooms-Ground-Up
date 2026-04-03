# Changelog

## v1.0.0 - 2026-04-02

### Guarantees
- Deterministic replay + canonical hashing.
- Tamper-evident proof-chain with snapshot binding.
- Versioned invariant registry with compatibility checks.
- Deterministic export bundle with manifest and release signature validation.
- Independent verifier with continuous audit mode.

### Breaking changes
- Proof entries now require `invariant_registry_version` and export manifest/signature expectations when present.

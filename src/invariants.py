from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol, Sequence


@dataclass(frozen=True)
class ValidationResult:
    name: str
    failures: list[dict[str, str]]
    warnings: list[str]
    details: dict[str, Any]

    @property
    def fail(self) -> bool:
        return bool(self.failures)


class Invariant(Protocol):
    @property
    def name(self) -> str: ...

    def validate(self, context: dict[str, Any]) -> ValidationResult: ...


@dataclass(frozen=True)
class FunctionInvariant:
    name: str
    fn: Callable[[dict[str, Any]], ValidationResult]

    def validate(self, context: dict[str, Any]) -> ValidationResult:
        return self.fn(context)


def run_all_invariants(
    context: dict[str, Any], registry: Sequence[Invariant]
) -> list[ValidationResult]:
    return [inv.validate(context) for inv in registry]

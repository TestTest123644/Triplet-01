"""Guardian Layer core package."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from guardian_layer.orchestrator import GuardianOrchestrator, Orchestrator

__all__ = ["GuardianOrchestrator", "Orchestrator"]


def __getattr__(name: str):
    if name == "GuardianOrchestrator":
        from guardian_layer.orchestrator import GuardianOrchestrator

        return GuardianOrchestrator
    if name == "Orchestrator":
        from guardian_layer.orchestrator import Orchestrator

        return Orchestrator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

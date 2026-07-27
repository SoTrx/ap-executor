"""Everything needed to actually invoke a resolved operator."""
from __future__ import annotations

from dataclasses import dataclass

from .manifest import OperatorManifest
from .registry import ServiceInstance


@dataclass
class ResolvedOperator:
    """Everything needed to actually invoke a resolved operator."""
    instance: ServiceInstance
    manifest: OperatorManifest

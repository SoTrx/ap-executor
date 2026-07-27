import re
from abc import ABC, abstractmethod
from typing import Optional

from .service_instance import ServiceInstance


class OperatorRegistry(ABC):
    """Resolves a logical operator name(+version) to a healthy service instance."""

    @abstractmethod
    async def resolve_operator(
        self, name: str, version: Optional[str] = None) -> ServiceInstance: ...

    def normalize_op_name(self, name: str) -> str:
        """Normalize an operator name to a slug format."""
        _SLUG_RE = re.compile(r"[^a-z0-9]+")
        return _SLUG_RE.sub("-", name.strip().lower()).strip("-")

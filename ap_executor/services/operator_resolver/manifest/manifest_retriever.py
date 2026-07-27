"""Fetches and parses an operator's `/.well-known/operator.json` manifest."""
from typing import Protocol

from .manifest import OperatorManifest


class ManifestRetriever(Protocol):
    """Fetches the manifest exposed by a resolved operator service instance."""

    async def fetch(self, url: str) -> OperatorManifest: ...

"""Resolves an AP graph's logical operator reference to something invokable."""
from typing import Optional

from ap_executor.services.operator_resolver.manifest.manifest_retriever import (
    ManifestRetriever,
)
from ap_executor.services.operator_resolver.registry.registry import OperatorRegistry
from ap_executor.services.operator_resolver.resolved_operator import ResolvedOperator


class OperatorResolver:
    """Resolves an operator name(+version) to something invokable.

    Depends on a registry (name/version -> `ServiceInstance`) and a manifest
    fetcher (`ServiceInstance` -> `OperatorManifest`); picks the execution
    strategy from the manifest's declared mode/protocol.
    """

    def __init__(self, registry: OperatorRegistry, manifest_fetcher: ManifestRetriever):
        self._registry = registry
        self._manifest_fetcher = manifest_fetcher

    async def resolve(self, op_name: str, op_version: Optional[str] = None) -> ResolvedOperator:
        instance = await self._registry.resolve_operator(op_name, op_version)
        manifest = await self._manifest_fetcher.fetch(instance.base_url)

        return ResolvedOperator(instance=instance, manifest=manifest)

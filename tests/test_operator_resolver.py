"""Unit tests for `OperatorResolver` — no HTTP/ASGI app involved at all.

Both dependencies (`OperatorRegistry`, `ManifestFetcher`) are satisfied by
bare Python fakes structurally, the same way `HttpSyncExecutionStrategy`
satisfies `OperatorExecutionStrategy` today — proving the "replace the
ConsulRegistry in testing" seam works standalone. Strategy selection isn't
`OperatorResolver`'s job — it just resolves the instance + manifest; the
caller (see `workflows/activities.py`) picks the strategy from those.
"""
from typing import Optional

import pytest

from ap_executor.services.operator_resolver.errors import UnsupportedOperatorError
from ap_executor.services.operator_resolver import OperatorResolver
from ap_executor.services.operator_resolver.manifest import OperatorManifest
from ap_executor.services.operator_resolver.registry import ServiceInstance

MANIFEST_JSON = {
    "manifest_version": "0.1.0", "operator": "Text to SQL", "version": "1.0.0",
    "execution": {"mode": "sync", "protocol": "http", "endpoint": "/execute"},
    "inputs": [{"name": "nl", "type": "string", "required": True, "default": ""}],
    "outputs": [{"name": "query", "type": "string", "required": True, "default": ""}],
}


class _FakeRegistry:
    def __init__(self, instance: ServiceInstance):
        self._instance = instance
        self.calls = []

    async def resolve_operator(self, name: str, version: Optional[str] = None) -> ServiceInstance:
        self.calls.append((name, version))
        return self._instance


class _FakeManifestFetcher:
    def __init__(self, manifest: OperatorManifest):
        self._manifest = manifest
        self.calls = []

    async def fetch(self, url: str) -> OperatorManifest:
        self.calls.append(url)
        return self._manifest


@pytest.mark.asyncio
async def test_resolve_bundles_instance_and_manifest():
    instance = ServiceInstance(service_id="x", service_name="text-to-sql", address="op", port=8080)
    manifest = OperatorManifest.model_validate(MANIFEST_JSON)
    registry = _FakeRegistry(instance)
    manifest_fetcher = _FakeManifestFetcher(manifest)
    resolver = OperatorResolver(registry, manifest_fetcher)

    resolved = await resolver.resolve("Text to SQL", "1.0.0")

    assert resolved.instance == instance
    assert resolved.manifest == manifest
    assert registry.calls == [("Text to SQL", "1.0.0")]
    assert manifest_fetcher.calls == [instance.base_url]


@pytest.mark.asyncio
async def test_resolve_propagates_registry_errors():
    class _RaisingRegistry:
        async def resolve_operator(self, name, version=None):
            raise UnsupportedOperatorError(f"{name}@{version or 'any'}")

    resolver = OperatorResolver(_RaisingRegistry(), _FakeManifestFetcher(OperatorManifest.model_validate(MANIFEST_JSON)))

    with pytest.raises(UnsupportedOperatorError):
        await resolver.resolve("Unknown Operator")

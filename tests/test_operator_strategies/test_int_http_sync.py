"""Integration test for the sync HTTP execution strategy.

Spins up a real dummy operator (a FastAPI app served over ASGITransport),
fetches its manifest the same way the executor does, and drives the sync
strategy end-to-end.
"""
import httpx
import pytest
from fastapi import HTTPException

from ap_executor.services.executor.errors import OperatorExecutionError
from ap_executor.services.executor.strategies import ExecutionStrategyFactory
from ap_executor.services.operator_resolver.manifest import HttpManifestRetriever
from ap_executor.services.operator_resolver.registry import ServiceInstance

MANIFEST = {
    "manifest_version": "0.1.0",
    "operator": "Text to SQL",
    "version": "1.0.0",
    "execution": {"mode": "sync", "protocol": "http", "endpoint": "/execute"},
    "inputs": [{"name": "nl", "type": "string", "required": True, "default": ""}],
    "outputs": [{"name": "query", "type": "string", "required": True, "default": ""}],
}

INSTANCE = ServiceInstance(
    service_id="text-to-sql-1", service_name="text-to-sql", address="text-to-sql", port=8080
)


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://text-to-sql:8080"
    )


@pytest.mark.asyncio
async def test_sync_operator_executes_end_to_end(operator_apps):
    app = operator_apps.sync(MANIFEST, lambda inp: {
                             "query": f"SELECT '{inp['nl']}'"})

    async with _client(app) as http:
        manifest = await HttpManifestRetriever(http).fetch(INSTANCE.base_url)
        strategy = ExecutionStrategyFactory.create(
            manifest.execution.mode, manifest.execution.protocol)
        handle = await strategy.start(http, INSTANCE, manifest, {"nl": "all users"})

    assert manifest.execution.mode == "sync"
    assert handle.done is True
    assert handle.success is True
    assert handle.output == {"query": "SELECT 'all users'"}


@pytest.mark.asyncio
async def test_sync_operator_error_raises(operator_apps):
    def _boom(_inp):
        raise HTTPException(status_code=500, detail="operator blew up")

    app = operator_apps.sync(MANIFEST, _boom)

    async with _client(app) as http:
        manifest = await HttpManifestRetriever(http).fetch(INSTANCE.base_url)
        strategy = ExecutionStrategyFactory.create(
            manifest.execution.mode, manifest.execution.protocol)
        with pytest.raises(OperatorExecutionError):
            await strategy.start(http, INSTANCE, manifest, {"nl": "boom"})

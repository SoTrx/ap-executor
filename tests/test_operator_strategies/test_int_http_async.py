"""Integration test for the async start+poll HTTP execution strategy.

Spins up a real dummy async operator (a FastAPI app served over ASGITransport)
that starts a job, reports ``running`` once, then ``done`` with a result, and
drives the async strategy through to completion.
"""
import httpx
import pytest

from ap_executor.services.executor.strategies import ExecutionStrategyFactory
from ap_executor.services.operator_resolver.manifest import HttpManifestRetriever
from ap_executor.services.operator_resolver.registry import ServiceInstance

MANIFEST = {
    "manifest_version": "0.1.0",
    "operator": "Long Running Op",
    "version": "1.0.0",
    "execution": {
        "mode": "async",
        "protocol": "http",
        "start_endpoint": "/jobs",
        "poll_endpoint": "/jobs/{id}",
    },
    "inputs": [{"name": "payload", "type": "string", "required": True, "default": ""}],
    "outputs": [{"name": "rows", "type": "number", "required": True, "default": 0}],
}

INSTANCE = ServiceInstance(
    service_id="long-running-op-1", service_name="long-running-op", address="long-op", port=9090
)


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://long-op:9090")


@pytest.mark.asyncio
async def test_async_operator_polls_to_completion(operator_apps):
    app = operator_apps.async_(MANIFEST, {"rows": 42}, running_polls=1)

    async with _client(app) as http:
        manifest = await HttpManifestRetriever(http).fetch(INSTANCE.base_url)
        strategy = ExecutionStrategyFactory.create(
            manifest.execution.mode, manifest.execution.protocol)

        handle = await strategy.start(http, INSTANCE, manifest, {"payload": "go"})
        assert manifest.execution.mode == "async"
        assert handle.done is False
        assert handle.poll_endpoint == "http://long-op:9090/jobs/job-1"

        # First poll: still running.
        handle = await strategy.poll(http, handle)
        assert handle.done is False

        # Second poll: terminal success with the operator's result.
        handle = await strategy.poll(http, handle)
        assert handle.done is True
        assert handle.success is True
        assert handle.output == {"rows": 42}

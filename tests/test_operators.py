"""Unit tests for operator discovery (Consul), contract (manifest), and execution strategies."""
import httpx
import pytest
import yaml

from ap_executor.services.executor.errors import OperatorExecutionError
from ap_executor.services.operator_resolver.errors import UnsupportedOperatorError
from ap_executor.services.executor.execution_handle import ExecutionHandle
from ap_executor.services.executor.strategies import ExecutionStrategyFactory
from ap_executor.services.executor.strategies.http_async_polling import (
    HttpAsyncPollingExecutionStrategy,
)
from ap_executor.services.executor.strategies.http_sync import HttpSyncExecutionStrategy
from ap_executor.services.operator_resolver.manifest import (
    HttpManifestRetriever,
    OperatorManifest,
)
from ap_executor.services.operator_resolver.registry import ServiceInstance
from ap_executor.services.operator_resolver.registry.consul_registry import (
    ConsulRegistryClient,
)

CONSUL_ADDR = "http://consul.test:8500"


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# --- ConsulRegistryClient -----------------------------------------------------

def test_normalize_op_name():
    registry = ConsulRegistryClient(http=None, consul_addr=CONSUL_ADDR)
    assert registry.normalize_op_name("Text to SQL") == "text-to-sql"
    assert registry.normalize_op_name("  Weird__Name!!") == "weird-name"


@pytest.mark.asyncio
async def test_resolve_operator_returns_healthy_instance():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/health/service/text-to-sql"
        assert request.url.params["passing"] == "true"
        return httpx.Response(200, json=[
            {
                "Service": {"ID": "text-to-sql-1", "Service": "text-to-sql",
                            "Address": "10.0.0.4", "Port": 8080, "Meta": {"version": "1.0.0"}},
                "Node": {"Address": "10.0.0.4"},
            }
        ])

    async with _client(handler) as http:
        registry = ConsulRegistryClient(http, CONSUL_ADDR)
        instance = await registry.resolve_operator("Text to SQL")

    assert instance == ServiceInstance(
        service_id="text-to-sql-1", service_name="text-to-sql",
        address="10.0.0.4", port=8080, meta={"version": "1.0.0"},
    )
    assert instance.base_url == "http://10.0.0.4:8080"


@pytest.mark.asyncio
async def test_resolve_operator_filters_by_version():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["filter"] == 'Service.Meta.version == "2.0.0"'
        return httpx.Response(200, json=[
            {"Service": {"ID": "x", "Service": "text-to-sql", "Address": "10.0.0.5",
                         "Port": 80, "Meta": {}}, "Node": {"Address": "10.0.0.5"}}
        ])

    async with _client(handler) as http:
        registry = ConsulRegistryClient(http, CONSUL_ADDR)
        await registry.resolve_operator("Text to SQL", version="2.0.0")


@pytest.mark.asyncio
async def test_resolve_operator_raises_when_no_healthy_instance():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    async with _client(handler) as http:
        registry = ConsulRegistryClient(http, CONSUL_ADDR)
        with pytest.raises(UnsupportedOperatorError):
            await registry.resolve_operator("Unknown Operator")


# --- manifest -------------------------------------------------------------

SYNC_MANIFEST_JSON = {
    "manifest_version": "0.1.0",
    "operator": "Text to SQL",
    "version": "1.0.0",
    "execution": {"mode": "sync", "protocol": "http", "endpoint": "/execute"},
}

ASYNC_MANIFEST_JSON = {
    "manifest_version": "0.1.0",
    "operator": "Long Running Op",
    "version": "1.0.0",
    "execution": {
        "mode": "async", "protocol": "http",
        "start_endpoint": "/jobs", "poll_endpoint": "/jobs/{id}",
    },
}


@pytest.mark.asyncio
async def test_fetch_manifest_parses_sync_spec():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/.well-known/operator.yaml"
        return httpx.Response(200, text=yaml.safe_dump(SYNC_MANIFEST_JSON), headers={"content-type": "application/yaml"})

    instance = ServiceInstance(service_id="x", service_name="text-to-sql",
                               address="10.0.0.4", port=8080)
    async with _client(handler) as http:
        manifest = await HttpManifestRetriever(http).fetch(instance.base_url)

    assert manifest.execution.mode == "sync"
    assert manifest.execution.endpoint == "/execute"


@pytest.mark.asyncio
async def test_fetch_manifest_parses_async_spec():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=yaml.safe_dump(ASYNC_MANIFEST_JSON), headers={"content-type": "application/yaml"})

    instance = ServiceInstance(service_id="x", service_name="long-running-op",
                               address="10.0.0.4", port=8080)
    async with _client(handler) as http:
        manifest = await HttpManifestRetriever(http).fetch(instance.base_url)

    assert manifest.execution.mode == "async"
    assert manifest.execution.poll_endpoint == "/jobs/{id}"


# --- execution strategies ----------------------------------------------------

@pytest.mark.asyncio
async def test_http_sync_strategy_start_success():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/execute"
        return httpx.Response(200, json={"query": "SELECT 1"})

    manifest = OperatorManifest.model_validate(SYNC_MANIFEST_JSON)
    instance = ServiceInstance(service_id="x", service_name="text-to-sql",
                               address="10.0.0.4", port=8080)
    strategy = HttpSyncExecutionStrategy()
    async with _client(handler) as http:
        handle = await strategy.start(http, instance, manifest, {"nl": "count rows"})

    assert handle.done is True
    assert handle.success is True
    assert handle.output == {"query": "SELECT 1"}


@pytest.mark.asyncio
async def test_http_sync_strategy_start_error_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    manifest = OperatorManifest.model_validate(SYNC_MANIFEST_JSON)
    instance = ServiceInstance(service_id="x", service_name="text-to-sql",
                               address="10.0.0.4", port=8080)
    strategy = HttpSyncExecutionStrategy()
    async with _client(handler) as http:
        with pytest.raises(OperatorExecutionError):
            await strategy.start(http, instance, manifest, {})


@pytest.mark.asyncio
async def test_http_async_polling_strategy_start_then_poll_to_completion():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/jobs":
            return httpx.Response(200, json={"id": "job-1"})
        if len(calls) == 2:
            return httpx.Response(200, json={"status": "running"})
        return httpx.Response(200, json={"status": "done", "result": {"rows": 42}})

    manifest = OperatorManifest.model_validate(ASYNC_MANIFEST_JSON)
    instance = ServiceInstance(service_id="x", service_name="long-running-op",
                               address="10.0.0.4", port=8080)
    strategy = HttpAsyncPollingExecutionStrategy()
    async with _client(handler) as http:
        handle = await strategy.start(http, instance, manifest, {})
        assert handle.done is False
        assert handle.poll_endpoint == "http://10.0.0.4:8080/jobs/job-1"

        handle = await strategy.poll(http, handle)
        assert handle.done is False

        handle = await strategy.poll(http, handle)
        assert handle.done is True
        assert handle.success is True
        assert handle.output == {"rows": 42}


def test_execution_strategy_factory_known_modes():
    assert isinstance(ExecutionStrategyFactory.create(
        "sync", "http"), HttpSyncExecutionStrategy)
    assert isinstance(ExecutionStrategyFactory.create("async", "http"),
                      HttpAsyncPollingExecutionStrategy)


def test_execution_strategy_factory_unknown_raises():
    with pytest.raises(UnsupportedOperatorError):
        ExecutionStrategyFactory.create("sync", "grpc")

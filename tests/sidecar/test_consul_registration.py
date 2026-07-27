"""Unit tests for the write-side Consul registration client."""
import json

import httpx
import pytest

from sidecar.consul_registration import ConsulRegistrationClient
from sidecar.errors import ConsulRegistrationError

CONSUL_ADDR = "http://consul.test:8500"


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_register_sends_expected_payload():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        assert request.url.path == "/v1/agent/service/register"
        assert json.loads(request.content) == {
            "ID": "text-to-sql-1",
            "Name": "text-to-sql",
            "Address": "10.0.0.4",
            "Port": 8000,
            "Meta": {"version": "1.0.0"},
            "Check": {
                "HTTP": "http://10.0.0.4:8000/health",
                "Interval": "10s",
                "Timeout": "5s",
                "DeregisterCriticalServiceAfter": "1m",
            },
        }
        return httpx.Response(200)

    async with _client(handler) as http:
        client = ConsulRegistrationClient(http, CONSUL_ADDR)
        await client.register(
            service_id="text-to-sql-1",
            service_name="text-to-sql",
            address="10.0.0.4",
            port=8000,
            version="1.0.0",
            check_url="http://10.0.0.4:8000/health",
        )


@pytest.mark.asyncio
async def test_register_raises_on_error_response():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    async with _client(handler) as http:
        client = ConsulRegistrationClient(http, CONSUL_ADDR)
        with pytest.raises(ConsulRegistrationError):
            await client.register(
                service_id="x", service_name="x", address="a", port=1,
                version="1.0.0", check_url="http://a:1/health",
            )


@pytest.mark.asyncio
async def test_register_raises_on_transport_error():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    async with _client(handler) as http:
        client = ConsulRegistrationClient(http, CONSUL_ADDR)
        with pytest.raises(ConsulRegistrationError):
            await client.register(
                service_id="x", service_name="x", address="a", port=1,
                version="1.0.0", check_url="http://a:1/health",
            )


@pytest.mark.asyncio
async def test_deregister_sends_expected_request():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        assert request.url.path == "/v1/agent/service/deregister/text-to-sql-1"
        return httpx.Response(200)

    async with _client(handler) as http:
        client = ConsulRegistrationClient(http, CONSUL_ADDR)
        await client.deregister("text-to-sql-1")


@pytest.mark.asyncio
async def test_deregister_raises_on_error_response():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="unavailable")

    async with _client(handler) as http:
        client = ConsulRegistrationClient(http, CONSUL_ADDR)
        with pytest.raises(ConsulRegistrationError):
            await client.deregister("x")

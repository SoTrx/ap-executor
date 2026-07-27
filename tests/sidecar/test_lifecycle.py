"""Register-on-startup / deregister-on-shutdown lifecycle."""
import httpx
import pytest
from fastapi.testclient import TestClient

from conftest import base_env
from sidecar.app import create_app
from sidecar.config import load_config


def _recording_consul_handler(calls):
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        return httpx.Response(200)
    return handler


def _unused_proxy_transport(request: httpx.Request) -> httpx.Response:
    raise AssertionError("proxy should not be used in lifecycle tests")


def test_register_then_deregister_in_order():
    calls = []
    config = load_config(base_env())
    app = create_app(
        config,
        consul_http_factory=lambda cfg: httpx.AsyncClient(
            transport=httpx.MockTransport(_recording_consul_handler(calls))),
        proxy_http_factory=lambda cfg: httpx.AsyncClient(transport=httpx.MockTransport(_unused_proxy_transport)),
    )

    with TestClient(app):
        assert calls == [("PUT", "/v1/agent/service/register")]

    assert calls == [
        ("PUT", "/v1/agent/service/register"),
        ("PUT", f"/v1/agent/service/deregister/{config.service_id}"),
    ]


def test_startup_fails_if_register_fails():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    config = load_config(base_env())
    app = create_app(
        config,
        consul_http_factory=lambda cfg: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        proxy_http_factory=lambda cfg: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(Exception):
        with TestClient(app):
            pass


def test_shutdown_swallows_deregister_failure():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT" and request.url.path.startswith("/v1/agent/service/deregister"):
            return httpx.Response(500, text="boom")
        return httpx.Response(200)

    config = load_config(base_env())
    app = create_app(
        config,
        consul_http_factory=lambda cfg: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        proxy_http_factory=lambda cfg: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    with TestClient(app):
        pass  # must not raise on exit despite the deregister call failing

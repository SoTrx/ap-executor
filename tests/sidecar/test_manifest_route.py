"""The manifest route is served directly from config -- never proxied, never
fetched from the upstream operator."""
import httpx
from fastapi.testclient import TestClient

from conftest import base_env
from sidecar.app import create_app
from sidecar.config import load_config


def _ok_consul_transport(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200)


def _unreachable_proxy_transport(request: httpx.Request) -> httpx.Response:
    raise AssertionError(f"proxy should not have been called for {request.url}")


def test_manifest_served_from_config_not_proxied():
    config = load_config(base_env())
    app = create_app(
        config,
        consul_http_factory=lambda cfg: httpx.AsyncClient(transport=httpx.MockTransport(_ok_consul_transport)),
        proxy_http_factory=lambda cfg: httpx.AsyncClient(transport=httpx.MockTransport(_unreachable_proxy_transport)),
    )

    with TestClient(app) as client:
        resp = client.get("/.well-known/operator.json")

    assert resp.status_code == 200
    assert resp.json() == config.manifest.model_dump(mode="json")

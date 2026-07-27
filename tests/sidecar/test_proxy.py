"""Catch-all proxy: everything except the manifest path is forwarded to the
real operator backend, streaming method/path/query/headers/body/status
through unchanged."""
import httpx
from fastapi.testclient import TestClient

from conftest import base_env
from sidecar.app import create_app
from sidecar.config import load_config


def _ok_consul_transport(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200)


def _build_app(upstream_app):
    config = load_config(base_env())
    return create_app(
        config,
        consul_http_factory=lambda cfg: httpx.AsyncClient(transport=httpx.MockTransport(_ok_consul_transport)),
        proxy_http_factory=lambda cfg: httpx.AsyncClient(
            transport=httpx.ASGITransport(app=upstream_app), base_url=cfg.upstream_base_url,
        ),
    )


def test_health_check_path_falls_through_to_upstream_health(dummy_upstream_app):
    app = _build_app(dummy_upstream_app)
    with TestClient(app) as client:
        resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_execute_body_is_forwarded_and_echoed(dummy_upstream_app):
    app = _build_app(dummy_upstream_app)
    with TestClient(app) as client:
        resp = client.post("/execute", json={"nl": "count rows"})
    assert resp.status_code == 200
    assert resp.json() == {"query": "SELECT * FROM students WHERE count rows"}


def test_query_params_and_headers_are_forwarded(dummy_upstream_app):
    app = _build_app(dummy_upstream_app)
    with TestClient(app) as client:
        resp = client.get("/echo/anything", params={"a": "1"}, headers={"X-Custom": "value"})
    body = resp.json()
    assert body["path"] == "/echo/anything"
    assert body["query"] == {"a": "1"}
    assert body["headers"].get("x-custom") == "value"


def test_upstream_error_status_relayed(dummy_upstream_app):
    app = _build_app(dummy_upstream_app)
    with TestClient(app) as client:
        resp = client.get("/does-not-exist")
    assert resp.status_code == 404

"""Shared fixtures/helpers for the sidecar's tests."""
from typing import Any, Dict

import pytest
from fastapi import FastAPI, Request

DEFAULT_MANIFEST_YAML = """\
manifest_version: "0.1.0"
operator: Text to SQL
version: "1.0.0"
execution:
  mode: sync
  protocol: http
  endpoint: /execute
inputs:
  - name: nl
    type: string
    required: true
outputs:
  - name: query
    type: string
    required: true
"""


def write_manifest(tmp_path, content: str = DEFAULT_MANIFEST_YAML, filename: str = "manifest.yaml") -> str:
    """Writes `content` to a real file under pytest's `tmp_path` and returns
    its path -- mirrors how `OPERATOR_MANIFEST_PATH` is actually consumed in
    production (a mounted file), rather than an in-memory shortcut."""
    path = tmp_path / filename
    path.write_text(content)
    return str(path)


def base_env(tmp_path, **overrides: str) -> Dict[str, str]:
    """A minimal, valid env mapping for `sidecar.config.load_config`; individual
    tests override/delete keys to exercise validation failures."""
    env = {
        "SIDECAR_SERVICE_NAME": "Text to SQL",
        "SIDECAR_ADVERTISE_ADDRESS": "sidecar.test",
        "UPSTREAM_PORT": "9000",
        "OPERATOR_MANIFEST_PATH": write_manifest(tmp_path),
    }
    env.update(overrides)
    return env


def _dummy_upstream_app() -> FastAPI:
    """A dummy real operator backend for the sidecar to proxy to."""
    app = FastAPI()

    @app.get("/health")
    async def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.post("/execute")
    async def execute(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"query": f"SELECT * FROM students WHERE {payload['nl']}"}

    @app.api_route("/echo/{rest:path}", methods=["GET", "POST"])
    async def echo(rest: str, request: Request) -> Dict[str, Any]:
        return {
            "method": request.method,
            "path": request.url.path,
            "query": dict(request.query_params),
            "headers": dict(request.headers),
        }

    return app


@pytest.fixture
def dummy_upstream_app() -> FastAPI:
    return _dummy_upstream_app()

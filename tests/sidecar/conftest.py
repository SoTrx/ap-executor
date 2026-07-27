"""Shared fixtures/helpers for the sidecar's tests."""
from typing import Any, Dict

import pytest
from fastapi import FastAPI, Request


def base_env(**overrides: str) -> Dict[str, str]:
    """A minimal, valid env mapping for `sidecar.config.load_config`; individual
    tests override/delete keys to exercise validation failures."""
    env = {
        "SIDECAR_SERVICE_NAME": "Text to SQL",
        "SIDECAR_ADVERTISE_ADDRESS": "sidecar.test",
        "UPSTREAM_PORT": "9000",
        "OPERATOR_NAME": "Text to SQL",
        "OPERATOR_VERSION": "1.0.0",
        "OPERATOR_EXECUTION": '{"mode":"sync","protocol":"http","endpoint":"/execute"}',
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

"""Shared fixtures for the integration-style tests.

These tests exercise the real code paths (manifest fetch, execution strategies,
the executor's orchestration + dataflow) against **real dummy operator apps**
wired in over :class:`httpx.ASGITransport` — actual ASGI routing, no
``MockTransport`` handlers and no network sockets.
"""
from types import SimpleNamespace
from typing import Any, Callable, Dict

import pytest
import yaml
from fastapi import FastAPI, Request, Response


def _serve_manifest(app: FastAPI, manifest: Dict[str, Any]) -> None:
    manifest_yaml = yaml.safe_dump(manifest)

    @app.get("/.well-known/operator.yaml")
    async def _manifest() -> Response:  # noqa: WPS430 (nested is fine for a dummy)
        return Response(content=manifest_yaml, media_type="application/yaml")


def _make_sync_operator_app(
    manifest: Dict[str, Any], on_execute: Callable[[Dict[str, Any]], Dict[str, Any]]
) -> FastAPI:
    """A dummy sync operator: serves its manifest and one ``execute`` endpoint."""
    app = FastAPI()
    _serve_manifest(app, manifest)

    @app.post(manifest["execution"]["endpoint"])
    async def _execute(request: Request) -> Dict[str, Any]:
        return on_execute(await request.json())

    return app


def _make_async_operator_app(
    manifest: Dict[str, Any], result: Dict[str, Any], *, running_polls: int = 1
) -> FastAPI:
    """A dummy async operator: start returns a job id, poll reports ``running``
    ``running_polls`` times, then ``done`` with ``result``."""
    app = FastAPI()
    _serve_manifest(app, manifest)
    polls: Dict[str, int] = {}

    @app.post(manifest["execution"]["start_endpoint"])
    async def _start(request: Request) -> Dict[str, Any]:
        polls["job-1"] = 0
        return {"id": "job-1"}

    poll_path = manifest["execution"]["poll_endpoint"].replace("{id}", "{job_id}")

    @app.get(poll_path)
    async def _poll(job_id: str) -> Dict[str, Any]:
        polls[job_id] += 1
        if polls[job_id] <= running_polls:
            return {"status": "running"}
        return {"status": "done", "result": result}

    return app


@pytest.fixture
def operator_apps() -> SimpleNamespace:
    """Factory for dummy operator ASGI apps (``.sync(...)`` / ``.async_(...)``)."""
    return SimpleNamespace(sync=_make_sync_operator_app, async_=_make_async_operator_app)

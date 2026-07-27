"""Throwaway reference upstream, for manually smoke-testing the sidecar's
proxy + Consul registration end-to-end (see `.devcontainer/docker-compose.yml`'s
`sidecar-dev` profile). Not used by the automated pytest suite -- those tests
use `httpx.ASGITransport` against in-process dummy apps instead
(`tests/sidecar/conftest.py`).

This is the "Minimal reference implementation" sync operator from
docs/docs/operators.md, minus its own `/.well-known/operator.json` route
(the sidecar now owns serving that), plus the `GET /health` endpoint the
sidecar's Consul check requires from any operator it fronts.
"""
from fastapi import FastAPI

app = FastAPI()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/execute")
async def execute(payload: dict):
    return {"query": f"SELECT * FROM students WHERE {payload['nl']}"}

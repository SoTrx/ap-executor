"""Health and readiness check endpoints."""
import os

import httpx
from fastapi.responses import JSONResponse


async def _check_consul(consul_addr: str) -> dict:
    """Check that the Consul agent (operator registry) is reachable."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as http:
            resp = await http.get(f"{consul_addr.rstrip('/')}/v1/status/leader")
            resp.raise_for_status()
        return {"status": "reachable"}
    except httpx.HTTPError as e:
        return {"status": "unreachable", "detail": str(e)}


async def _check_dapr_sidecar(dapr_http_port: str) -> dict:
    """Check that the local Dapr sidecar (workflow orchestration) is healthy."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as http:
            resp = await http.get(f"http://localhost:{dapr_http_port}/v1.0/healthz")
            resp.raise_for_status()
        return {"status": "reachable"}
    except httpx.HTTPError as e:
        return {"status": "unreachable", "detail": str(e)}


async def health_check():
    """Liveness check – returns service version info."""
    return {"status": "ok", "service": "ap-executor"}


async def readiness_check():
    """
    Readiness check – verifies that Consul (operator registry) and the local
    Dapr sidecar (workflow orchestration) are reachable before the service
    is considered ready to handle traffic.

    Returns HTTP 200 with ``status: ready`` when all dependencies are reachable,
    or HTTP 503 with ``status: not_ready`` together with per-dependency details
    when at least one dependency is unavailable.
    """
    consul_status = await _check_consul(os.getenv("CONSUL_HTTP_ADDR", "http://localhost:8500"))
    dapr_status = await _check_dapr_sidecar(os.getenv("DAPR_HTTP_PORT", "3500"))

    all_ready = all(s["status"] == "reachable" for s in (consul_status, dapr_status))

    body = {
        "status": "ready" if all_ready else "not_ready",
        "dependencies": {
            "consul": consul_status,
            "dapr_sidecar": dapr_status,
        },
    }
    return JSONResponse(content=body, status_code=200 if all_ready else 503)

"""The operator sidecar ASGI app: serves its own manifest, registers/
deregisters itself in Consul around the app lifespan, and transparently
proxies everything else to the real operator backend.
"""
import logging
from contextlib import asynccontextmanager
from typing import Callable, Optional

import httpx
from fastapi import FastAPI, Request

from .config import SidecarConfig, load_config
from .consul_registration import ConsulRegistrationClient
from .errors import ConsulRegistrationError
from .proxy import proxy_request

logger = logging.getLogger(__name__)

MANIFEST_PATH = "/.well-known/operator.json"

_ANY_METHOD = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


def create_app(
    config: Optional[SidecarConfig] = None,
    *,
    consul_http_factory: Callable[[SidecarConfig], httpx.AsyncClient] = (
        lambda cfg: httpx.AsyncClient(timeout=cfg.consul_timeout_seconds)
    ),
    proxy_http_factory: Callable[[SidecarConfig], httpx.AsyncClient] = (
        lambda cfg: httpx.AsyncClient(timeout=cfg.proxy_timeout_seconds)
    ),
) -> FastAPI:
    """Build the sidecar's FastAPI app.

    `consul_http_factory`/`proxy_http_factory` are injectable so tests can
    swap in MockTransport/ASGITransport-backed clients without touching a
    real network -- the same DI shape as
    `ap_executor/workflows/operator_execution.py`'s `http_client_factory`
    default-arg pattern.
    """
    cfg = config or load_config()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        proxy_http = proxy_http_factory(cfg)
        consul_http = consul_http_factory(cfg)
        consul_client = ConsulRegistrationClient(consul_http, cfg.consul_addr)
        app.state.proxy_http = proxy_http
        app.state.consul_client = consul_client

        # Fatal: if we can't register, don't serve traffic -- fail the
        # process so an orchestrator restarts it.
        await consul_client.register(
            service_id=cfg.service_id,
            service_name=cfg.service_slug,
            address=cfg.advertise_address,
            port=cfg.advertise_port,
            version=cfg.manifest.version,
            check_url=cfg.consul_check_url,
            check_interval=cfg.health_check_interval,
            check_timeout=cfg.health_check_timeout,
            deregister_after=cfg.deregister_after,
        )
        try:
            yield
        finally:
            try:
                await consul_client.deregister(cfg.service_id)
            except ConsulRegistrationError as exc:
                # Best-effort: the process is exiting anyway; Consul's own
                # critical-check + DeregisterCriticalServiceAfter is the
                # belt-and-suspenders fallback.
                logger.warning(
                    "Deregister failed on shutdown, relying on Consul's critical-check TTL: %s",
                    exc.message,
                )
            await proxy_http.aclose()
            await consul_http.aclose()

    app = FastAPI(lifespan=lifespan)

    @app.get(MANIFEST_PATH)
    async def manifest():
        # Served directly from the in-memory config -- never proxied, never
        # fetched from the upstream operator.
        return cfg.manifest.model_dump(mode="json")

    @app.api_route("/{_full_path:path}", methods=_ANY_METHOD)
    async def catch_all(request: Request):
        return await proxy_request(request, app.state.proxy_http, cfg.upstream_base_url)

    return app

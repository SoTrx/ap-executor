"""Sidecar configuration: everything is read from plain env vars, once, at
process startup (mirrors the repo-wide ``os.getenv`` convention used across
``ap_executor`` -- no settings framework). ``load_config`` raises
``SidecarConfigError`` immediately on any missing/invalid value; callers
(``sidecar/main.py``, ``sidecar/app.py``) call it eagerly, before binding a
port or touching Consul.
"""
import json
import os
import re
from dataclasses import dataclass
from typing import Mapping, Optional

from pydantic import ValidationError

from ap_executor.services.operator_resolver.manifest.manifest import OperatorManifest

from .errors import SidecarConfigError

# Duplicated from OperatorRegistry.normalize_op_name
# (ap_executor/services/operator_resolver/registry/registry.py): that method
# lives on an ABC instance, not a staticmethod, so there's no clean way to
# call it without instantiating a concrete registry. Keep in sync.
_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(name: str) -> str:
    return _SLUG_RE.sub("-", name.strip().lower()).strip("-")


@dataclass(frozen=True)
class SidecarConfig:
    consul_addr: str
    service_name: str
    service_slug: str
    service_id: str
    bind_host: str
    bind_port: int
    advertise_address: str
    advertise_port: int
    health_check_path: str
    health_check_interval: str
    health_check_timeout: str
    deregister_after: str
    proxy_timeout_seconds: float
    consul_timeout_seconds: float
    upstream_base_url: str
    manifest: OperatorManifest

    @property
    def consul_check_url(self) -> str:
        """The URL Consul's agent polls -- the sidecar's own address, not the upstream's."""
        return f"http://{self.advertise_address}:{self.advertise_port}{self.health_check_path}"


def load_config(env: Optional[Mapping[str, str]] = None) -> SidecarConfig:
    e = env if env is not None else os.environ

    def require(name: str) -> str:
        value = e.get(name)
        if not value:
            raise SidecarConfigError(f"missing required environment variable '{name}'")
        return value

    def parse_json(name: str, raw: str) -> object:
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SidecarConfigError(f"'{name}' is not valid JSON: {exc}") from exc

    service_name = require("SIDECAR_SERVICE_NAME")
    service_slug = _slugify(service_name)
    bind_host = e.get("SIDECAR_HOST", "0.0.0.0")
    bind_port = int(e.get("SIDECAR_PORT", "8000"))
    advertise_address = require("SIDECAR_ADVERTISE_ADDRESS")
    advertise_port = int(e.get("SIDECAR_ADVERTISE_PORT", str(bind_port)))
    service_id = e.get("SIDECAR_SERVICE_ID") or f"{service_slug}-{advertise_address}-{advertise_port}"

    upstream_host = e.get("UPSTREAM_HOST", "localhost")
    upstream_port = require("UPSTREAM_PORT")

    try:
        manifest = OperatorManifest.model_validate({
            "manifest_version": e.get("OPERATOR_MANIFEST_VERSION", "0.1.0"),
            "operator": require("OPERATOR_NAME"),
            "version": require("OPERATOR_VERSION"),
            "execution": parse_json("OPERATOR_EXECUTION", require("OPERATOR_EXECUTION")),
            "inputs": parse_json("OPERATOR_INPUTS", e.get("OPERATOR_INPUTS", "[]")),
            "outputs": parse_json("OPERATOR_OUTPUTS", e.get("OPERATOR_OUTPUTS", "[]")),
        })
    except ValidationError as exc:
        raise SidecarConfigError(f"OPERATOR_* fields failed manifest validation: {exc}") from exc

    return SidecarConfig(
        consul_addr=e.get("CONSUL_HTTP_ADDR", "http://localhost:8500").rstrip("/"),
        service_name=service_name,
        service_slug=service_slug,
        service_id=service_id,
        bind_host=bind_host,
        bind_port=bind_port,
        advertise_address=advertise_address,
        advertise_port=advertise_port,
        health_check_path=e.get("SIDECAR_HEALTH_CHECK_PATH", "/health"),
        health_check_interval=e.get("SIDECAR_HEALTH_CHECK_INTERVAL", "10s"),
        health_check_timeout=e.get("SIDECAR_HEALTH_CHECK_TIMEOUT", "5s"),
        deregister_after=e.get("SIDECAR_DEREGISTER_AFTER", "1m"),
        proxy_timeout_seconds=float(e.get("SIDECAR_PROXY_TIMEOUT_SECONDS", "30")),
        consul_timeout_seconds=float(e.get("SIDECAR_CONSUL_TIMEOUT_SECONDS", "10")),
        upstream_base_url=f"http://{upstream_host}:{upstream_port}",
        manifest=manifest,
    )

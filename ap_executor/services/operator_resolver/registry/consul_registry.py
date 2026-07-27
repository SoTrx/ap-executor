"""Consul-backed operator service registry.

Resolves the logical operator identifiers declared in an AP graph (name +
optional version) to a healthy, remotely-invokable service instance. Every
operator implementation is expected to self-register into Consul via its own
sidecar (see the AP execution engine design issue) — this client only reads
the registry, it never writes to it.
"""
import logging
import random
from typing import Optional

import httpx

from ap_executor.services.operator_resolver.errors import UnsupportedOperatorError

from .registry import OperatorRegistry, ServiceInstance

logger = logging.getLogger(__name__)


class ConsulRegistryClient(OperatorRegistry):
    """Resolves a logical operator name(+version) to a healthy Consul-registered instance."""

    def __init__(self, http: httpx.AsyncClient, consul_addr: str):
        self._http = http
        self._consul_addr = consul_addr.rstrip("/")

    async def resolve_operator(self, name: str, version: Optional[str] = None) -> ServiceInstance:
        """Return one healthy instance implementing the given operator.

        Args:
            name: Human-readable operator name, e.g. "Text to SQL".
            version: Optional operator version to disambiguate between coexisting implementations.

        Raises:
            UnsupportedOperatorError: If no healthy instance is registered for this operator.
        """
        service_name = self.normalize_op_name(name)
        params = {"passing": "true"}
        if version:
            params["filter"] = f'Service.Meta.version == "{version}"'

        resp = await self._http.get(
            f"{self._consul_addr}/v1/health/service/{service_name}", params=params
        )
        resp.raise_for_status()
        entries = resp.json()
        if not entries:
            raise UnsupportedOperatorError(f"{name}@{version or 'any'}")

        # Trivial load-balancing across healthy instances.
        entry = random.choice(entries)
        svc = entry["Service"]
        address = svc.get("Address") or entry.get("Node", {}).get("Address")
        return ServiceInstance(
            service_id=svc["ID"],
            service_name=svc["Service"],
            address=address,
            port=svc["Port"],
            meta=svc.get("Meta") or {},
        )

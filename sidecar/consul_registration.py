"""Write-side Consul client: registers/deregisters this sidecar's own service
instance. Counterpart to (but intentionally separate from) the read-only
``ConsulRegistryClient`` in
``ap_executor/services/operator_resolver/registry/consul_registry.py``,
which only ever resolves operators, never writes to Consul.
"""
import logging

import httpx

from .errors import ConsulRegistrationError

logger = logging.getLogger(__name__)


class ConsulRegistrationClient:
    """Registers/deregisters a single service instance with a Consul agent."""

    def __init__(self, http: httpx.AsyncClient, consul_addr: str):
        self._http = http
        self._consul_addr = consul_addr.rstrip("/")

    async def register(
        self,
        *,
        service_id: str,
        service_name: str,
        address: str,
        port: int,
        version: str,
        check_url: str,
        check_interval: str = "10s",
        check_timeout: str = "5s",
        deregister_after: str = "1m",
    ) -> None:
        body = {
            "ID": service_id,
            "Name": service_name,
            "Address": address,
            "Port": port,
            "Meta": {"version": version},
            "Check": {
                "HTTP": check_url,
                "Interval": check_interval,
                "Timeout": check_timeout,
                "DeregisterCriticalServiceAfter": deregister_after,
            },
        }
        try:
            resp = await self._http.put(f"{self._consul_addr}/v1/agent/service/register", json=body)
        except httpx.HTTPError as exc:
            raise ConsulRegistrationError(service_id, str(exc)) from exc
        if resp.status_code >= 400:
            raise ConsulRegistrationError(service_id, f"register returned {resp.status_code}: {resp.text}")
        logger.info("Registered '%s' (id=%s) with Consul", service_name, service_id)

    async def deregister(self, service_id: str) -> None:
        try:
            resp = await self._http.put(f"{self._consul_addr}/v1/agent/service/deregister/{service_id}")
        except httpx.HTTPError as exc:
            raise ConsulRegistrationError(service_id, str(exc)) from exc
        if resp.status_code >= 400:
            raise ConsulRegistrationError(service_id, f"deregister returned {resp.status_code}: {resp.text}")
        logger.info("Deregistered '%s' from Consul", service_id)

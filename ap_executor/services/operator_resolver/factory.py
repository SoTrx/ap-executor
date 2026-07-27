"""Default OperatorResolver factory: Consul registry + HTTP manifest fetch.

Kept separate from ``ap_executor.di`` so both the FastAPI DI container and the
Dapr workflow activities can depend on it without creating a cycle --- ``di``
transitively imports the activities (via ``ap_executor.workflows``), so the
activities can never import back from ``di``.
"""
import os

import httpx

from ap_executor.services.operator_resolver.manifest import HttpManifestRetriever
from ap_executor.services.operator_resolver.operator_resolver import OperatorResolver
from ap_executor.services.operator_resolver.registry.consul_registry import (
    ConsulRegistryClient,
)

CONSUL_HTTP_ADDR = os.getenv("CONSUL_HTTP_ADDR", "http://localhost:8500")


def default_operator_resolver(http: httpx.AsyncClient) -> OperatorResolver:
    """Production OperatorResolver: Consul registry + HTTP manifest fetch."""
    return OperatorResolver(
        ConsulRegistryClient(http, CONSUL_HTTP_ADDR),
        HttpManifestRetriever(http),
    )

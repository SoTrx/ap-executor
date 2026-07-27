"""Operator execution strategy abstraction – hides operator invocation-protocol differences.

An operator's manifest declares how it must be invoked (execution mode +
protocol). The strategy for that combination is responsible for translating
a generic ``start``/``poll`` call into whatever the operator's manifest
actually requires. Polling cadence is intentionally NOT owned by the
strategy — callers (the Dapr workflow orchestrator, via its activities)
decide when to call ``poll`` again, so a slow operator never ties up a call
for its whole duration.
"""
from typing import Any, Dict, Protocol

import httpx

from ap_executor.services.executor.execution_handle import ExecutionHandle
from ap_executor.services.operator_resolver.manifest.manifest import OperatorManifest
from ap_executor.services.operator_resolver.registry.service_instance import (
    ServiceInstance,
)


class OperatorExecutionStrategy(Protocol):
    """Invokes an operator implementation according to its manifest's declared execution mode."""

    async def start(
        self,
        http: httpx.AsyncClient,
        instance: ServiceInstance,
        manifest: OperatorManifest,
        inputs: Dict[str, Any],
    ) -> ExecutionHandle:
        """Kick off the operator invocation. May return an already-terminal handle."""
        ...

    async def poll(self, http: httpx.AsyncClient, handle: ExecutionHandle) -> ExecutionHandle:
        """Check the status of a previously started invocation. Called until ``handle.done``."""
        ...

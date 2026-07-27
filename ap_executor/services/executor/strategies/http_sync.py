"""Strategy for operators exposing a single synchronous HTTP execute endpoint."""
from typing import Any, Dict

import httpx

from ap_executor.services.executor.errors import OperatorExecutionError
from ap_executor.services.executor.execution_handle import ExecutionHandle
from ap_executor.services.operator_resolver.manifest import OperatorManifest
from ap_executor.services.operator_resolver.registry import ServiceInstance


class HttpSyncExecutionStrategy:
    """Calls ``execution.endpoint`` and returns the response body as the operator's output."""

    async def start(
        self, http: httpx.AsyncClient, instance: ServiceInstance, manifest: OperatorManifest, inputs: Dict[str, Any],
    ) -> ExecutionHandle:
        spec = manifest.execution

        resp = await http.post(f"{instance.base_url}{spec.endpoint}", json=inputs, timeout=30.0)
        if resp.is_error:
            raise OperatorExecutionError(
                manifest.operator, f"HTTP {resp.status_code}: {resp.text}")

        return ExecutionHandle(done=True, success=True, output=resp.json())

    async def poll(self, http: httpx.AsyncClient, handle: ExecutionHandle) -> ExecutionHandle:
        return handle

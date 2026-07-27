"""Strategy for operators exposing an async start+poll job pattern over HTTP."""
from typing import Any, Dict

import httpx

from ap_executor.services.executor.errors import OperatorExecutionError
from ap_executor.services.executor.execution_handle import ExecutionHandle
from ap_executor.services.operator_resolver.manifest import OperatorManifest
from ap_executor.services.operator_resolver.registry import ServiceInstance

_TERMINAL_SUCCESS = {"done", "completed", "success"}
_TERMINAL_FAILURE = {"error", "failed"}


class HttpAsyncPollingExecutionStrategy:
    """Starts a job via ``execution.start_endpoint`` and tracks it via ``execution.poll_endpoint``."""

    async def start(
        self,
        http: httpx.AsyncClient,
        instance: ServiceInstance,
        manifest: OperatorManifest,
        inputs: Dict[str, Any],
    ) -> ExecutionHandle:
        spec = manifest.execution
        resp = await http.post(f"{instance.base_url}{spec.start_endpoint}", json=inputs, timeout=30.0)
        if resp.is_error:
            raise OperatorExecutionError(
                manifest.operator, f"HTTP {resp.status_code}: {resp.text}")

        job_id = resp.json()["id"]
        poll_endpoint = f"{instance.base_url}{spec.poll_endpoint.format(id=job_id)}"

        return ExecutionHandle(done=False, poll_endpoint=poll_endpoint)

    async def poll(self, http: httpx.AsyncClient, handle: ExecutionHandle) -> ExecutionHandle:

        resp = await http.get(handle.poll_endpoint, timeout=10.0)
        resp.raise_for_status()
        body = resp.json()

        status = body.get("status")
        if status in _TERMINAL_SUCCESS:
            return handle.model_copy(update={"done": True, "success": True, "output": body.get("result")})
        if status in _TERMINAL_FAILURE:
            return handle.model_copy(update={"done": True, "success": False, "error": body.get("error")})

        return handle

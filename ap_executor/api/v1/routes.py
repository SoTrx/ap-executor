"""API v1 router – aggregates all sub-routers and standalone routes."""
from fastapi import APIRouter

from ap_executor.api.v1.execution.async_exec import execute_ap_async, get_task_status
from ap_executor.api.v1.execution.sync import execute_ap_sync
from ap_executor.api.v1.health import health_check, readiness_check
from ap_executor.di import REDIS_ENABLED

router = APIRouter(prefix="/api/v1", tags=["v1"])

# --- Execution endpoints ---
router.add_api_route(
    "/execute",
    execute_ap_sync,
    methods=["POST"],
    tags=["execute"],
    summary="Execute an AP synchronously",
    description="Parses the AP, resolves operator execution order, and runs each operator. Returns the full result inline.",
)

# Async endpoints are only registered when a Redis broker is configured
if REDIS_ENABLED:
    router.add_api_route(
        "/execute/async",
        execute_ap_async,
        methods=["POST"],
        tags=["execute-async"],
        summary="Execute an AP asynchronously",
        description="Dispatches the AP execution to a Celery worker and returns a task_id (HTTP 202).",
    )
    router.add_api_route(
        "/execute/async/{task_id}",
        get_task_status,
        methods=["GET"],
        tags=["execute-async"],
        summary="Poll async execution result",
        description="Returns the current status and result of a previously dispatched async execution task.",
    )

# --- Health ---
router.add_api_route("/health", health_check, methods=["GET"], tags=["health"])
router.add_api_route("/ready", readiness_check,
                     methods=["GET"], tags=["health"])

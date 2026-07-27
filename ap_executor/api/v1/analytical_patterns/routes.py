

from fastapi import APIRouter

from .exec_async import execute_ap_async, get_task_status
from .exec_sync import execute_ap_sync

router = APIRouter(tags=["analytical patterns"])


router.add_api_route(
    "/execute",
    execute_ap_sync,
    methods=["POST"],
    summary="Execute an AP synchronously",
    description="Accepts an AP instance ({ap, metadata.parameters}), resolves operator execution order, injects parameters, and runs each operator. Returns the full result inline.",
)
router.add_api_route(
    "/execute/async",
    execute_ap_async,
    methods=["POST"],
    summary="Execute an AP asynchronously",
    description="Schedules the AP instance execution as a Dapr workflow instance and returns a task_id (HTTP 202).",
)
router.add_api_route(
    "/execute/async/{task_id}",
    get_task_status,
    methods=["GET"],
    summary="Poll async execution result",
    description="Returns the current status and result of a previously dispatched async execution task.",
)

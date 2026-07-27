"""API v1 router – aggregates all sub-routers and standalone routes."""
from fastapi import APIRouter

from ap_executor.api.v1.analytical_patterns.routes import router as ap_router
from ap_executor.api.v1.health import health_check, readiness_check

router = APIRouter(prefix="/api/v1")

# --- Health ---
router.include_router(ap_router, prefix="/aps")
router.add_api_route("/health", health_check, methods=["GET"])
router.add_api_route("/ready", readiness_check, methods=["GET"])

import logging
from contextlib import asynccontextmanager

from dapr.ext.workflow.aio import DaprWorkflowClient
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from httpx import AsyncClient

import ap_executor.workflows  # noqa: F401 - registers the workflow + activities onto `wfr`
from ap_executor.services.operator_resolver import OperatorResolver
from ap_executor.services.operator_resolver.factory import default_operator_resolver
from ap_executor.workflows.runtime import wfr

load_dotenv()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def container_lifespan(app: FastAPI):
    """
    Lifespan context manager for the FastAPI application.
    Starts the Dapr WorkflowRuntime (registers the AP execution workflow and
    its activities, then connects to the local Dapr sidecar) and makes a
    ``DaprWorkflowClient`` available to route handlers via ``app.state``.
    """
    wfr.start()
    app.state.workflow_client = DaprWorkflowClient()
    try:
        yield
    finally:
        wfr.shutdown()


def get_workflow_client(request: Request) -> DaprWorkflowClient:
    """FastAPI dependency providing the shared Dapr workflow client."""
    return request.app.state.workflow_client


def get_operator_resolver() -> OperatorResolver:
    """FastAPI dependency providing the shared operator resolver."""
    with AsyncClient() as http:
        return default_operator_resolver(http)

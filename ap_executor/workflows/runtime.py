"""The module-level Dapr WorkflowRuntime.

Importing ``ap_executor.workflows`` registers the AP execution workflow and
its activities onto ``wfr`` (via their ``@wfr.workflow``/``@wfr.activity``
decorators). ``ap_executor.di`` starts/stops ``wfr`` alongside the FastAPI
app's lifespan.
"""
from dapr.ext.workflow import WorkflowRuntime

wfr = WorkflowRuntime()

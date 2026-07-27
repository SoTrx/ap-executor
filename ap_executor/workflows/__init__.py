"""Dapr Workflow wiring for AP execution: the orchestrator and its activities.

Importing this package registers ``ap_execution_workflow`` and its
activities onto the shared ``wfr`` runtime.
"""
from ap_executor.workflows.ap_execution import ap_execution_workflow
from ap_executor.workflows.runtime import wfr

__all__ = ["wfr", "ap_execution_workflow"]

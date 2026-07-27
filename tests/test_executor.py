"""Unit tests for operator ordering (ApInstance) and orchestrator status aggregation."""
from ap_executor.domain.ap_instance import ApInstance
from ap_executor.services.executor.execution import (
    ExecutionStatus,
    OperatorResult,
    OperatorStatus,
)
from ap_executor.workflows.ap_execution import _aggregate_status

DB_ID = "11111111-1111-4111-8111-111111111111"
OP_ANNOTATE_ID = "22222222-2222-4222-8222-222222222222"
OP_QUERY_ID = "33333333-3333-4333-8333-333333333333"
AP_ID = "44444444-4444-4444-8444-444444444444"

SAMPLE_AP_WITH_ORDER = {
    "nodes": [
        {"id": AP_ID, "labels": ["Analytical_Pattern"],
            "properties": {"name": "Test AP"}},
        {
            "id": OP_ANNOTATE_ID,
            "labels": ["Operator", "Provenance_Annotate_Dataset_Operator"],
            "properties": {"name": "Annotate"},
        },
        {
            "id": OP_QUERY_ID,
            "labels": ["Operator", "Provenance_SQL_Operator"],
            "properties": {"name": "Query"},
        },
    ],
    "edges": [
        {"from": AP_ID, "to": OP_ANNOTATE_ID, "labels": ["consist_of"]},
        {"from": AP_ID, "to": OP_QUERY_ID, "labels": ["consist_of"]},
        {"from": OP_QUERY_ID, "to": OP_ANNOTATE_ID, "labels": ["follows"]},
    ],
}


def _instance(ap_dict: dict) -> ApInstance:
    return ApInstance.model_validate({"ap": ap_dict})


def test_iter_execution_order():
    """Operators connected by 'follows' should be sorted topologically."""
    instance = _instance(SAMPLE_AP_WITH_ORDER)
    order = list(instance.iter_operators())
    names = [o.properties["name"] for o in order if o.properties]
    assert names == ["Annotate", "Query"]


def test_iter_execution_order_no_operators():
    ap = {
        "nodes": [{"id": AP_ID, "labels": ["Analytical_Pattern"], "properties": {"name": "Empty AP"}}],
        "edges": [],
    }
    instance = _instance(ap)
    assert list(instance.iter_operators()) == []


def _result(node_id: str, status: OperatorStatus, **kwargs) -> OperatorResult:
    return OperatorResult(
        operator_id=node_id, operator_name="Op", operator_labels=["Operator"], status=status, **kwargs
    )


def test_aggregate_status_all_success():
    results = [_result("1", OperatorStatus.SUCCESS),
               _result("2", OperatorStatus.SUCCESS)]
    assert _aggregate_status(results) == ExecutionStatus.SUCCESS


def test_aggregate_status_mixed_is_partial():
    ok = _result("1", OperatorStatus.SUCCESS)
    failed = _result("2", OperatorStatus.ERROR, error="x")
    assert _aggregate_status([ok, failed]) == ExecutionStatus.PARTIAL_SUCCESS


def test_aggregate_status_all_failed_is_error():
    failed = _result("1", OperatorStatus.ERROR, error="x")
    assert _aggregate_status([failed]) == ExecutionStatus.ERROR


def test_aggregate_status_empty_is_success():
    assert _aggregate_status([]) == ExecutionStatus.SUCCESS

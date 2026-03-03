"""Unit tests for the executor service – operator ordering."""
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock

from ap_executor.services.executor import ExecutorService
from ap_executor.models.execution import ExecutionStatus, OperatorStatus
from ap_executor.models.pg_json import PgJson


SAMPLE_AP_WITH_ORDER = {
    "nodes": [
        {
            "id": "db-1",
            "labels": ["Relational_Database"],
            "properties": {"name": "testdb"},
        },
        {
            "id": "op-annotate",
            "labels": ["Operator", "Provenance_Annotate_Dataset_Operator"],
            "properties": {"name": "Annotate"},
        },
        {
            "id": "op-query",
            "labels": ["Operator", "Provenance_SQL_Operator"],
            "properties": {
                "name": "Query",
                "query": "SELECT 1",
                "command": "query",
            },
        },
    ],
    "edges": [
        {"from": "op-query", "to": "op-annotate", "labels": ["follows"]},
    ],
}


def test_resolve_execution_order():
    """Operators connected by 'follows' should be sorted topologically."""
    ap = PgJson.model_validate(SAMPLE_AP_WITH_ORDER)
    # We don't need a real connection for order resolution
    service = ExecutorService(conn=None)  # type: ignore
    order = service._resolve_execution_order(ap)
    names = [o.properties["name"] for o in order if o.properties]
    assert names == ["Annotate", "Query"]


@pytest.mark.asyncio
async def test_execute_ap_skips_non_executable():
    """Non-SQL operators without a command are skipped."""
    ap = PgJson.model_validate({
        "nodes": [
            {
                "id": "op-noop",
                "labels": ["Operator"],
                "properties": {"name": "NoOp"},
            },
        ],
        "edges": [],
    })

    mock_conn = AsyncMock()
    service = ExecutorService(conn=mock_conn)
    result = await service.execute_ap(ap, "testdb", "public", "Test")

    assert result.status == ExecutionStatus.SUCCESS
    assert len(result.operators) == 1
    assert result.operators[0].status == OperatorStatus.SKIPPED

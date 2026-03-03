"""
POST /execute – synchronous AP execution.

Receives an AP in PG-JSON format, resolves the execution order from the
operator graph, executes each operator against the target database, and
returns the full ``ExecutionResult`` inline.
"""
from logging import getLogger

from fastapi import HTTPException, status

from ap_executor.api.v1.dependencies.ap_parser import (
    ApName,
    DatabaseName,
    SchemaName,
)
from ap_executor.di import get_db_connection_for_ap
from ap_executor.services.executor import ExecutorService
from ap_executor.types.execution import ExecutionResult
from ap_executor.types.pg_json import PgJson

logger = getLogger(__name__)


async def execute_ap_sync(
    ap: PgJson,
    db_name: DatabaseName,
    schema_name: SchemaName,
    ap_name: ApName,
) -> ExecutionResult:
    """Execute the AP synchronously and return the result immediately.

    The AP graph is parsed, operators are resolved in dependency order, and
    each one is executed against the database indicated by the
    ``Relational_Database`` node.

    Returns:
        ``ExecutionResult`` with per-operator outcomes.
    """
    logger.info("Synchronous execution requested for AP '%s' on db '%s'", ap_name, db_name)

    connection_factory = get_db_connection_for_ap(db_name)

    async for conn in connection_factory():
        service = ExecutorService(conn)
        result = await service.execute_ap(ap, db_name, schema_name, ap_name)
        return result

    raise HTTPException(
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Could not establish a database connection",
    )

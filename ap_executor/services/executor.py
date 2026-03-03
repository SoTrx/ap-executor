"""
Executor service – walks the operator graph of an AP and runs each step.

The service receives a parsed AP (PgJson), resolves the execution order from
the graph edges, and executes each operator sequentially against the target
database.
"""
import logging
from typing import List, Optional

from psycopg import AsyncConnection

from ap_executor.models.execution import (
    ExecutionResult,
    ExecutionStatus,
    OperatorResult,
    OperatorStatus,
)
from ap_executor.models.pg_json import PgJson, PgJsonNode

logger = logging.getLogger(__name__)

# Labels recognised as executable operators
OPERATOR_LABELS = {"Operator"}
# Known concrete operator sub-labels and their handler names
SQL_OPERATOR_LABELS = {
    "Provenance_SQL_Operator",
    "SQL_Operator",
    "Select_Operator",
    "Query_Operator",
}


class ExecutorService:
    """Executes the operators defined in an Analytical Pattern."""

    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def execute_ap(
        self,
        ap: PgJson,
        db_name: str,
        schema_name: str,
        ap_name: Optional[str] = None,
    ) -> ExecutionResult:
        """Execute all operators in the AP in topological order.

        Args:
            ap: The parsed AP graph.
            db_name: Target database name.
            schema_name: Target schema name.
            ap_name: Human-readable AP name (optional).

        Returns:
            An ``ExecutionResult`` summarising the outcome of every operator.
        """
        operators = self._resolve_execution_order(ap)
        logger.info("Resolved %d operators for execution in AP '%s'",
                    len(operators), ap_name or "unnamed")

        result = ExecutionResult(
            ap_name=ap_name,
            database_name=db_name,
            schema_name=schema_name,
            status=ExecutionStatus.RUNNING,
        )

        has_error = False
        for node in operators:
            op_result = await self._execute_operator(node, schema_name)
            result.operators.append(op_result)
            if op_result.status == OperatorStatus.ERROR:
                has_error = True

        if has_error and all(o.status == OperatorStatus.ERROR for o in result.operators):
            result.status = ExecutionStatus.ERROR
        elif has_error:
            result.status = ExecutionStatus.PARTIAL_SUCCESS
        else:
            result.status = ExecutionStatus.SUCCESS

        return result

    def _resolve_execution_order(self, ap: PgJson) -> List[PgJsonNode]:
        """Return operator nodes in topological (dependency) order.

        Operators connected via ``follows`` edges are ordered such that
        predecessors are executed first.  Operators without ordering
        constraints are appended in node-list order.
        """
        operator_nodes = [
            n for n in ap.nodes
            if OPERATOR_LABELS & set(n.labels)
        ]

        if not operator_nodes:
            return []

        # Build adjacency from "follows" edges (from_ follows to_ => to_ first)
        id_to_node = {n.id: n for n in operator_nodes}
        in_degree: dict[str, int] = {n.id: 0 for n in operator_nodes}
        adj: dict[str, list[str]] = {n.id: [] for n in operator_nodes}

        for edge in ap.edges:
            if "follows" in edge.labels:
                # edge.from_ follows edge.to  →  edge.to must come before edge.from_
                if edge.to in id_to_node and edge.from_ in id_to_node:
                    adj[edge.to].append(edge.from_)
                    in_degree[edge.from_] = in_degree.get(edge.from_, 0) + 1

        # Kahn's algorithm
        queue = [nid for nid, deg in in_degree.items() if deg == 0]
        ordered: List[PgJsonNode] = []
        while queue:
            nid = queue.pop(0)
            ordered.append(id_to_node[nid])
            for neighbour in adj.get(nid, []):
                in_degree[neighbour] -= 1
                if in_degree[neighbour] == 0:
                    queue.append(neighbour)

        # Append any operator not reachable (isolated)
        seen = {n.id for n in ordered}
        for n in operator_nodes:
            if n.id not in seen:
                ordered.append(n)

        return ordered

    async def _execute_operator(self, node: PgJsonNode, schema_name: str) -> OperatorResult:
        """Dispatch execution of a single operator node."""
        op_name = (node.properties or {}).get("name", node.id)
        labels_set = set(node.labels)

        if labels_set & SQL_OPERATOR_LABELS:
            return await self._execute_sql_operator(node, schema_name, op_name)

        # Generic operator – attempt to run its "command" property if present
        if node.properties and "command" in node.properties:
            return await self._execute_generic_command(node, schema_name, op_name)

        # Non-executable operator (e.g. Annotate placeholder) – skip gracefully
        logger.info("Skipping non-executable operator '%s' (%s)",
                    op_name, node.labels)
        return OperatorResult(
            operator_id=node.id,
            operator_name=op_name,
            operator_labels=node.labels,
            status=OperatorStatus.SKIPPED,
        )

    async def _execute_sql_operator(
        self, node: PgJsonNode, schema_name: str, op_name: str
    ) -> OperatorResult:
        """Execute a SQL-based operator."""
        query = (node.properties or {}).get("query", "")
        if not query:
            return OperatorResult(
                operator_id=node.id,
                operator_name=op_name,
                operator_labels=node.labels,
                status=OperatorStatus.ERROR,
                error="SQL operator has no 'query' property",
            )

        logger.info("Executing SQL operator '%s': %s", op_name, query[:120])
        try:
            cursor = await self.conn.execute(f"SET search_path TO {schema_name}, public")
            cursor = await self.conn.execute(query)
            rows = await cursor.fetchall()
            columns = [
                desc.name for desc in cursor.description] if cursor.description else []
            result_data = [dict(zip(columns, row)) for row in rows]
            return OperatorResult(
                operator_id=node.id,
                operator_name=op_name,
                operator_labels=node.labels,
                status=OperatorStatus.SUCCESS,
                result=result_data,
                rows_affected=len(result_data),
            )
        except Exception as e:
            logger.exception("SQL operator '%s' failed", op_name)
            return OperatorResult(
                operator_id=node.id,
                operator_name=op_name,
                operator_labels=node.labels,
                status=OperatorStatus.ERROR,
                error=str(e),
            )

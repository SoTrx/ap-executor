"""FastAPI dependencies for parsing and validating AP (Analytical Pattern) structures."""
from logging import getLogger
from typing import Annotated, List, Optional

from fastapi import Depends, HTTPException, status

from ap_executor.models.pg_json import PgJson, PgJsonNode

logger = getLogger(__name__)


def extract_database_name(ap: PgJson) -> str:
    """
    Extract and validate database name from the Relational_Database node in the AP.

    Args:
        ap: The PgJson AP structure

    Returns:
        The database name from name property

    Raises:
        HTTPException: If the database node is missing or malformed
    """
    db_nodes = ap.get_nodes_by_label("Relational_Database")
    if not db_nodes or len(db_nodes) == 0:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="This AP has no Relational_Database node!"
        )

    db_node = db_nodes[0]
    if not db_node.properties or "name" not in db_node.properties:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Relational_Database node is missing 'name' property!"
        )

    return db_node.properties["name"]


def extract_schema_name(ap: PgJson) -> str:
    """
    Extract schema name from the AP.

    Looks at Table nodes for a schema prefix (schema.table), otherwise
    defaults to 'public'.
    """
    table_nodes = ap.get_nodes_by_label("Table")
    for node in table_nodes:
        if node.properties and "name" in node.properties:
            parts = node.properties["name"].split(".", 1)
            if len(parts) == 2:
                return parts[0]
    return "public"


def extract_operators(ap: PgJson) -> List[PgJsonNode]:
    """
    Extract all Operator nodes from the AP.

    Raises:
        HTTPException: If no operators are found
    """
    operator_nodes = [
        n for n in ap.nodes
        if "Operator" in n.labels
    ]
    if not operator_nodes:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="This AP has no Operator nodes!"
        )
    return operator_nodes


def extract_table_names(ap: PgJson) -> List[str]:
    """
    Extract and validate table names from Table nodes in the AP.
    Returns only the table name part (without schema prefix).
    """
    tables_nodes = ap.get_nodes_by_label("Table")
    if not tables_nodes or len(tables_nodes) == 0:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="This AP has no Table nodes!"
        )

    tables_names = []
    for node in tables_nodes:
        if not node.properties or "name" not in node.properties:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Some Table nodes are missing the 'name' property!"
            )
        table_name = node.properties["name"]
        parts = table_name.split(".", 1)
        tables_names.append(parts[-1])  # take table name without schema

    return tables_names


def extract_ap_name(ap: PgJson) -> Optional[str]:
    """Extract an optional human-readable name from the Analytical_Pattern node."""
    ap_nodes = ap.get_nodes_by_label("Analytical_Pattern")
    if ap_nodes and ap_nodes[0].properties and "name" in ap_nodes[0].properties:
        return ap_nodes[0].properties["name"]
    return None


# Type aliases for cleaner function signatures
DatabaseName = Annotated[str, Depends(extract_database_name)]
SchemaName = Annotated[str, Depends(extract_schema_name)]
Operators = Annotated[List[PgJsonNode], Depends(extract_operators)]
TableNames = Annotated[List[str], Depends(extract_table_names)]
ApName = Annotated[Optional[str], Depends(extract_ap_name)]

"""Unit tests for operator input resolution from an AP instance."""
import json
from pathlib import Path

from ap_executor.domain.ap_instance import ApInstance

FIXTURES = Path(__file__).parent.parent / "fixtures"

TEXT_TO_SQL_ID = "1de6e343-6952-4361-a17f-e4a9f1eaeae2"
SQL_PROVENANCE_ID = "68281dc0-9bb6-4caa-8bf8-b7d0054f1729"


def _load_instance(name: str, parameters: dict) -> ApInstance:
    ap_json = json.loads((FIXTURES / name).read_text())
    return ApInstance.model_validate({"ap": ap_json, "state": parameters})


def test_leaf_input_comes_from_parameters():
    """An operator with no upstream wiring gets its inputs straight from state[node_id]."""
    instance = _load_instance(
        "composed/01_02.json", {TEXT_TO_SQL_ID: {"nl": "show me all users"}}
    )

    resolved = instance.resolve_operator_input_values(TEXT_TO_SQL_ID)

    assert resolved == {"nl": "show me all users"}


def test_upstream_output_mapped_into_downstream_input_with_rename():
    """Text-to-SQL's ``query`` output feeds SQL-Provenance's ``sql`` input via the graph mapping."""
    instance = _load_instance(
        "composed/01_02.json", {TEXT_TO_SQL_ID: {"query": "SELECT 1"}}
    )

    resolved = instance.resolve_operator_input_values(SQL_PROVENANCE_ID)

    assert resolved == {"sql": "SELECT 1"}


def test_upstream_overrides_same_named_parameter():
    """A value wired in from upstream takes precedence over a caller parameter."""
    instance = _load_instance(
        "composed/01_02.json",
        {
            TEXT_TO_SQL_ID: {"query": "SELECT 1"},
            SQL_PROVENANCE_ID: {"sql": "stale"},
        },
    )

    resolved = instance.resolve_operator_input_values(SQL_PROVENANCE_ID)

    assert resolved == {"sql": "SELECT 1"}

"""Unit tests for the AP parser dependencies."""
import pytest
from fastapi import HTTPException

from ap_executor.api.v1.dependencies.ap_parser import (
    extract_ap_name,
    extract_database_name,
    extract_operators,
    extract_schema_name,
    extract_table_names,
)
from ap_executor.models.pg_json import PgJson


SAMPLE_AP = {
    "nodes": [
        {
            "id": "db-1",
            "labels": ["Relational_Database"],
            "properties": {"name": "testdb", "contentUrl": "postgresql://u:p@h/testdb"},
        },
        {
            "id": "tbl-1",
            "labels": ["Table"],
            "properties": {"name": "myschema.users"},
        },
        {
            "id": "op-1",
            "labels": ["Operator", "SQL_Operator"],
            "properties": {"name": "Select users", "query": "SELECT * FROM users"},
        },
        {
            "id": "ap-1",
            "labels": ["Analytical_Pattern"],
            "properties": {"name": "Test AP"},
        },
    ],
    "edges": [
        {"from": "op-1", "to": "tbl-1", "labels": ["input"]},
        {"from": "tbl-1", "to": "db-1", "labels": ["contain"]},
    ],
}


def test_extract_database_name():
    ap = PgJson.model_validate(SAMPLE_AP)
    assert extract_database_name(ap) == "testdb"


def test_extract_schema_name():
    ap = PgJson.model_validate(SAMPLE_AP)
    assert extract_schema_name(ap) == "myschema"


def test_extract_schema_name_defaults_to_public():
    ap_data = {
        "nodes": [
            {"id": "t", "labels": ["Table"], "properties": {"name": "users"}},
        ],
        "edges": [],
    }
    ap = PgJson.model_validate(ap_data)
    assert extract_schema_name(ap) == "public"


def test_extract_operators():
    ap = PgJson.model_validate(SAMPLE_AP)
    ops = extract_operators(ap)
    assert len(ops) == 1
    assert ops[0].id == "op-1"


def test_extract_table_names():
    ap = PgJson.model_validate(SAMPLE_AP)
    names = extract_table_names(ap)
    assert names == ["users"]


def test_extract_ap_name():
    ap = PgJson.model_validate(SAMPLE_AP)
    assert extract_ap_name(ap) == "Test AP"


def test_extract_database_name_missing():
    ap = PgJson.model_validate({"nodes": [], "edges": []})
    with pytest.raises(HTTPException):
        extract_database_name(ap)

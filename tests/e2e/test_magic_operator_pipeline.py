"""Submits the 3-operator magic_operator AP graph (e2e/magic_e2e.json) to a
real, running ap_executor over real HTTP, and asserts the full chain
worked: Consul resolution (via the Consul client agents in
e2e/docker-compose.yml), self-served operator manifests, both HTTP
execution strategies (Magic Echo A is sync_http, Magic Echo B is
async_http), and cross-operator dataflow wiring (Magic Combine's inputs
come from A's and B's actual outputs, not the raw caller parameters).
"""
import json
from pathlib import Path

import pytest

from magic_operator.llm.mock import MOCK_PREFIX
from magic_operator.prompt import render_prompt

FIXTURE_PATH = Path(__file__).parent.parent.parent / "e2e" / "magic_e2e.json"

A_ID = "d0000000-0000-0000-0000-000000000002"
B_ID = "d0000000-0000-0000-0000-000000000003"
C_ID = "d0000000-0000-0000-0000-000000000006"


@pytest.mark.e2e
def test_three_operator_pipeline_sync_and_async(http_client, base_url):
    ap = json.loads(FIXTURE_PATH.read_text())
    body = {
        "ap": ap,
        "state": {A_ID: {"topic": "solar power"}, B_ID: {"audience": "children"}},
    }

    resp = http_client.post(f"{base_url}/api/v1/aps/execute", json=body)

    assert resp.status_code == 200
    result = resp.json()
    assert result["status"] == "success"
    assert len(result["operators"]) == 3

    by_id = {op["operator_id"]: op for op in result["operators"]}

    expected_summary = f"{MOCK_PREFIX}{render_prompt('Summarize the topic: {topic}', {'topic': 'solar power'})}"
    expected_tone = f"{MOCK_PREFIX}{render_prompt('Pick a tone of voice for the audience: {audience}', {'audience': 'children'})}"
    combine_template = "Write a message summarizing '{summary}' in a '{tone}' tone."
    expected_message = f"{MOCK_PREFIX}{render_prompt(combine_template, {'summary': expected_summary, 'tone': expected_tone})}"

    assert by_id[A_ID]["status"] == "success"
    assert by_id[A_ID]["execution_mode"] == "sync"
    assert by_id[A_ID]["result"] == {"summary": expected_summary}

    assert by_id[B_ID]["status"] == "success"
    assert by_id[B_ID]["execution_mode"] == "async"
    assert by_id[B_ID]["result"] == {"tone": expected_tone}

    assert by_id[C_ID]["status"] == "success"
    assert by_id[C_ID]["execution_mode"] == "sync"
    assert by_id[C_ID]["result"] == {"message": expected_message}


@pytest.mark.e2e
def test_missing_required_input_surfaces_as_operator_error(http_client, base_url):
    """Magic Echo A's `topic` is required and has no default -- omitting the
    caller parameter entirely should surface as an executor-side error
    (the executor's own `_filter_inputs` catches this before ever calling
    the operator), not a 200 with a bogus result."""
    ap = json.loads(FIXTURE_PATH.read_text())
    body = {"ap": ap, "state": {B_ID: {"audience": "children"}}}

    resp = http_client.post(f"{base_url}/api/v1/aps/execute", json=body)

    assert resp.status_code == 200
    result = resp.json()
    assert result["status"] in ("error", "partial_success")
    by_id = {op["operator_id"]: op for op in result["operators"]}
    assert by_id[A_ID]["status"] == "error"

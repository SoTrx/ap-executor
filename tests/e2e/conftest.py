"""Fixtures for the e2e suite: real HTTP against a real, already-running
`docker-compose.e2e.yml` stack -- no `ASGITransport`, no mocks."""
import os
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))
from wait_for_e2e_stack import DEFAULT_SERVICE_SLUGS, wait_for_stack  # noqa: E402


@pytest.fixture(scope="session")
def base_url() -> str:
    return os.getenv("E2E_EXECUTOR_BASE_URL", "http://localhost:5000")


@pytest.fixture(scope="session")
def consul_addr() -> str:
    return os.getenv("E2E_CONSUL_ADDR", "http://localhost:8500")


@pytest.fixture(scope="session", autouse=True)
def _wait_for_stack(base_url, consul_addr):
    """Makes a bare `pytest tests/e2e -m e2e` also work standalone against an
    already-`docker compose up`'d stack, not just inside the CI step that
    already waited once."""
    wait_for_stack(base_url, consul_addr, DEFAULT_SERVICE_SLUGS, timeout=90.0)


@pytest.fixture
def http_client():
    with httpx.Client(timeout=30.0) as client:
        yield client

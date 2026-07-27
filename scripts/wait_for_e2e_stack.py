"""Polls the e2e stack until it's ready to accept the test AP graph:
`ap_executor`'s own readiness endpoint (Consul + local Dapr sidecar), then
Consul's health-check endpoint for each expected operator instance.

Used both by CI (`.github/workflows/e2e-tests.yml`) and by
`tests/e2e/conftest.py` so `pytest tests/e2e -m e2e` also works standalone
against an already-running stack.
"""
import sys
import time
from typing import Sequence

import httpx

DEFAULT_SERVICE_SLUGS = ("magic-echo-a", "magic-echo-b", "magic-combine")


def wait_for_stack(
    base_url: str = "http://localhost:5000",
    consul_addr: str = "http://localhost:8500",
    service_slugs: Sequence[str] = DEFAULT_SERVICE_SLUGS,
    timeout: float = 90.0,
    poll_interval: float = 2.0,
) -> None:
    deadline = time.monotonic() + timeout
    last_error = "never attempted"

    with httpx.Client(timeout=5.0) as client:
        while time.monotonic() < deadline:
            try:
                resp = client.get(f"{base_url}/api/v1/ready")
                if resp.status_code != 200:
                    last_error = f"GET {base_url}/api/v1/ready -> {resp.status_code}: {resp.text}"
                else:
                    not_passing = _not_yet_passing(client, consul_addr, service_slugs)
                    if not not_passing:
                        return
                    last_error = f"not yet passing in Consul: {not_passing}"
            except httpx.HTTPError as exc:
                last_error = f"{exc.__class__.__name__}: {exc}"
            time.sleep(poll_interval)

    raise RuntimeError(f"e2e stack did not become ready within {timeout}s; last error: {last_error}")


def _not_yet_passing(client: httpx.Client, consul_addr: str, service_slugs: Sequence[str]) -> list[str]:
    not_passing = []
    for slug in service_slugs:
        resp = client.get(f"{consul_addr}/v1/health/service/{slug}", params={"passing": "true"})
        if resp.status_code != 200 or not resp.json():
            not_passing.append(slug)
    return not_passing


if __name__ == "__main__":
    try:
        wait_for_stack()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    print("e2e stack is ready.")

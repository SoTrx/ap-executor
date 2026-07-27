"""In-memory job store for `async_http` mode, single-process only (mirrors
the constraint `sidecar/main.py` documents for Consul registration -- this
operator must also run as a single `uvicorn.run()` process, no `workers=`).

The LLM call happens synchronously when the job is created; only the
*reported* completion is deferred by a poll-cycle counter. This avoids
background tasks/threads/real sleeps entirely -- fast and deterministic for
CI.
"""
import uuid
from dataclasses import dataclass
from typing import Dict, Literal


@dataclass
class Job:
    result: dict
    running_polls_remaining: int


class JobStore:
    """`running_polls_remaining=N` means the first N calls to `poll()` report
    "running"; the call after that (and every one thereafter) reports "done"."""

    def __init__(self) -> None:
        self._jobs: Dict[str, Job] = {}

    def create(self, result: dict, running_polls: int) -> str:
        job_id = str(uuid.uuid4())
        self._jobs[job_id] = Job(result=result, running_polls_remaining=max(running_polls, 0))
        return job_id

    def poll(self, job_id: str) -> tuple[Literal["running", "done"], dict]:
        job = self._jobs[job_id]
        if job.running_polls_remaining > 0:
            job.running_polls_remaining -= 1
            return "running", job.result
        return "done", job.result

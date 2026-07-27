"""The magic operator ASGI app: validates its declared inputs, renders a
customizable prompt template against them, calls the configured LLM
provider, and returns the response as its single output. Serves either
`sync_http` or `async_http` routes depending on `MagicOperatorConfig.execution_mode`.
"""
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import ASYNC_POLL_PATH, ASYNC_START_PATH, SYNC_EXECUTE_PATH, MagicOperatorConfig, load_config
from .errors import InputValidationFailed
from .jobs import JobStore
from .llm.factory import build_llm_provider
from .prompt import render_prompt
from .validation import validate_inputs


def create_app(config: Optional[MagicOperatorConfig] = None) -> FastAPI:
    cfg = config or load_config()
    llm = build_llm_provider(cfg)
    jobs = JobStore()

    app = FastAPI()

    @app.exception_handler(InputValidationFailed)
    async def _on_invalid_input(request: Request, exc: InputValidationFailed):
        return _validation_error_response(exc)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    async def _run(payload: dict) -> dict:
        resolved = validate_inputs(payload, cfg.inputs)
        prompt = render_prompt(cfg.prompt_template, resolved)
        response = await llm.respond(prompt)
        return {cfg.output_name: response}

    if cfg.execution_mode == "sync_http":

        @app.post(SYNC_EXECUTE_PATH)
        async def execute(request: Request):
            return await _run(await request.json())

    else:  # async_http

        @app.post(ASYNC_START_PATH)
        async def start(request: Request):
            result = await _run(await request.json())
            job_id = jobs.create(result, cfg.async_poll_cycles)
            return {"id": job_id}

        @app.get(ASYNC_POLL_PATH)
        async def poll(job_id: str):
            status, result = jobs.poll(job_id)
            if status == "running":
                return {"status": "running"}
            return {"status": "done", "result": result}

    return app


def _validation_error_response(exc: InputValidationFailed):
    return JSONResponse(
        status_code=422,
        content={"error": "invalid_input", "missing": exc.missing, "message": exc.message},
    )

"""FastAPI application entry point for the AP Executor."""
import logging
from os import getenv
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from tomllib import loads as loads_toml

from ap_executor.api.v1.routes import router
from ap_executor.di import container_lifespan

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Retrieve current project version from toml (relative to this file)
pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
pyproject = loads_toml(pyproject_path.read_text())
project_version = pyproject["project"]["version"]

ROOT_PATH = getenv("ROOT_PATH", "")

app = FastAPI(
    title="AP Executor API",
    description="API for orchestrating Analytical Pattern operator execution via Consul-discovered operator implementations",
    version=project_version,
    lifespan=container_lifespan,
    root_path=ROOT_PATH,
)


@app.get("/")
def index():
    return {
        "service": "AP Executor",
        "version": app.version,
    }


app.include_router(router)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)

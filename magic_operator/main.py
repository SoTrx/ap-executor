"""Entrypoint: `uv run python magic_operator/main.py`.

Single-process `uvicorn.run()`, no `workers=` -- `magic_operator/jobs.py`'s
JobStore is in-memory and single-process only.
"""
import logging

import uvicorn
from dotenv import load_dotenv

from magic_operator.app import create_app
from magic_operator.config import load_config

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = load_config()  # fail fast, before binding a port
app = create_app(config)

if __name__ == "__main__":
    uvicorn.run(app, host=config.bind_host, port=config.bind_port)

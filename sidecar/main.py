"""Entrypoint: `uv run python sidecar/main.py`.

Runs as a single-process `uvicorn.run()` deliberately -- with no `workers=`.
Uvicorn's own SIGTERM/SIGINT handling then drives exactly one ASGI
`lifespan.shutdown` event, which is what triggers the Consul deregister call
in `sidecar/app.py`. Running multiple workers (or under a supervisor that
overrides uvicorn's own signal installation) would make each worker
register/deregister the same Consul service ID independently and race.
"""
import logging

import uvicorn
from dotenv import load_dotenv

from sidecar.app import create_app
from sidecar.config import load_config

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = load_config()  # fail fast, before binding a port or touching Consul
app = create_app(config)

if __name__ == "__main__":
    uvicorn.run(
        app,
        host=config.bind_host,
        port=config.bind_port,
        timeout_graceful_shutdown=10,
    )

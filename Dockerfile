# Install uv
FROM python:3.14-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Change the working directory to the `app` directory
WORKDIR /app

# Copy the lockfile and `pyproject.toml` into the image
COPY uv.lock /app/uv.lock
COPY pyproject.toml /app/pyproject.toml

# Install dependencies
RUN uv sync --frozen --no-install-project

# Copy the project into the image
COPY . /app

# Test stage, runs tests
FROM builder AS test

RUN uv sync --all-groups --frozen

CMD [ "uv", "run", "pytest", "-m", "not e2e" ]

# Actual production image
FROM builder AS prod

RUN uv sync --frozen

CMD [ "uv", "run", "python", "ap_executor/main.py" ]

# Operator sidecar image: transparent operator reverse-proxy + Consul self-registration
FROM builder AS sidecar

RUN uv sync --frozen

CMD [ "uv", "run", "python", "sidecar/main.py" ]

# Magic operator image: reference/test operator (validates inputs, calls an
# LLM through a pluggable provider) used by the e2e test suite. Built with
# the `llm` extra so it can optionally run with a real litellm-backed
# provider for manual testing, without a rebuild.
FROM builder AS magic-operator

RUN uv sync --frozen --extra llm

CMD [ "uv", "run", "python", "magic_operator/main.py" ]

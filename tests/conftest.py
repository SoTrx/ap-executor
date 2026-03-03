from dataclasses import dataclass
from pathlib import Path
from typing import AsyncGenerator
from urllib.parse import urlparse, urlunparse

import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool
from testcontainers.core.image import DockerImage
from testcontainers.postgres import PostgresContainer


@dataclass
class TestSchema:
    table: str = "assessment"
    schema: str = "mathe"


@pytest.fixture(scope="session")
def test_schema() -> TestSchema:
    return TestSchema()


@pytest.fixture(scope="function")
def postgres_container():
    """Spin up a PostgreSQL container from the local Dockerfile."""
    project_root = Path(__file__).parent.parent

    with DockerImage(
        path=str(project_root),
        dockerfile_path="dependencies/postgres-provsql/Dockerfile",
        tag="testdb:latest",
        clean_up=False,
        buildargs={"FIXTURES_PATH": "fixtures/postgres-seed"},
    ) as image:
        with PostgresContainer(
            image=str(image),
            username="provdemo",
            password="provdemo",
            dbname="mathe",
        ) as postgres:
            yield postgres


@pytest_asyncio.fixture
async def db_pool(postgres_container: PostgresContainer) -> AsyncGenerator[AsyncConnectionPool]:
    """Provides a connection pool to the test database."""
    qs = postgres_container.get_connection_url()
    parsed = urlparse(qs)
    scheme = parsed.scheme.split("+", 1)[0]
    qs = urlunparse(parsed._replace(scheme=scheme))

    pool = AsyncConnectionPool(
        conninfo=qs,
        min_size=1,
        max_size=5,
    )
    await pool.open()
    yield pool  # type: ignore
    await pool.close()


@pytest_asyncio.fixture
async def db_connection(db_pool: AsyncConnectionPool) -> AsyncGenerator[AsyncConnection]:
    """Returns a database connection from the pool."""
    async with db_pool.connection() as conn:
        yield conn

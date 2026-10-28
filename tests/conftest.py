"""Shared fixtures.

Unit tests need nothing. Integration tests (``tests/integration``) run against *real* PostgreSQL and
RabbitMQ started by testcontainers: mocks cannot prove ``SKIP LOCKED``, dead-lettering or TTL behaviour.
They are skipped automatically when Docker is not reachable.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from payment_service.config import (
    ApiSettings,
    DatabaseSettings,
    GatewaySettings,
    LoggingSettings,
    OutboxSettings,
    RabbitSettings,
    RetrySettings,
    Settings,
    WebhookSettings,
)
from payment_service.persistence import Database

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

    from testcontainers.community.postgres import PostgresContainer
    from testcontainers.community.rabbitmq import RabbitMqContainer

ROOT = Path(__file__).resolve().parent.parent

TEST_API_KEY = "test-api-key-0123456789"
TEST_WEBHOOK_SECRET = "test-webhook-secret-0123456789"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark everything under ``tests/integration`` so it can be selected with ``-m integration``."""
    for item in items:
        if "integration" in item.path.parts:
            item.add_marker(pytest.mark.integration)


def _docker_available() -> bool:
    try:
        import docker  # noqa: PLC0415

        docker.from_env().ping()
    except Exception:
        return False
    return True


@pytest.fixture(scope="session")
def docker_required() -> None:
    """Skip the requesting test when no Docker daemon is reachable."""
    if not _docker_available():
        pytest.skip("Docker is not available")


@pytest.fixture(scope="session")
def postgres_container(docker_required: None) -> Iterator[PostgresContainer]:
    """A throw-away PostgreSQL shared by the whole test session."""
    from testcontainers.community.postgres import PostgresContainer  # noqa: PLC0415

    with PostgresContainer("postgres:17-alpine", driver="asyncpg") as container:
        yield container


@pytest.fixture(scope="session")
def rabbit_container(docker_required: None) -> Iterator[RabbitMqContainer]:
    """A throw-away RabbitMQ shared by the whole test session."""
    from testcontainers.community.rabbitmq import RabbitMqContainer  # noqa: PLC0415

    with RabbitMqContainer("rabbitmq:4-management-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def database_url(postgres_container: PostgresContainer) -> str:
    """SQLAlchemy URL of the test PostgreSQL (asyncpg driver)."""
    return postgres_container.get_connection_url()


@pytest.fixture(scope="session")
def rabbit_url(rabbit_container: RabbitMqContainer) -> str:
    """AMQP URL of the test RabbitMQ."""
    host = rabbit_container.get_container_host_ip()
    port = rabbit_container.get_exposed_port(rabbit_container.port)
    return f"amqp://{rabbit_container.username}:{rabbit_container.password}@{host}:{port}/"


def alembic_config(url: str) -> Config:
    """Alembic config pointing at the project migrations and the given database."""
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    config.attributes["sqlalchemy_url"] = url
    return config


@pytest_asyncio.fixture(scope="session")
async def migrated_database_url(database_url: str) -> str:
    """Apply all migrations once per session; the URL of the migrated database."""
    # Alembic's env.py calls asyncio.run(), so it must run outside the test event loop.
    await asyncio.to_thread(command.upgrade, alembic_config(database_url), "head")
    return database_url


@pytest.fixture
def make_settings() -> Callable[..., Settings]:
    """Factory of complete ``Settings`` with test-friendly defaults (instant gateway, short retries)."""

    def factory(
        *,
        db_url: str = "postgresql+asyncpg://test:test@localhost:5432/test",
        rabbit_url: str = "amqp://guest:guest@localhost:5672/",
        **overrides: dict[str, Any],
    ) -> Settings:
        groups: dict[str, dict[str, Any]] = {
            "db": {"url": db_url},
            "rabbit": {"url": rabbit_url, "prefetch_count": 5},
            "api": {"key": TEST_API_KEY},
            "webhook": {"signing_secret": TEST_WEBHOOK_SECRET, "allow_private_networks": True},
            "retry": {"max_attempts": 3, "base_delay_seconds": 0.2, "backoff_factor": 2.0},
            "outbox": {"poll_interval_seconds": 0.05, "error_backoff_seconds": 0.1},
            "gateway": {"min_delay_seconds": 0.0, "max_delay_seconds": 0.0, "success_rate": 1.0},
            "logging": {"level": "WARNING", "json_format": False},
        }
        for name, values in overrides.items():
            groups[name].update(values)
        return Settings(
            db=DatabaseSettings(**groups["db"]),
            rabbit=RabbitSettings(**groups["rabbit"]),
            api=ApiSettings(**groups["api"]),
            webhook=WebhookSettings(**groups["webhook"]),
            retry=RetrySettings(**groups["retry"]),
            outbox=OutboxSettings(**groups["outbox"]),
            gateway=GatewaySettings(**groups["gateway"]),
            logging=LoggingSettings(**groups["logging"]),
        )

    return factory


@pytest_asyncio.fixture(scope="session")
async def database(migrated_database_url: str) -> AsyncIterator[Database]:
    """A ``Database`` bound to the migrated test PostgreSQL."""
    db = Database(DatabaseSettings(url=migrated_database_url))
    yield db
    await db.dispose()


@pytest_asyncio.fixture
async def clean_database(database: Database) -> Database:
    """The test database with empty tables (isolation between tests)."""
    async with database.engine.begin() as connection:
        await connection.execute(text("TRUNCATE TABLE payments, outbox"))
    return database

"""Alembic environment running migrations through the async engine.

The database URL comes from ``DB_URL`` (same variable as the application). Tests may inject a URL via
``config.attributes["sqlalchemy_url"]`` instead of touching the process environment.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine

from payment_service.config import DatabaseSettings
from payment_service.persistence import Base

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

config = context.config
target_metadata = Base.metadata


def _database_url() -> str:
    override = config.attributes.get("sqlalchemy_url")
    if override:
        return str(override)
    return DatabaseSettings().url.get_secret_value()  # type: ignore[call-arg]


def run_migrations_offline() -> None:
    """Emit SQL to stdout without a database connection (``alembic upgrade head --sql``)."""
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def _run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations on a dedicated, non-pooled async connection."""
    engine = create_async_engine(_database_url(), poolclass=pool.NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())

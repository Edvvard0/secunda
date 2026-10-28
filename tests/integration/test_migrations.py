from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING

import pytest_asyncio
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from conftest import alembic_config
from payment_service.persistence import Base

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.engine import Connection

    from payment_service.persistence import Database

APP_TABLES = {"payments", "outbox"}


def _with_database(url: str, database_name: str) -> str:
    """Same server and credentials as ``url``, another database (``str(url)`` would mask the password)."""
    return make_url(url).set(database=database_name).render_as_string(hide_password=False)


async def _table_names(url: str) -> set[str]:
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            return await connection.run_sync(lambda sync_conn: set(inspect(sync_conn).get_table_names()))
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def scratch_database_url(database_url: str) -> AsyncIterator[str]:
    """An empty throw-away database on the test server, dropped afterwards."""
    name = f"migrations_{uuid.uuid4().hex[:12]}"
    admin_engine = create_async_engine(database_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        async with admin_engine.connect() as connection:
            await connection.execute(text(f'CREATE DATABASE "{name}"'))
        yield _with_database(database_url, name)
    finally:
        async with admin_engine.connect() as connection:
            await connection.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        await admin_engine.dispose()


def test_migrations_have_a_single_head():
    heads = ScriptDirectory.from_config(alembic_config("postgresql+asyncpg://unused/unused")).get_heads()

    assert len(heads) == 1


async def test_migrated_schema_matches_the_orm_models(database: Database):
    def detect_drift(connection: Connection) -> list[object]:
        context = MigrationContext.configure(connection, opts={"compare_type": True})
        return compare_metadata(context, Base.metadata)

    async with database.engine.connect() as connection:
        drift = await connection.run_sync(detect_drift)

    assert drift == []


async def test_upgrade_downgrade_upgrade_round_trip(scratch_database_url: str):
    config = alembic_config(scratch_database_url)
    assert await _table_names(scratch_database_url) == set()

    await asyncio.to_thread(command.upgrade, config, "head")
    assert await _table_names(scratch_database_url) >= APP_TABLES

    await asyncio.to_thread(command.downgrade, config, "base")
    remaining = await _table_names(scratch_database_url)
    assert not remaining & APP_TABLES

    await asyncio.to_thread(command.upgrade, config, "head")
    assert await _table_names(scratch_database_url) >= APP_TABLES


async def test_downgrade_removes_every_object_created_by_upgrade(scratch_database_url: str):
    config = alembic_config(scratch_database_url)
    await asyncio.to_thread(command.upgrade, config, "head")
    await asyncio.to_thread(command.downgrade, config, "base")

    engine = create_async_engine(scratch_database_url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            leftovers = (
                (
                    await connection.execute(
                        text(
                            "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                            " WHERE n.nspname = 'public' AND c.relkind IN ('r', 'i', 'S', 'v')"
                            " AND c.relname <> 'alembic_version' AND c.relname <> 'alembic_version_pkc'"
                        )
                    )
                )
                .scalars()
                .all()
            )
    finally:
        await engine.dispose()

    assert leftovers == []

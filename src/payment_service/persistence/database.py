"""Database engine lifecycle."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from payment_service.persistence.uow import UnitOfWork

if TYPE_CHECKING:
    from payment_service.config import DatabaseSettings


class Database:
    """Owns the async engine and hands out units of work.

    One instance per process, created in the composition root and disposed on shutdown.
    """

    def __init__(self, settings: DatabaseSettings) -> None:
        self._engine: AsyncEngine = create_async_engine(
            settings.url.get_secret_value(),
            pool_size=settings.pool_size,
            max_overflow=settings.max_overflow,
            pool_timeout=settings.pool_timeout_seconds,
            pool_pre_ping=True,  # survive connections dropped by the server or a proxy
            echo=settings.echo,
        )
        self._session_factory = async_sessionmaker(
            self._engine,
            expire_on_commit=False,  # keep loaded attributes usable after commit (no implicit IO)
            autoflush=False,
        )

    @property
    def engine(self) -> AsyncEngine:
        """The underlying engine (used by migrations and tests)."""
        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """Factory of raw sessions."""
        return self._session_factory

    def unit_of_work(self) -> UnitOfWork:
        """Create a new, not yet entered, unit of work. Matches ``UnitOfWorkFactory``."""
        return UnitOfWork(self._session_factory)

    async def ping(self) -> None:
        """Run ``SELECT 1``; raises if the database is unreachable (readiness probe)."""
        async with self._engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def dispose(self) -> None:
        """Close all pooled connections."""
        await self._engine.dispose()

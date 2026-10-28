"""Unit of Work: one database transaction and the repositories that share it."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Self

from payment_service.persistence.repositories import OutboxRepository, PaymentRepository

if TYPE_CHECKING:
    from types import TracebackType

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class UnitOfWork:
    """Async context manager owning a single transaction.

    Usage::

        async with uow_factory() as uow:
            await uow.payments.insert_if_absent(...)
            uow.outbox.add(event)
            await uow.commit()

    Leaving the block without ``commit()`` (or because of an exception) rolls everything back, so a
    payment and its outbox event are always persisted together or not at all.
    """

    payments: PaymentRepository
    outbox: OutboxRepository

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> Self:
        session = self._session_factory()
        self._session = session
        self.payments = PaymentRepository(session)
        self.outbox = OutboxRepository(session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        session = self._require_session()
        try:
            if exc_type is not None:
                await session.rollback()
        finally:
            await session.close()
            self._session = None

    async def commit(self) -> None:
        """Commit the transaction."""
        await self._require_session().commit()

    async def rollback(self) -> None:
        """Roll the transaction back explicitly."""
        await self._require_session().rollback()

    def _require_session(self) -> AsyncSession:
        if self._session is None:
            msg = "UnitOfWork must be used as an async context manager"
            raise RuntimeError(msg)
        return self._session


type UnitOfWorkFactory = Callable[[], UnitOfWork]
"""Services receive a factory, not an instance: each operation gets its own short transaction."""

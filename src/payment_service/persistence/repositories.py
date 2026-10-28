"""Repositories: the only place that talks SQL.

``BaseRepository`` holds the logic shared by every aggregate; concrete repositories add the queries that
carry the reliability guarantees of the service (idempotent insert, compare-and-set update, row locking).
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence  # noqa: TC003
from datetime import datetime  # noqa: TC003
from decimal import Decimal  # noqa: TC003
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.persistence.base import Base
from payment_service.persistence.models import OutboxEvent, Payment

if TYPE_CHECKING:
    from sqlalchemy import CursorResult
    from sqlalchemy.ext.asyncio import AsyncSession

_MAX_ERROR_LENGTH = 2000


class BaseRepository[ModelT: Base]:
    """Generic repository: a session plus the operations every aggregate needs.

    A repository never commits; the :class:`~payment_service.persistence.uow.UnitOfWork` owns the
    transaction boundary.
    """

    model: type[ModelT]

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, entity_id: uuid.UUID) -> ModelT | None:
        """Load an entity by primary key (``None`` when absent)."""
        return await self._session.get(self.model, entity_id)

    def add(self, entity: ModelT) -> None:
        """Schedule a new entity for insertion on flush/commit."""
        self._session.add(entity)

    async def flush(self) -> None:
        """Send pending changes to the database without committing."""
        await self._session.flush()


class PaymentRepository(BaseRepository[Payment]):
    """Queries over the ``payments`` table."""

    model = Payment

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        """Return the payment created with ``key`` (``None`` when the key is new)."""
        result = await self._session.scalars(select(Payment).where(Payment.idempotency_key == key))
        return result.one_or_none()

    async def insert_if_absent(
        self,
        *,
        idempotency_key: str,
        request_fingerprint: str,
        amount: Decimal,
        currency: Currency,
        description: str,
        metadata: dict[str, Any],
        webhook_url: str,
    ) -> Payment | None:
        """Insert a payment unless ``idempotency_key`` is already taken.

        ``INSERT ... ON CONFLICT DO NOTHING`` makes concurrent requests with one key safe: the loser
        blocks until the winner commits, inserts nothing and gets ``None`` back, after which the caller
        reads the winner's row. No application-level locking or retry loops are needed.
        """
        stmt = (
            pg_insert(Payment)
            .values(
                {
                    Payment.id: uuid.uuid4(),
                    Payment.idempotency_key: idempotency_key,
                    Payment.request_fingerprint: request_fingerprint,
                    Payment.amount: amount,
                    Payment.currency: currency,
                    Payment.description: description,
                    Payment.metadata_: metadata,
                    Payment.webhook_url: webhook_url,
                }
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        result = await self._session.scalars(stmt)
        return result.one_or_none()

    async def finalize(
        self, payment_id: uuid.UUID, status: PaymentStatus, processed_at: datetime
    ) -> Payment | None:
        """Move a payment out of ``pending`` exactly once (compare-and-set).

        Returns the updated row, or ``None`` when somebody else already finalized the payment. Because
        the guard lives in the ``WHERE`` clause, duplicate message deliveries can never overwrite a
        final status.
        """
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=processed_at)
            .returning(Payment)
            .execution_options(populate_existing=True, synchronize_session=False)
        )
        result = await self._session.scalars(stmt)
        return result.one_or_none()

    async def mark_webhook_delivered(self, payment_id: uuid.UUID, delivered_at: datetime) -> None:
        """Remember that the receiver acknowledged the webhook."""
        await self._session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=delivered_at)
            .execution_options(synchronize_session=False)
        )


class OutboxRepository(BaseRepository[OutboxEvent]):
    """Queries over the ``outbox`` table."""

    model = OutboxEvent

    async def lock_unpublished(self, limit: int) -> Sequence[OutboxEvent]:
        """Claim up to ``limit`` unpublished events, oldest first.

        ``FOR UPDATE SKIP LOCKED`` lets several relay instances poll the same table without blocking
        each other or publishing the same row twice at the same time. Locks live until the
        surrounding transaction ends.
        """
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.scalars(stmt)
        return result.all()

    async def mark_published(self, event_id: uuid.UUID, published_at: datetime) -> None:
        """Record a successful publish."""
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(published_at=published_at, attempts=OutboxEvent.attempts + 1, last_error=None)
            .execution_options(synchronize_session=False)
        )

    async def record_failure(self, event_id: uuid.UUID, error: str) -> None:
        """Record a failed publish attempt (kept for diagnostics and alerting)."""
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(attempts=OutboxEvent.attempts + 1, last_error=error[:_MAX_ERROR_LENGTH])
            .execution_options(synchronize_session=False)
        )

    async def purge_published_before(self, cutoff: datetime) -> int:
        """Delete already published events older than ``cutoff``; returns the number of rows removed."""
        result = await self._session.execute(
            delete(OutboxEvent)
            .where(OutboxEvent.published_at.is_not(None), OutboxEvent.published_at < cutoff)
            .execution_options(synchronize_session=False)
        )
        return cast("CursorResult[Any]", result).rowcount

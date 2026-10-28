"""Outbox relay: moves events from the ``outbox`` table to the message broker."""

from __future__ import annotations

import logging
import time
from datetime import timedelta
from typing import TYPE_CHECKING

from payment_service.common.time import utcnow
from payment_service.common.worker import BackgroundWorker

if TYPE_CHECKING:
    from collections.abc import Callable

    from payment_service.config import OutboxSettings
    from payment_service.persistence.uow import UnitOfWorkFactory
    from payment_service.services.interfaces import EventPublisher

logger = logging.getLogger(__name__)


class OutboxRelay(BackgroundWorker):
    """Polling publisher of the transactional outbox.

    One iteration claims a batch of unpublished rows with ``SELECT ... FOR UPDATE SKIP LOCKED``,
    publishes them in creation order and marks them published, all in a single transaction:

    * ``SKIP LOCKED`` lets any number of relay instances run concurrently: each one takes rows the
      others have not locked, so no row is handled by two relays at the same time and nobody blocks;
    * the broker publish (with publisher confirm) happens *before* the commit of ``published_at``, so
      delivery is **at-least-once**: a crash between the confirm and the commit leaves the row
      unpublished and it is sent again. The consumer is idempotent and tolerates that duplicate;
    * a failed publish is recorded (``attempts``, ``last_error``) and committed together with the marks
      of the events published before it, then the error propagates so :class:`BackgroundWorker` backs
      off. Order is preserved because the failing event is retried first on the next iteration.

    Published rows are deleted after ``OUTBOX_RETENTION_HOURS`` (see :meth:`purge_published`).
    """

    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        publisher: EventPublisher,
        settings: OutboxSettings,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create the relay.

        Args:
            uow_factory: Source of short transactions.
            publisher: Broker adapter; ``publish`` must return only after the broker confirmed.
            settings: Batch size, polling, backoff and retention parameters.
            monotonic: Clock used to schedule purges (injectable for tests).
        """
        super().__init__(
            name="outbox-relay",
            poll_interval_seconds=settings.poll_interval_seconds,
            error_backoff_seconds=settings.error_backoff_seconds,
        )
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._settings = settings
        self._monotonic = monotonic
        # The first purge is due one interval after start, not at startup.
        self._next_purge_at = monotonic() + settings.purge_interval_seconds

    async def run_once(self) -> bool:
        """Publish one batch of unpublished events.

        Returns:
            ``True`` if a batch was published, ``False`` if the outbox had nothing to send.

        Raises:
            Exception: whatever the publisher raised for the first failing event; the failure is
                recorded in the row and the events before it stay published.
        """
        await self._purge_if_due()
        async with self._uow_factory() as uow:
            events = await uow.outbox.lock_unpublished(self._settings.batch_size)
            if not events:
                return False
            for event in events:
                try:
                    await self._publisher.publish(
                        routing_key=event.event_type,
                        payload=event.payload,
                        message_id=str(event.id),
                        correlation_id=event.correlation_id,
                    )
                except Exception as exc:
                    # Any publisher failure (network, nack, unroutable, ...) is handled the same way:
                    # remember it for diagnostics, keep the marks of the events published before it,
                    # and re-raise so the worker loop logs it and backs off.
                    logger.warning("Publishing outbox event %s (%s) failed", event.id, event.event_type)
                    await uow.outbox.record_failure(event.id, f"{type(exc).__name__}: {exc}")
                    await uow.commit()
                    raise
                await uow.outbox.mark_published(event.id, utcnow())
            await uow.commit()
        logger.debug("Published %d outbox events", len(events))
        return True

    async def purge_published(self) -> int:
        """Delete published events older than ``OUTBOX_RETENTION_HOURS``.

        Returns:
            The number of deleted rows.
        """
        cutoff = utcnow() - timedelta(hours=self._settings.retention_hours)
        async with self._uow_factory() as uow:
            deleted = await uow.outbox.purge_published_before(cutoff)
            await uow.commit()
        if deleted:
            logger.info("Purged %d published outbox events older than %s", deleted, cutoff.isoformat())
        return deleted

    async def _purge_if_due(self) -> None:
        """Run the retention purge at most once per ``OUTBOX_PURGE_INTERVAL_SECONDS``.

        Checked on every iteration (not only when idle) so that a permanently busy outbox is still
        cleaned up. Housekeeping must never stop delivery, hence failures are logged, not raised.
        """
        now = self._monotonic()
        if now < self._next_purge_at:
            return
        self._next_purge_at = now + self._settings.purge_interval_seconds
        try:
            await self.purge_published()
        except Exception:
            # Boundary of a best-effort housekeeping task: log with traceback, retry next interval.
            logger.exception("Purging published outbox events failed")

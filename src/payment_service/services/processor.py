"""Idempotent processing of a payment: charge, persist the outcome, notify the client."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from payment_service.common.time import utcnow
from payment_service.domain.enums import PaymentStatus
from payment_service.domain.exceptions import PaymentNotFoundError
from payment_service.services.dto import PaymentDTO

if TYPE_CHECKING:
    import uuid

    from payment_service.persistence.uow import UnitOfWorkFactory
    from payment_service.services.interfaces import PaymentGateway, WebhookSender

logger = logging.getLogger(__name__)


class PaymentProcessor:
    """Processes one payment; safe for duplicate, concurrent and redelivered messages.

    The message broker delivers at-least-once, so :meth:`process` can run several times (even at the
    same moment) for the same payment. The design that keeps this harmless:

    * **No external call happens inside a database transaction.** The gateway needs seconds and the
      webhook receiver can hang up to its timeout; holding a transaction (and a pooled connection)
      that long would exhaust the pool and keep row locks. Every database step below is its own short
      unit of work.
    * **The outcome is applied once via compare-and-set** (``UPDATE ... WHERE status = 'pending'``).
      Of several processors that charged the same payment, exactly one wins; the others see ``None``,
      reload the stored row and continue with the *winning* status, so the client never receives a
      status that differs from the one persisted.
    * **Progress is persisted between steps.** The final status is saved before the webhook is sent, so
      when the webhook fails and the message is redelivered, the retry skips the gateway and repeats
      only the webhook. ``webhook_delivered_at`` records that the receiver acknowledged it.

    The webhook is therefore *at-least-once*: if two processors race past the CAS, or the process dies
    between the receiver's 2xx and ``mark_webhook_delivered``, it is sent twice. Receivers are expected
    to deduplicate by ``payment_id``.
    """

    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        gateway: PaymentGateway,
        webhook_sender: WebhookSender,
    ) -> None:
        self._uow_factory = uow_factory
        self._gateway = gateway
        self._webhook_sender = webhook_sender

    async def process(self, payment_id: uuid.UUID) -> None:
        """Bring the payment to "final status stored and webhook delivered".

        Every step is skipped when its effect is already stored, so calling this again after a partial
        failure resumes where the previous attempt stopped.

        Args:
            payment_id: id of the payment to process.

        Raises:
            PaymentNotFoundError: if the payment does not exist (it may become visible later, so the
                caller retries).
            ValueError: if the gateway returned a non-final status; the payment stays ``pending``.
            WebhookDeliveryError: if the webhook could not be delivered; the final status is already
                saved, so a retry will not charge again.
        """
        payment = await self._load(payment_id)
        did_work = False

        if payment.status is PaymentStatus.PENDING:
            did_work = True
            status = await self._gateway.charge(payment)  # outside any transaction, see class docstring
            if not status.is_final:
                msg = f"Gateway returned non-final status {status.value!r} for payment {payment_id}"
                raise ValueError(msg)
            payment = await self._finalize(payment_id, status)

        if payment.webhook_delivered_at is None:
            did_work = True
            await self._webhook_sender.send(payment)  # outside any transaction, see class docstring
            await self._mark_webhook_delivered(payment_id)

        if did_work:
            logger.info(
                "payment processed",
                extra={"payment_id": str(payment_id), "status": payment.status.value},
            )
        else:
            logger.info("duplicate delivery ignored", extra={"payment_id": str(payment_id)})

    async def _load(self, payment_id: uuid.UUID) -> PaymentDTO:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(payment_id)
            if payment is None:
                msg = f"Payment {payment_id} not found"
                raise PaymentNotFoundError(msg)
            return PaymentDTO.from_model(payment)

    async def _finalize(self, payment_id: uuid.UUID, status: PaymentStatus) -> PaymentDTO:
        """Store the outcome with CAS and return the row that is authoritative afterwards."""
        async with self._uow_factory() as uow:
            finalized = await uow.payments.finalize(payment_id, status, utcnow())
            if finalized is not None:
                result = PaymentDTO.from_model(finalized)
                await uow.commit()
                return result
            # Lost the race: the UPDATE only returns 0 rows after the winner committed (it waits on the
            # row lock), so the winner's final state is visible to this read.
            current = await uow.payments.get(payment_id)
            if current is None:
                msg = f"Payment {payment_id} not found"
                raise PaymentNotFoundError(msg)
            return PaymentDTO.from_model(current)

    async def _mark_webhook_delivered(self, payment_id: uuid.UUID) -> None:
        async with self._uow_factory() as uow:
            await uow.payments.mark_webhook_delivered(payment_id, utcnow())
            await uow.commit()

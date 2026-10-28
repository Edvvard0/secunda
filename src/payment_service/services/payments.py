"""Use cases around accepting and reading payments."""

from __future__ import annotations

import logging
import re
import uuid
from typing import TYPE_CHECKING

from payment_service.common.time import utcnow
from payment_service.domain.events import PaymentCreatedEvent
from payment_service.domain.exceptions import (
    IdempotencyKeyReuseError,
    InvalidIdempotencyKeyError,
    PaymentNotFoundError,
)
from payment_service.persistence.models import OutboxEvent
from payment_service.services.dto import CreatePaymentResult, PaymentDTO

if TYPE_CHECKING:
    from payment_service.persistence.uow import UnitOfWorkFactory
    from payment_service.services.dto import CreatePaymentCommand

logger = logging.getLogger(__name__)

_MAX_KEY_LENGTH = 255
_MAX_CORRELATION_ID_LENGTH = 64
"""Width of ``outbox.correlation_id``; a longer client-supplied request id must not fail the insert."""
_KEY_PATTERN = re.compile(r"[\x21-\x7e]+")
"""Printable ASCII without spaces. Matched with ``fullmatch``: ``$`` would accept a trailing newline."""


def validate_idempotency_key(key: str) -> None:
    """Check the ``Idempotency-Key`` format.

    Args:
        key: raw header value; an empty string stands for "header missing".

    Raises:
        InvalidIdempotencyKeyError: if the key is empty, longer than 255 characters or contains anything
            but printable ASCII characters (spaces and control characters included).
    """
    if not key:
        msg = "Idempotency-Key header is required and must not be empty"
        raise InvalidIdempotencyKeyError(msg)
    if len(key) > _MAX_KEY_LENGTH:
        msg = f"Idempotency-Key must be at most {_MAX_KEY_LENGTH} characters long"
        raise InvalidIdempotencyKeyError(msg)
    if _KEY_PATTERN.fullmatch(key) is None:
        msg = "Idempotency-Key must consist of printable ASCII characters without spaces"
        raise InvalidIdempotencyKeyError(msg)


class PaymentService:
    """Accepts payment requests idempotently and serves reads."""

    def __init__(self, uow_factory: UnitOfWorkFactory) -> None:
        self._uow_factory = uow_factory

    async def create(
        self,
        command: CreatePaymentCommand,
        *,
        idempotency_key: str,
        correlation_id: str | None = None,
    ) -> CreatePaymentResult:
        """Create a payment, or return the one already created with ``idempotency_key``.

        The payment row and its ``payments.new`` outbox event are written in **one transaction**, so the
        event exists if and only if the payment was committed. A repeated request is detected by the
        unique key (``INSERT ... ON CONFLICT DO NOTHING``), which is safe under concurrency: a parallel
        request with the same key waits for the first transaction and then inserts nothing.

        Args:
            command: validated request body.
            idempotency_key: value of the ``Idempotency-Key`` header.
            correlation_id: request id stored with the outbox event for end-to-end tracing.

        Returns:
            The payment and whether this call created it (``False`` for a replay).

        Raises:
            InvalidIdempotencyKeyError: if the key has an invalid format.
            IdempotencyKeyReuseError: if the key was used before with a different request body.
        """
        validate_idempotency_key(idempotency_key)
        fingerprint = command.fingerprint()
        stored_correlation_id = correlation_id[:_MAX_CORRELATION_ID_LENGTH] if correlation_id else None

        async with self._uow_factory() as uow:
            payment = await uow.payments.insert_if_absent(
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
                amount=command.amount,
                currency=command.currency,
                description=command.description,
                metadata=command.metadata,
                webhook_url=command.webhook_url,
            )
            if payment is not None:
                event = PaymentCreatedEvent(
                    event_id=uuid.uuid4(), payment_id=payment.id, occurred_at=utcnow()
                )
                uow.outbox.add(
                    OutboxEvent(
                        id=event.event_id,
                        event_type=PaymentCreatedEvent.EVENT_TYPE,
                        aggregate_id=payment.id,
                        payload=event.to_payload(),
                        correlation_id=stored_correlation_id,
                    )
                )
                result = CreatePaymentResult(PaymentDTO.from_model(payment), created=True)
                await uow.commit()
            else:
                # READ COMMITTED: every statement sees a fresh snapshot, so the row of the concurrent
                # transaction we just waited for is visible here.
                existing = await uow.payments.get_by_idempotency_key(idempotency_key)
                if existing is None:
                    msg = "Idempotency key conflicted on insert but the payment is not visible"
                    raise RuntimeError(msg)
                if existing.request_fingerprint != fingerprint:
                    raise IdempotencyKeyReuseError
                result = CreatePaymentResult(PaymentDTO.from_model(existing), created=False)

        logger.info(
            "payment create handled",
            # "created" would clash with the LogRecord attribute of the same name.
            extra={"payment_id": str(result.payment.id), "payment_created": result.created},
        )
        return result

    async def get(self, payment_id: uuid.UUID) -> PaymentDTO:
        """Return a payment by id.

        Raises:
            PaymentNotFoundError: if there is no such payment.
        """
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(payment_id)
            if payment is None:
                msg = f"Payment {payment_id} not found"
                raise PaymentNotFoundError(msg)
            return PaymentDTO.from_model(payment)

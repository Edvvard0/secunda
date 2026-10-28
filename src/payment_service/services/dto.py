"""Data transfer objects exchanged between the API/consumer layers and the services.

Plain frozen dataclasses: no ORM instances leak out of the persistence layer, so callers can not trigger
lazy loading or accidentally mutate a tracked entity.
"""

from __future__ import annotations

import hashlib
import json
import uuid  # noqa: TC003 - dataclass annotations are inspected at runtime
from dataclasses import dataclass
from datetime import datetime  # noqa: TC003
from decimal import Decimal  # noqa: TC003
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from payment_service.domain.enums import Currency, PaymentStatus
    from payment_service.persistence.models import Payment


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    """Validated input of the "create payment" use case."""

    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str

    def fingerprint(self) -> str:
        """Return a stable SHA-256 of the command, independent of key order and ``10`` vs ``10.00``.

        It is stored next to the idempotency key so that re-using a key with a *different* body can be
        told apart from a legitimate retry of the same request.
        """
        canonical = json.dumps(
            {
                "amount": f"{self.amount:.2f}",
                "currency": str(self.currency),
                "description": self.description,
                "metadata": self.metadata,
                "webhook_url": self.webhook_url,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        return hashlib.sha256(canonical.encode("ascii")).hexdigest()


@dataclass(frozen=True, slots=True)
class PaymentDTO:
    """Read model of a payment."""

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> Self:
        """Build the read model from an ORM entity."""
        return cls(
            id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=dict(payment.metadata_),
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
            webhook_delivered_at=payment.webhook_delivered_at,
        )


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    """Outcome of the "create payment" use case."""

    payment: PaymentDTO
    created: bool
    """``False`` when an earlier request with the same idempotency key is being replayed."""

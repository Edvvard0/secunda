"""ORM models: ``payments`` and the transactional ``outbox``."""

from __future__ import annotations

import uuid
from datetime import datetime  # noqa: TC003 - SQLAlchemy resolves annotations at runtime
from decimal import Decimal  # noqa: TC003 - SQLAlchemy resolves annotations at runtime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.persistence.base import Base


def _enum_values(enum_cls: type[Currency] | type[PaymentStatus]) -> list[str]:
    """Persist enum *values* (``pending``) rather than member names (``PENDING``)."""
    return [member.value for member in enum_cls]


class Payment(Base):
    """A payment request and its processing state."""

    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key"),
        CheckConstraint("amount > 0", name="amount_positive"),
        # Supports monitoring queries such as "payments stuck in pending for more than N minutes".
        Index("ix_payments_status_created_at", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(
        Enum(
            Currency,
            name="currency",
            native_enum=False,
            length=3,
            create_constraint=True,
            values_callable=_enum_values,
        )
    )
    description: Mapped[str] = mapped_column(Text)
    # ``metadata`` is reserved by SQLAlchemy's declarative base, hence the attribute alias.
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, default=dict, server_default=text("'{}'::jsonb")
    )
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(
            PaymentStatus,
            name="payment_status",
            native_enum=False,
            length=16,
            create_constraint=True,
            values_callable=_enum_values,
        ),
        default=PaymentStatus.PENDING,
        server_default=PaymentStatus.PENDING.value,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255))
    request_fingerprint: Mapped[str] = mapped_column(String(64))
    """SHA-256 of the canonical request body: detects key reuse with a different payload."""
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    """Set once the receiver acknowledged the webhook with 2xx; lets redeliveries skip a resend."""


class OutboxEvent(Base):
    """A domain event waiting to be relayed to the broker (transactional outbox)."""

    __tablename__ = "outbox"
    __table_args__ = (
        # Tiny index that only contains the rows the relay polls for, regardless of table size.
        Index(
            "ix_outbox_unpublished",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    event_type: Mapped[str] = mapped_column(String(100))
    """Routing key the event is published with."""
    aggregate_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    correlation_id: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text)

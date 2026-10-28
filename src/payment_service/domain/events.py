"""Integration events published to the message broker.

Events are *notifications*: they carry identifiers only, the consumer loads the current state from the
database. That keeps messages small, avoids stale data in redelivered messages, and never puts sensitive
payment details onto the broker.
"""

from __future__ import annotations

from typing import ClassVar
from uuid import UUID  # noqa: TC003 - needed at runtime by pydantic

from pydantic import AwareDatetime, BaseModel, ConfigDict


class PaymentCreatedEvent(BaseModel):
    """A new payment was accepted and must be processed (routing key ``payments.new``)."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    EVENT_TYPE: ClassVar[str] = "payments.new"
    """Routing key and outbox ``event_type``; also the name of the work queue."""

    version: int = 1
    """Schema version: bump on incompatible changes, consumers may route on it."""
    event_id: UUID
    """Unique id of this event (equals the outbox row id): the message-level deduplication key."""
    payment_id: UUID
    occurred_at: AwareDatetime

    def to_payload(self) -> dict[str, object]:
        """Serialize to a JSON-compatible dict (used for the outbox ``payload`` column)."""
        return self.model_dump(mode="json")

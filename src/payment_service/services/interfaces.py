"""Ports: abstractions the services depend on (Dependency Inversion).

Concrete adapters live elsewhere (RabbitMQ publisher, HTTP webhook sender, gateway emulation), which
keeps the business logic testable without a broker, a network or randomness.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

    from payment_service.domain.enums import PaymentStatus
    from payment_service.services.dto import PaymentDTO


class PaymentGateway(ABC):
    """An external payment provider."""

    @abstractmethod
    async def charge(self, payment: PaymentDTO) -> PaymentStatus:
        """Attempt the payment and return its final status (``SUCCEEDED`` or ``FAILED``).

        A declined payment is a *business outcome* and is returned, not raised. Raise only for
        infrastructure problems, which are retried by the messaging layer.
        """


class WebhookSender(ABC):
    """Delivers the result of a payment to the client's ``webhook_url``."""

    @abstractmethod
    async def send(self, payment: PaymentDTO) -> None:
        """Deliver the webhook.

        Raises:
            WebhookDeliveryError: on any failure, including a non-2xx answer.
        """


class EventPublisher(ABC):
    """Publishes events to the message broker with delivery confirmation."""

    @abstractmethod
    async def publish(
        self,
        *,
        routing_key: str,
        payload: Mapping[str, Any],
        message_id: str,
        correlation_id: str | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        """Publish one event; return only after the broker confirmed it.

        Raises:
            Exception: if the broker did not confirm the message. The caller keeps the event and
                retries later (at-least-once).
        """

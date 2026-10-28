"""Publishing to RabbitMQ: broker factory, the shared confirmed-publish path and the outbox adapter.

Every message the service puts on the broker (outbox events, retry and dead-letter republishes) goes
through :class:`ConfirmedPublisher`, so the delivery guarantees are defined in exactly one place:
persistent, ``mandatory`` and acknowledged by the broker (publisher confirm). A failure, a nack or an
unroutable return surfaces as an exception and the caller decides what that means.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Final

from faststream.middlewares import AckPolicy
from faststream.rabbit import Channel, RabbitBroker

from payment_service.services.interfaces import EventPublisher

if TYPE_CHECKING:
    from collections.abc import Mapping

    from faststream.rabbit import RabbitExchange

    from payment_service.config import RabbitSettings

JSON_CONTENT_TYPE: Final = "application/json"
APP_ID: Final = "payment-service"
DEFAULT_CONFIRM_TIMEOUT_SECONDS: Final = 30.0
"""How long to wait for the broker's confirm; a stuck broker must not block the caller forever."""


def create_broker(settings: RabbitSettings) -> RabbitBroker:
    """Create the (not yet connected) broker used by both worker processes.

    * ``publisher_confirms`` makes a publish return only after the broker took responsibility for the message;
    * ``on_return_raises`` turns an unroutable ``mandatory`` message into an exception. It defaults to
      ``False`` in aio-pika, which would silently drop such a message;
    * ``AckPolicy.REJECT_ON_ERROR`` acknowledges after the handler returned and rejects (without requeue,
      so the queue's dead-letter exchange takes over) when it raised, which is what happens to payloads
      that cannot be parsed.
    """
    return RabbitBroker(
        settings.url.get_secret_value(),
        default_channel=Channel(
            prefetch_count=settings.prefetch_count,
            publisher_confirms=True,
            on_return_raises=True,
        ),
        ack_policy=AckPolicy.REJECT_ON_ERROR,
        app_id=APP_ID,
        # A standard-library logger, so access lines share the process log format (see logging_config).
        logger=logging.getLogger("faststream.access.rabbit"),
    )


class ConfirmedPublisher:
    """Sends one already serialised message with the delivery guarantees described in the module doc."""

    def __init__(
        self,
        broker: RabbitBroker,
        *,
        confirm_timeout_seconds: float = DEFAULT_CONFIRM_TIMEOUT_SECONDS,
    ) -> None:
        self._broker = broker
        self._confirm_timeout_seconds = confirm_timeout_seconds

    async def send(
        self,
        *,
        exchange: RabbitExchange,
        routing_key: str,
        body: bytes,
        message_id: str | None,
        correlation_id: str | None,
        message_type: str,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        """Publish a JSON ``body`` and return only after the broker confirmed it.

        Raises:
            Exception: if the broker nacked the message, did not answer in time, or could not route it to
                any queue (aio-pika's ``DeliveryError``); the connection errors of the client library also
                propagate.
        """
        await self._broker.publish(
            body,
            exchange=exchange,
            routing_key=routing_key,
            persist=True,
            mandatory=True,
            timeout=self._confirm_timeout_seconds,
            headers=dict(headers) if headers else None,
            message_id=message_id,
            correlation_id=correlation_id,
            content_type=JSON_CONTENT_TYPE,
            message_type=message_type,
        )


class RabbitEventPublisher(EventPublisher):
    """:class:`EventPublisher` port implemented over RabbitMQ: what the outbox relay publishes with."""

    def __init__(
        self,
        broker: RabbitBroker,
        exchange: RabbitExchange,
        *,
        confirm_timeout_seconds: float = DEFAULT_CONFIRM_TIMEOUT_SECONDS,
    ) -> None:
        self._sender = ConfirmedPublisher(broker, confirm_timeout_seconds=confirm_timeout_seconds)
        self._exchange = exchange

    async def publish(
        self,
        *,
        routing_key: str,
        payload: Mapping[str, Any],
        message_id: str,
        correlation_id: str | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        """Publish ``payload`` as JSON to the configured exchange; the routing key is the message type."""
        await self._sender.send(
            exchange=self._exchange,
            routing_key=routing_key,
            body=json.dumps(payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(),
            message_id=message_id,
            correlation_id=correlation_id,
            message_type=routing_key,
            headers=headers,
        )

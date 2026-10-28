"""The ``payments.new`` consumer: handler, subscription and FastStream application assembly."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from faststream import FastStream
from faststream.exceptions import NackMessage
from faststream.middlewares import AckPolicy
from faststream.rabbit import RabbitMessage  # noqa: TC002 - FastStream resolves the annotation at runtime

from payment_service.domain.events import PaymentCreatedEvent  # noqa: TC001 - needed at runtime by FastStream
from payment_service.logging_config import set_correlation_id
from payment_service.messaging.retry import parse_attempt

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from faststream.rabbit import RabbitBroker

    from payment_service.messaging.retry import RetryPolicy, RetryRouter
    from payment_service.messaging.topology import Topology
    from payment_service.services.processor import PaymentProcessor

logger = logging.getLogger(__name__)


class PaymentConsumer:
    """Processes ``payments.new`` messages and turns failures into retries or dead letters.

    The method is a FastStream handler: the body is validated into :class:`PaymentCreatedEvent` before
    this code runs, so a payload that does not parse never reaches it. That exception makes the
    ``REJECT_ON_ERROR`` policy reject the message and the work queue's dead-letter exchange moves it to the
    DLQ. A failure of the *processing* is different: it is expected, so it is caught here and the message
    is republished (wait queue or DLQ) and then acknowledged by returning normally.
    """

    def __init__(
        self, processor: PaymentProcessor, retry_router: RetryRouter, retry_policy: RetryPolicy
    ) -> None:
        self._processor = processor
        self._retry_router = retry_router
        self._retry_policy = retry_policy

    async def handle(self, event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        """Process one event; return normally (so the message is acked) unless it cannot be re-routed.

        Raises:
            NackMessage: if processing failed *and* the republish failed too. The original is then
                returned to the queue instead of being acknowledged, so nothing is lost.
        """
        set_correlation_id(message.correlation_id)
        try:
            attempt = parse_attempt(message.headers)
            try:
                await self._processor.process(event.payment_id)
            except Exception as error:  # the worker boundary: any failure becomes a retry or a dead letter
                await self._reroute(event, message, attempt, error)
        finally:
            set_correlation_id(None)

    async def _reroute(
        self,
        event: PaymentCreatedEvent,
        message: RabbitMessage,
        attempt: int,
        error: Exception,
    ) -> None:
        try:
            outcome = await self._retry_router.handle_failure(
                body=message.body,
                headers=message.headers,
                message_id=message.message_id,
                correlation_id=message.correlation_id,
                attempt=attempt,
                error=error,
            )
        except Exception as republish_error:
            logger.exception(
                "Cannot re-route the failed message, returning it to the queue: "
                "payment_id=%s attempt=%d error_type=%s",
                event.payment_id,
                attempt,
                type(error).__name__,
            )
            raise NackMessage from republish_error

        logger.warning(
            "Payment processing failed: payment_id=%s attempt=%d/%d outcome=%s error_type=%s",
            event.payment_id,
            attempt,
            self._retry_policy.max_attempts,
            outcome,
            type(error).__name__,
            # Retries are routine and stay one line; the final failure is worth a traceback.
            exc_info=error if outcome == "dead-letter" else None,
        )


def register_consumer(broker: RabbitBroker, topology: Topology, consumer: PaymentConsumer) -> None:
    """Subscribe ``consumer.handle`` to the work queue.

    The queue object comes from the topology, so the subscriber declares it with identical arguments
    (dead-letter exchange and routing key included) and binds it to the ``payments`` exchange.
    """
    subscriber = broker.subscriber(
        topology.work_queue,
        topology.payments_exchange,
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
    subscriber(consumer.handle)


def create_consumer_app(broker: RabbitBroker, topology: Topology, consumer: PaymentConsumer) -> FastStream:
    """Assemble the FastStream application of the consumer process.

    FastStream starts subscribers inside ``broker.start()``, i.e. before any ``after_startup`` hook and
    after every ``on_startup`` hook (which run *before* the connection exists). The lifespan is the only
    place that sits in between the two requirements, so the topology is declared there: connected, but
    not yet consuming. Hence a message that is dead-lettered or republished can never meet a missing queue.
    """

    @asynccontextmanager
    async def lifespan() -> AsyncIterator[None]:
        await broker.connect()
        try:
            await topology.declare(broker)
        except BaseException:
            await broker.stop()  # FastStream only stops a broker it has started
            raise
        yield

    register_consumer(broker, topology, consumer)
    return FastStream(broker, lifespan=lifespan)

"""RabbitMQ topology: the single source of truth for exchange/queue names, arguments and bindings.

Both worker processes call :meth:`Topology.declare` at start-up, before they publish or consume, so the
broker is configured identically no matter which process starts first. The layout (see
``docs/ARCHITECTURE.md``, section 6)::

    payments        --payments.new-->         payments.new                (work queue)
    payments.retry  --payments.new.retry.n--> payments.new.retry.n        (wait queue, TTL, DLX -> payments)
    payments.dlx    --payments.new.dlq-->     payments.new.dlq            (dead letters, kept for humans)

The work queue dead-letters rejected messages (poison messages) into ``payments.dlx``; a wait queue
dead-letters expired messages back into ``payments``.

Queue arguments are part of a queue's identity in RabbitMQ: redeclaring an existing queue with *different*
arguments fails with ``PRECONDITION_FAILED``. Changing ``RETRY_BASE_DELAY_SECONDS``/``RETRY_BACKOFF_FACTOR``
therefore requires deleting the wait queues (they are transient by nature) before the next start.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, cast

from faststream.rabbit import ExchangeType, QueueType, RabbitExchange, RabbitQueue

from payment_service.domain.events import PaymentCreatedEvent
from payment_service.messaging.retry import RetryPolicy

if TYPE_CHECKING:
    from faststream.rabbit import RabbitBroker
    from faststream.rabbit.schemas.queue import ClassicQueueArgs, QuorumQueueArgs

    from payment_service.config import RabbitSettings, RetrySettings

PAYMENTS_EXCHANGE: Final = "payments"
RETRY_EXCHANGE: Final = "payments.retry"
DEAD_LETTER_EXCHANGE: Final = "payments.dlx"

WORK_QUEUE: Final = PaymentCreatedEvent.EVENT_TYPE
"""``payments.new``: the queue name doubles as the routing key of new-payment events."""
WORK_ROUTING_KEY: Final = PaymentCreatedEvent.EVENT_TYPE
DEAD_LETTER_QUEUE: Final = f"{WORK_QUEUE}.dlq"
DEAD_LETTER_ROUTING_KEY: Final = DEAD_LETTER_QUEUE


def retry_queue_name(attempt: int) -> str:
    """Return the name (also the routing key) of the wait queue used after attempt ``attempt`` failed."""
    return f"{WORK_QUEUE}.retry.{attempt}"


class Topology:
    """Exchanges, queues and bindings of the payment pipeline, built from settings.

    The ``RabbitExchange``/``RabbitQueue`` attributes are plain declarations: pass the same objects to
    the subscriber and to publishers so every party refers to the very same arguments.
    """

    def __init__(self, rabbit: RabbitSettings, retry: RetrySettings) -> None:
        self._policy = RetryPolicy.from_settings(retry)
        self._queue_type = QueueType(rabbit.queue_type)

        self.payments_exchange = RabbitExchange(PAYMENTS_EXCHANGE, type=ExchangeType.DIRECT, durable=True)
        self.retry_exchange = RabbitExchange(RETRY_EXCHANGE, type=ExchangeType.DIRECT, durable=True)
        self.dead_letter_exchange = RabbitExchange(
            DEAD_LETTER_EXCHANGE, type=ExchangeType.DIRECT, durable=True
        )

        self.work_queue = self._queue(
            WORK_QUEUE,
            routing_key=WORK_ROUTING_KEY,
            dead_letter_exchange=DEAD_LETTER_EXCHANGE,
            dead_letter_routing_key=DEAD_LETTER_ROUTING_KEY,
        )
        self.dead_letter_queue = self._queue(DEAD_LETTER_QUEUE, routing_key=DEAD_LETTER_ROUTING_KEY)
        # Wait queues: element ``n - 1`` serves attempt ``n``; empty when ``max_attempts == 1``.
        self.retry_queues: tuple[RabbitQueue, ...] = tuple(
            self._queue(
                retry_queue_name(attempt),
                routing_key=self.retry_routing_key(attempt),
                dead_letter_exchange=PAYMENTS_EXCHANGE,
                dead_letter_routing_key=WORK_ROUTING_KEY,
                message_ttl_ms=self.retry_ttl_ms(attempt),
            )
            for attempt in range(1, self._policy.max_attempts)
        )

    @property
    def work_routing_key(self) -> str:
        """Routing key (and message ``type``) of new-payment events."""
        return WORK_ROUTING_KEY

    @property
    def dead_letter_routing_key(self) -> str:
        """Routing key that leads from the dead-letter exchange into the DLQ."""
        return DEAD_LETTER_ROUTING_KEY

    def retry_routing_key(self, attempt: int) -> str:
        """Return the key that routes a message to the wait queue used after attempt ``attempt`` failed.

        Raises:
            ValueError: if no wait queue exists for ``attempt`` (it must be in ``1..max_attempts-1``).
        """
        self._check_retry_attempt(attempt)
        return retry_queue_name(attempt)

    def retry_ttl_ms(self, attempt: int) -> int:
        """Return the TTL of the wait queue for ``attempt`` in milliseconds: ``base * factor**(n-1)``.

        Raises:
            ValueError: if no wait queue exists for ``attempt`` (it must be in ``1..max_attempts-1``).
        """
        self._check_retry_attempt(attempt)
        return max(1, round(self._policy.delay_seconds(attempt) * 1000))

    async def declare(self, broker: RabbitBroker) -> None:
        """Declare every exchange, queue and binding; safe to call any number of times, from any process.

        The broker must be connected. Call it *before* consumption starts: a message dead-lettered or
        republished into a queue that does not exist yet would be dropped or returned as unroutable.

        Raises:
            Exception: whatever the broker raises, e.g. ``PRECONDITION_FAILED`` when a queue already exists
                with different arguments.
        """
        payments = await broker.declare_exchange(self.payments_exchange)
        retry = await broker.declare_exchange(self.retry_exchange)
        dead_letter = await broker.declare_exchange(self.dead_letter_exchange)

        dlq = await broker.declare_queue(self.dead_letter_queue)
        await dlq.bind(dead_letter, routing_key=self.dead_letter_queue.routing())

        for wait_queue in self.retry_queues:
            wait = await broker.declare_queue(wait_queue)
            await wait.bind(retry, routing_key=wait_queue.routing())

        work = await broker.declare_queue(self.work_queue)
        await work.bind(payments, routing_key=self.work_queue.routing())

    def _check_retry_attempt(self, attempt: int) -> None:
        if not 1 <= attempt < self._policy.max_attempts:
            msg = (
                f"no wait queue for attempt {attempt}: valid attempts are 1..{self._policy.max_attempts - 1}"
            )
            raise ValueError(msg)

    def _queue(
        self,
        name: str,
        *,
        routing_key: str,
        dead_letter_exchange: str | None = None,
        dead_letter_routing_key: str | None = None,
        message_ttl_ms: int | None = None,
    ) -> RabbitQueue:
        """Create a durable queue of the configured type with the given dead-letter and TTL arguments."""
        arguments: dict[str, Any] = {}
        if dead_letter_exchange is not None:
            arguments["x-dead-letter-exchange"] = dead_letter_exchange
        if dead_letter_routing_key is not None:
            arguments["x-dead-letter-routing-key"] = dead_letter_routing_key
        if message_ttl_ms is not None:
            arguments["x-message-ttl"] = message_ttl_ms

        # FastStream types the arguments per queue type, hence one branch per type.
        if self._queue_type is QueueType.QUORUM:
            return RabbitQueue(
                name,
                queue_type=QueueType.QUORUM,
                routing_key=routing_key,
                arguments=cast("QuorumQueueArgs", arguments),
            )
        return RabbitQueue(
            name,
            queue_type=QueueType.CLASSIC,
            routing_key=routing_key,
            arguments=cast("ClassicQueueArgs", arguments),
        )

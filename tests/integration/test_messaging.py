"""Messaging against a real RabbitMQ: retry via wait queues, dead-lettering, publishing guarantees.

Mocks cannot prove broker behaviour (TTL expiry, DLX routing, publisher returns), so these tests run the
production wiring (``create_broker`` + ``Topology`` + ``create_consumer_app``) over a throw-away broker.
The queue names are fixed by the architecture, therefore every test starts by deleting the whole topology.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import aio_pika
import pytest
import pytest_asyncio
from aio_pika.exceptions import PublishError
from faststream.rabbit import RabbitExchange

from payment_service.common.time import utcnow
from payment_service.domain.events import PaymentCreatedEvent
from payment_service.messaging.consumer import PaymentConsumer, create_consumer_app
from payment_service.messaging.publisher import ConfirmedPublisher, RabbitEventPublisher, create_broker
from payment_service.messaging.retry import RetryPolicy, RetryRouter
from payment_service.messaging.topology import Topology

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractIncomingMessage
    from faststream.rabbit import RabbitBroker

    from payment_service.config import Settings
    from payment_service.services.processor import PaymentProcessor

# testcontainers' RabbitMQ container (used by the session ``rabbit_container`` fixture) still relies on a
# decorator that it deprecated itself; with ``filterwarnings = error`` its import would fail the fixture.
pytestmark = pytest.mark.filterwarnings(
    "ignore:The @wait_container_is_ready decorator is deprecated:DeprecationWarning"
)

WAIT_TIMEOUT = 15.0
TIMING_TOLERANCE = 0.05
"""Timestamps are taken in the handler, the broker measures the TTL from the republish: allow clock jitter."""

UNBOUND_EXCHANGE = "payments.test.unbound"
KNOWN_RETRY_QUEUES = 9


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
async def wait_until(
    condition: Callable[[], Awaitable[bool] | bool], *, what: str, within_seconds: float = WAIT_TIMEOUT
) -> None:
    """Poll ``condition`` every 50 ms until it holds; fail with ``what`` after ``within_seconds``."""
    deadline = time.monotonic() + within_seconds
    while True:
        result = condition()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        if time.monotonic() >= deadline:
            pytest.fail(f"timed out after {within_seconds}s waiting for {what}")
        await asyncio.sleep(0.05)


class StubFailureError(Exception):
    """The error the stub processor fails with."""


class StubProcessor:
    """Stands in for ``PaymentProcessor``: records every call, fails the first ``fail_times`` calls.

    ``fail_times=None`` fails forever.
    """

    def __init__(self, *, fail_times: int | None = 0) -> None:
        self.fail_times = fail_times
        self.calls: list[tuple[uuid.UUID, float]] = []

    async def process(self, payment_id: uuid.UUID) -> None:
        self.calls.append((payment_id, time.monotonic()))
        if self.fail_times is None or len(self.calls) <= self.fail_times:
            msg = f"stub failure #{len(self.calls)}"
            raise StubFailureError(msg)

    @property
    def timestamps(self) -> list[float]:
        return [timestamp for _, timestamp in self.calls]


@dataclass(frozen=True)
class Inspector:
    """Looks into the broker through its own connection, independent of the code under test.

    Every call uses a fresh channel: aio-pika caches queue objects per channel, so a repeated passive
    declare on one channel would keep returning the first (stale) message count.
    """

    connection: AbstractConnection

    @asynccontextmanager
    async def _channel(self) -> AsyncIterator[AbstractChannel]:
        channel = await self.connection.channel()
        try:
            yield channel
        finally:
            await channel.close()

    async def ready(self, queue_name: str) -> int:
        """Number of messages waiting in the queue (unacknowledged deliveries are not counted)."""
        async with self._channel() as channel:
            queue = await channel.declare_queue(queue_name, passive=True)
            return queue.declaration_result.message_count or 0

    async def take(self, queue_name: str) -> AbstractIncomingMessage:
        """Remove and return the first message of the queue."""
        async with self._channel() as channel:
            queue = await channel.declare_queue(queue_name, passive=True)
            message = await queue.get(no_ack=True, fail=False)
        assert message is not None, f"{queue_name} is empty"
        return message

    async def publish_raw(self, body: bytes, *, routing_key: str = "payments.new") -> None:
        """Publish bytes to the ``payments`` exchange exactly as a (possibly misbehaving) producer would."""
        async with self._channel() as channel:
            exchange = await channel.get_exchange("payments")
            message = aio_pika.Message(body, content_type="application/json", message_id=str(uuid.uuid4()))
            await exchange.publish(message, routing_key=routing_key, mandatory=True)


def ready_count_is(inspector: Inspector, queue_name: str, expected: int) -> Callable[[], Awaitable[bool]]:
    """A ``wait_until`` condition: the queue holds exactly ``expected`` waiting messages."""

    async def check() -> bool:
        return await inspector.ready(queue_name) == expected

    return check


async def delete_topology(channel: AbstractChannel) -> None:
    """Remove every queue and exchange of the pipeline (deletion of a missing entity is not an error)."""
    names = [
        "payments.new",
        "payments.new.dlq",
        *(f"payments.new.retry.{n}" for n in range(1, KNOWN_RETRY_QUEUES + 1)),
    ]
    for name in names:
        await channel.queue_delete(name)
    for name in ("payments", "payments.retry", "payments.dlx", UNBOUND_EXCHANGE):
        await channel.exchange_delete(name)


@pytest_asyncio.fixture
async def inspector(rabbit_url: str) -> AsyncIterator[Inspector]:
    """An :class:`Inspector` on a broker whose topology has just been wiped."""
    connection = await aio_pika.connect_robust(rabbit_url)
    async with connection:
        channel = await connection.channel()
        await delete_topology(channel)
        yield Inspector(connection)
        await delete_topology(channel)


@dataclass(frozen=True)
class Running:
    """A started consumer process (minus signal handling) and the objects it is made of."""

    broker: RabbitBroker
    topology: Topology
    processor: StubProcessor


class FlakySender:
    """Wraps the real :class:`ConfirmedPublisher`; the first ``fail_first`` sends raise."""

    def __init__(self, inner: ConfirmedPublisher, *, fail_first: int) -> None:
        self.inner = inner
        self.fail_first = fail_first
        self.calls = 0

    async def send(self, **kwargs: Any) -> None:
        self.calls += 1
        if self.calls <= self.fail_first:
            msg = "broker hiccup"
            raise ConnectionError(msg)
        await self.inner.send(**kwargs)


@asynccontextmanager
async def running_consumer(
    settings: Settings,
    processor: StubProcessor,
    *,
    wrap_sender: Callable[[ConfirmedPublisher], Any] = lambda sender: sender,
) -> AsyncIterator[Running]:
    """Assemble and start the consumer exactly like ``entrypoints.consumer`` does, stop it on exit."""
    broker = create_broker(settings.rabbit)
    topology = Topology(settings.rabbit, settings.retry)
    policy = RetryPolicy.from_settings(settings.retry)
    router = RetryRouter(wrap_sender(ConfirmedPublisher(broker)), topology, policy)
    consumer = PaymentConsumer(cast("PaymentProcessor", processor), router, policy)
    app = create_consumer_app(broker, topology, consumer)

    async with app.lifespan_context():
        await app.start()
        try:
            yield Running(broker, topology, processor)
        finally:
            # Waits for in-flight handlers, so every finished message has been acknowledged by now:
            # whatever is still unacknowledged would be put back into the queue.
            await app.stop()


@dataclass(frozen=True)
class RelaySide:
    """A second process' view of the broker: connected, topology declared, publishing only."""

    topology: Topology
    events: RabbitEventPublisher
    raw: ConfirmedPublisher


@asynccontextmanager
async def relay_side(settings: Settings) -> AsyncIterator[RelaySide]:
    broker = create_broker(settings.rabbit)
    topology = Topology(settings.rabbit, settings.retry)
    async with broker:
        await topology.declare(broker)
        yield RelaySide(
            topology, RabbitEventPublisher(broker, topology.payments_exchange), ConfirmedPublisher(broker)
        )


def make_event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(event_id=uuid.uuid4(), payment_id=uuid.uuid4(), occurred_at=utcnow())


async def publish_event(
    settings: Settings, event: PaymentCreatedEvent, *, correlation_id: str = "corr-1"
) -> bytes:
    """Publish ``event`` as the relay would and return the exact body that went on the wire."""
    body = event.model_dump_json().encode()
    async with relay_side(settings) as relay:
        await relay.raw.send(
            exchange=relay.topology.payments_exchange,
            routing_key=relay.topology.work_routing_key,
            body=body,
            message_id=str(event.event_id),
            correlation_id=correlation_id,
            message_type=relay.topology.work_routing_key,
        )
    return body


# --------------------------------------------------------------------------- #
# (1) happy path
# --------------------------------------------------------------------------- #
async def test_event_published_by_the_relay_publisher_is_processed_by_the_consumer(
    make_settings: Callable[..., Settings], rabbit_url: str, inspector: Inspector
) -> None:
    settings = make_settings(rabbit_url=rabbit_url)
    processor = StubProcessor()
    event = make_event()

    async with running_consumer(settings, processor):
        async with relay_side(settings) as relay:
            await relay.events.publish(
                routing_key=PaymentCreatedEvent.EVENT_TYPE,
                payload=event.to_payload(),
                message_id=str(event.event_id),
                correlation_id="corr-happy",
            )
        await wait_until(lambda: len(processor.calls) == 1, what="the payment to be processed")

    assert [payment_id for payment_id, _ in processor.calls] == [event.payment_id]
    for queue in ("payments.new", "payments.new.dlq", "payments.new.retry.1", "payments.new.retry.2"):
        assert await inspector.ready(queue) == 0, queue


async def test_published_message_is_persistent_and_carries_the_agreed_properties(
    make_settings: Callable[..., Settings], rabbit_url: str, inspector: Inspector
) -> None:
    settings = make_settings(rabbit_url=rabbit_url)
    event = make_event()

    async with relay_side(settings) as relay:
        await relay.events.publish(
            routing_key=PaymentCreatedEvent.EVENT_TYPE,
            payload=event.to_payload(),
            message_id=str(event.event_id),
            correlation_id="corr-props",
            headers={"origin": "relay"},
        )

    message = await inspector.take("payments.new")
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert message.content_type == "application/json"
    assert message.message_id == str(event.event_id)
    assert message.correlation_id == "corr-props"
    assert message.type == "payments.new"
    assert message.headers["origin"] == "relay"
    assert json.loads(message.body) == event.to_payload()


# --------------------------------------------------------------------------- #
# (2) unroutable messages are an error, not a silent drop
# --------------------------------------------------------------------------- #
async def test_publish_to_an_exchange_without_a_matching_queue_raises(
    make_settings: Callable[..., Settings], rabbit_url: str, inspector: Inspector
) -> None:
    settings = make_settings(rabbit_url=rabbit_url)
    broker = create_broker(settings.rabbit)
    exchange = RabbitExchange(UNBOUND_EXCHANGE)

    async with broker:
        await broker.declare_exchange(exchange)
        publisher = RabbitEventPublisher(broker, exchange)

        with pytest.raises(PublishError):
            await publisher.publish(routing_key="nobody.listens", payload={"a": 1}, message_id="m-1")


# --------------------------------------------------------------------------- #
# (3) retries with growing delays, then the DLQ
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("queue_type", ["quorum", "classic"])
async def test_always_failing_processor_is_attempted_max_attempts_times_and_ends_in_the_dlq(
    make_settings: Callable[..., Settings], rabbit_url: str, inspector: Inspector, queue_type: str
) -> None:
    settings = make_settings(
        rabbit_url=rabbit_url,
        rabbit={"queue_type": queue_type},
        retry={"max_attempts": 3, "base_delay_seconds": 0.2, "backoff_factor": 2.0},
    )
    processor = StubProcessor(fail_times=None)
    event = make_event()

    async with running_consumer(settings, processor):
        body = await publish_event(settings, event, correlation_id="corr-dlq")
        await wait_until(
            ready_count_is(inspector, "payments.new.dlq", 1), what="the message to reach the DLQ"
        )

    # The original has been acknowledged: after a clean stop nothing is left to be redelivered.
    assert len(processor.calls) == 3
    for queue in ("payments.new", "payments.new.retry.1", "payments.new.retry.2"):
        assert await inspector.ready(queue) == 0, queue

    first_gap, second_gap = (
        b - a for a, b in zip(processor.timestamps, processor.timestamps[1:], strict=False)
    )
    assert first_gap >= 0.2 - TIMING_TOLERANCE
    assert second_gap >= 0.4 - TIMING_TOLERANCE
    assert {payment_id for payment_id, _ in processor.calls} == {event.payment_id}

    dead = await inspector.take("payments.new.dlq")
    assert dead.body == body
    assert dead.message_id == str(event.event_id)
    assert dead.correlation_id == "corr-dlq"
    assert dead.type == "payments.new"
    assert dead.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert dead.headers["x-attempt"] == 3
    assert dead.headers["x-error-type"] == "StubFailureError"
    assert dead.headers["x-error-message"] == "stub failure #3"
    failed_at = datetime.fromisoformat(str(dead.headers["x-failed-at"]))
    assert failed_at.utcoffset() == timedelta(0)
    assert utcnow() - failed_at < timedelta(seconds=WAIT_TIMEOUT)
    # The message travelled through two wait queues, the broker stamped it each time; none of that is copied.
    assert not [key for key in dead.headers if key.startswith(("x-death", "x-first-death", "x-last-death"))]
    assert await inspector.ready("payments.new.dlq") == 0


async def test_single_attempt_policy_dead_letters_the_first_failure_without_waiting(
    make_settings: Callable[..., Settings], rabbit_url: str, inspector: Inspector
) -> None:
    settings = make_settings(rabbit_url=rabbit_url, retry={"max_attempts": 1})
    processor = StubProcessor(fail_times=None)

    async with running_consumer(settings, processor):
        await publish_event(settings, make_event())
        await wait_until(
            ready_count_is(inspector, "payments.new.dlq", 1), what="the message to reach the DLQ"
        )

    assert len(processor.calls) == 1
    assert (await inspector.take("payments.new.dlq")).headers["x-attempt"] == 1
    assert await inspector.ready("payments.new") == 0


# --------------------------------------------------------------------------- #
# (4) recovery on a later attempt
# --------------------------------------------------------------------------- #
async def test_processor_failing_once_succeeds_on_the_second_attempt_and_leaves_the_dlq_empty(
    make_settings: Callable[..., Settings], rabbit_url: str, inspector: Inspector
) -> None:
    settings = make_settings(
        rabbit_url=rabbit_url, retry={"max_attempts": 3, "base_delay_seconds": 0.2, "backoff_factor": 2.0}
    )
    processor = StubProcessor(fail_times=1)
    event = make_event()

    async with running_consumer(settings, processor):
        await publish_event(settings, event)
        await wait_until(lambda: len(processor.calls) == 2, what="the second attempt")

    assert len(processor.calls) == 2
    assert processor.timestamps[1] - processor.timestamps[0] >= 0.2 - TIMING_TOLERANCE
    for queue in ("payments.new", "payments.new.dlq", "payments.new.retry.1", "payments.new.retry.2"):
        assert await inspector.ready(queue) == 0, queue


async def test_message_whose_republish_fails_is_redelivered_instead_of_lost(
    make_settings: Callable[..., Settings], rabbit_url: str, inspector: Inspector
) -> None:
    settings = make_settings(
        rabbit_url=rabbit_url, retry={"max_attempts": 3, "base_delay_seconds": 0.2, "backoff_factor": 2.0}
    )
    processor = StubProcessor(fail_times=2)
    senders: list[FlakySender] = []

    def wrap(sender: ConfirmedPublisher) -> FlakySender:
        senders.append(FlakySender(sender, fail_first=1))
        return senders[0]

    async with running_consumer(settings, processor, wrap_sender=wrap):
        await publish_event(settings, make_event())
        await wait_until(lambda: len(processor.calls) == 3, what="the third attempt")

    # Attempt 1 fails and so does its republish: nack with requeue, the broker hands the message out again
    # at once (still attempt 1, no wait queue). Attempt 2 is republished normally and returns after the TTL.
    first_gap, second_gap = (
        b - a for a, b in zip(processor.timestamps, processor.timestamps[1:], strict=False)
    )
    assert first_gap < 0.2
    assert second_gap >= 0.2 - TIMING_TOLERANCE
    assert senders[0].calls == 2
    for queue in ("payments.new", "payments.new.dlq", "payments.new.retry.1", "payments.new.retry.2"):
        assert await inspector.ready(queue) == 0, queue


# --------------------------------------------------------------------------- #
# (5) poison messages
# --------------------------------------------------------------------------- #
POISON_BODIES = [
    pytest.param(b"{this is not json", id="invalid-json"),
    pytest.param(b"", id="empty-body"),
    pytest.param(b'{"version": 1}', id="missing-fields"),
    pytest.param(
        b'{"event_id": "not-a-uuid", "payment_id": "also-not", "occurred_at": "yesterday"}', id="wrong-types"
    ),
    pytest.param(b'["a", "list"]', id="not-an-object"),
]


@pytest.mark.parametrize("body", POISON_BODIES)
async def test_unparsable_message_is_dead_lettered_by_the_broker_without_calling_the_processor(
    make_settings: Callable[..., Settings], rabbit_url: str, inspector: Inspector, body: bytes
) -> None:
    settings = make_settings(rabbit_url=rabbit_url)
    processor = StubProcessor()

    async with running_consumer(settings, processor):
        await inspector.publish_raw(body)
        await wait_until(
            ready_count_is(inspector, "payments.new.dlq", 1), what="the poison message in the DLQ"
        )

    assert processor.calls == []
    dead = await inspector.take("payments.new.dlq")
    assert dead.body == body
    # Dead-lettered by the work queue's DLX (``rejected``), not republished by our retry code: no x-error-*.
    [death] = dead.headers["x-death"]
    assert death["queue"] == "payments.new"
    assert death["reason"] == "rejected"
    assert "x-error-type" not in dead.headers
    for queue in ("payments.new", "payments.new.retry.1", "payments.new.retry.2"):
        assert await inspector.ready(queue) == 0, queue


async def test_poison_message_does_not_block_valid_messages_behind_it(
    make_settings: Callable[..., Settings], rabbit_url: str, inspector: Inspector
) -> None:
    settings = make_settings(rabbit_url=rabbit_url)
    processor = StubProcessor()
    event = make_event()

    async with running_consumer(settings, processor):
        await inspector.publish_raw(b"{garbage")
        await publish_event(settings, event)
        await wait_until(lambda: len(processor.calls) == 1, what="the valid message to be processed")
        await wait_until(
            ready_count_is(inspector, "payments.new.dlq", 1), what="the poison message in the DLQ"
        )

    assert [payment_id for payment_id, _ in processor.calls] == [event.payment_id]


# --------------------------------------------------------------------------- #
# (6) idempotent declaration
# --------------------------------------------------------------------------- #
async def test_declare_is_idempotent_and_a_second_process_can_repeat_it(
    make_settings: Callable[..., Settings], rabbit_url: str, inspector: Inspector
) -> None:
    settings = make_settings(rabbit_url=rabbit_url)
    topology = Topology(settings.rabbit, settings.retry)

    first = create_broker(settings.rabbit)
    async with first:
        await topology.declare(first)
        await topology.declare(first)  # same process: served from the declarer's cache

    # A new broker has no cache, so this really sends the declarations again and the server compares
    # the arguments with the existing queues.
    second = create_broker(settings.rabbit)
    async with second:
        await Topology(settings.rabbit, settings.retry).declare(second)

    for queue in ("payments.new", "payments.new.dlq", "payments.new.retry.1", "payments.new.retry.2"):
        assert await inspector.ready(queue) == 0, queue


async def test_declare_binds_every_queue_so_messages_published_before_the_consumer_starts_are_kept(
    make_settings: Callable[..., Settings], rabbit_url: str, inspector: Inspector
) -> None:
    settings = make_settings(rabbit_url=rabbit_url)
    event = make_event()

    await publish_event(settings, event)  # declares the topology, nobody consumes

    assert await inspector.ready("payments.new") == 1
    processor = StubProcessor()
    async with running_consumer(settings, processor):
        await wait_until(lambda: len(processor.calls) == 1, what="the backlog message to be processed")


async def test_consumer_startup_declares_the_topology_before_consuming(
    make_settings: Callable[..., Settings], rabbit_url: str, inspector: Inspector
) -> None:
    """A failing message needs the wait queue at once: it must exist although no relay ever started."""
    settings = make_settings(
        rabbit_url=rabbit_url, retry={"max_attempts": 2, "base_delay_seconds": 0.2, "backoff_factor": 2.0}
    )
    processor = StubProcessor(fail_times=1)

    async with running_consumer(settings, processor):
        assert await inspector.ready("payments.new.retry.1") == 0
        assert await inspector.ready("payments.new.dlq") == 0
        await inspector.publish_raw(make_event().model_dump_json().encode())
        await wait_until(lambda: len(processor.calls) == 2, what="the retry to come back")

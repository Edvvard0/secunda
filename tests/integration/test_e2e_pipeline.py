"""End-to-end tests of the whole payment pipeline over real PostgreSQL, real RabbitMQ and a real receiver.

Everything runs in one process but is assembled the way the entrypoints assemble it:

    httpx (ASGI) -> create_app(PaymentService) -> PostgreSQL (payments + outbox)
        -> OutboxRelay + RabbitEventPublisher -> RabbitMQ topology
        -> create_consumer_app(PaymentConsumer, RetryRouter, PaymentProcessor(EmulatedPaymentGateway,
           HttpWebhookSender)) -> webhook receiver (the demo sink under a real uvicorn server on 127.0.0.1)

Nothing in the chain is mocked; the only test seams are an outage switch in front of the relay's publisher
(to simulate a lost broker) and an ASGI wrapper that records what the receiver was sent. The queue names are
fixed by the architecture, therefore every test starts by wiping the whole RabbitMQ topology (see
``test_messaging.py``) and truncates the tables (``clean_database``).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
import uuid
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from itertools import pairwise
from typing import TYPE_CHECKING, Any

import aio_pika
import httpx
import pytest
import pytest_asyncio
import uvicorn
from sqlalchemy import func, select

from payment_service.api.app import create_app
from payment_service.domain.enums import PaymentStatus
from payment_service.logging_config import set_correlation_id
from payment_service.messaging.consumer import PaymentConsumer, create_consumer_app
from payment_service.messaging.publisher import ConfirmedPublisher, RabbitEventPublisher, create_broker
from payment_service.messaging.retry import RetryPolicy, RetryRouter
from payment_service.messaging.topology import Topology
from payment_service.persistence import OutboxEvent, Payment
from payment_service.services.gateway import EmulatedPaymentGateway
from payment_service.services.interfaces import EventPublisher
from payment_service.services.outbox import OutboxRelay
from payment_service.services.payments import PaymentService
from payment_service.services.processor import PaymentProcessor
from payment_service.services.webhook import HttpWebhookSender, verify_signature
from payment_service.tools import webhook_sink

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping

    from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractIncomingMessage
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

    from payment_service.config import Settings
    from payment_service.persistence import Database

# testcontainers' RabbitMQ container (used by the session ``rabbit_container`` fixture) still relies on a
# decorator that it deprecated itself; with ``filterwarnings = error`` its import would fail the fixture.
pytestmark = pytest.mark.filterwarnings(
    "ignore:The @wait_container_is_ready decorator is deprecated:DeprecationWarning"
)

WAIT_TIMEOUT = 20.0
TIMING_TOLERANCE = 0.05
"""Arrival times are taken in the receiver, the broker measures the TTL from the republish: allow jitter."""

PAYMENTS_URL = "/api/v1/payments"
PROCESSOR_LOGGER = "payment_service.services.processor"

WORK_QUEUE = "payments.new"
DEAD_LETTER_QUEUE = "payments.new.dlq"
RETRY_QUEUES = ("payments.new.retry.1", "payments.new.retry.2")
KNOWN_RETRY_QUEUES = 9

PAYMENT_FIELDS: dict[str, Any] = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
}


# --------------------------------------------------------------------------- #
# Waiting and looking into the broker (patterns of test_messaging.py)
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


async def wait_for[T](
    fetch: Callable[[], Awaitable[T]],
    predicate: Callable[[T], bool],
    *,
    what: str,
    within_seconds: float = WAIT_TIMEOUT,
) -> T:
    """Poll ``fetch`` until ``predicate`` accepts its result and return that result."""
    accepted: list[T] = []

    async def satisfied() -> bool:
        value = await fetch()
        if predicate(value):
            accepted.append(value)
        return bool(accepted)

    await wait_until(satisfied, what=what, within_seconds=within_seconds)
    return accepted[0]


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


def ready_count_is(inspector: Inspector, queue_name: str, expected: int) -> Callable[[], Awaitable[bool]]:
    """A ``wait_until`` condition: the queue holds exactly ``expected`` waiting messages."""

    async def check() -> bool:
        return await inspector.ready(queue_name) == expected

    return check


async def assert_queues_empty(inspector: Inspector, *queue_names: str) -> None:
    """Fail unless every given queue has no waiting message."""
    for queue_name in queue_names:
        assert await inspector.ready(queue_name) == 0, queue_name


async def delete_topology(channel: AbstractChannel) -> None:
    """Remove every queue and exchange of the pipeline (deletion of a missing entity is not an error)."""
    names = [
        WORK_QUEUE,
        DEAD_LETTER_QUEUE,
        *(f"payments.new.retry.{n}" for n in range(1, KNOWN_RETRY_QUEUES + 1)),
    ]
    for name in names:
        await channel.queue_delete(name)
    for name in ("payments", "payments.retry", "payments.dlx"):
        await channel.exchange_delete(name)


# --------------------------------------------------------------------------- #
# Looking into PostgreSQL
# --------------------------------------------------------------------------- #
async def fetch_payment(database: Database, payment_id: uuid.UUID) -> Payment:
    """Load the stored payment through a fresh session."""
    async with database.session_factory() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None, f"payment {payment_id} is not stored"
    return payment


async def fetch_outbox(database: Database) -> list[OutboxEvent]:
    """Load every outbox row, oldest first."""
    async with database.session_factory() as session:
        return list((await session.scalars(select(OutboxEvent).order_by(OutboxEvent.created_at))).all())


async def count_rows(database: Database, model: type[Payment] | type[OutboxEvent]) -> int:
    """Number of rows of ``model``'s table."""
    async with database.session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model)) or 0


# --------------------------------------------------------------------------- #
# The webhook receiver: the demo sink behind a recording wrapper, served by a real uvicorn
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Delivery:
    """One webhook request as the receiver saw it, and the status it answered with."""

    path: str
    headers: Mapping[str, str]
    body: bytes
    status_code: int
    received_at: float
    """``time.monotonic()`` when the request body had arrived."""

    @property
    def payment_id(self) -> str:
        """The ``X-Payment-Id`` header (empty when absent)."""
        return self.headers.get("x-payment-id", "")

    def payload(self) -> dict[str, Any]:
        """The JSON body."""
        payload: dict[str, Any] = json.loads(self.body)
        return payload

    def is_signed_with(self, secret: str) -> bool:
        """Verify ``X-Signature`` over ``X-Timestamp`` and the raw body, as a real receiver would."""
        return verify_signature(
            secret, int(self.headers["x-timestamp"]), self.body, self.headers["x-signature"]
        )


class RecordingApp:
    """ASGI wrapper that keeps every POST (headers, raw body, answer) of the application it serves.

    A delivery is recorded *before* the response is released to the client: once the sender has seen
    an answer (and, say, marked the webhook delivered), the delivery is certain to be on the list.
    """

    def __init__(self, app: ASGIApp) -> None:
        self._app = app
        self.deliveries: list[Delivery] = []

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] != "POST":
            await self._app(scope, receive, send)
            return
        body = await _read_body(receive)
        headers = {key.decode("latin-1").lower(): value.decode("latin-1") for key, value in scope["headers"]}
        received_at = time.monotonic()
        replayed = False

        async def replay() -> Message:
            nonlocal replayed
            if replayed:
                return await receive()  # whatever the client does next, e.g. disconnecting
            replayed = True
            return {"type": "http.request", "body": body, "more_body": False}

        async def record_then_send(message: Message) -> None:
            if message["type"] == "http.response.start":
                self.deliveries.append(Delivery(scope["path"], headers, body, message["status"], received_at))
            await send(message)

        await self._app(scope, replay, record_then_send)


async def _read_body(receive: Receive) -> bytes:
    chunks: list[bytes] = []
    while True:
        message = await receive()
        if message["type"] != "http.request":
            break
        chunks.append(message.get("body", b""))
        if not message.get("more_body", False):
            break
    return b"".join(chunks)


@dataclass(frozen=True)
class Receiver:
    """A running webhook receiver."""

    port: int
    recorder: RecordingApp

    def url(self, path: str) -> str:
        """Absolute URL of one of the sink's routes (``/ok``, ``/fail``, ``/flaky``)."""
        return f"http://127.0.0.1:{self.port}{path}"

    @property
    def deliveries(self) -> list[Delivery]:
        """Every webhook received so far, in arrival order."""
        return self.recorder.deliveries

    def deliveries_of(self, payment_id: uuid.UUID) -> list[Delivery]:
        """The webhooks received for one payment, in arrival order."""
        return [delivery for delivery in self.deliveries if delivery.payment_id == str(payment_id)]


@asynccontextmanager
async def running_receiver(signing_secret: str) -> AsyncIterator[Receiver]:
    """Serve the demo sink with a programmatic uvicorn on a free loopback port; stop it cleanly on exit."""
    recorder = RecordingApp(webhook_sink.create_app(signing_secret=signing_secret))
    config = uvicorn.Config(
        recorder,
        host="127.0.0.1",
        port=0,
        interface="asgi3",
        lifespan="off",
        ws="none",
        log_config=None,
        log_level="warning",
        access_log=False,
        timeout_graceful_shutdown=5,
    )
    server = uvicorn.Server(config)
    serving = asyncio.create_task(server.serve())
    try:
        await wait_until(lambda: server.started or serving.done(), what="the webhook receiver to start")
        if not server.started:
            await serving  # re-raises the reason it could not start
        port: int = server.servers[0].sockets[0].getsockname()[1]
        yield Receiver(port, recorder)
    finally:
        server.should_exit = True
        async with asyncio.timeout(WAIT_TIMEOUT):
            await serving


# --------------------------------------------------------------------------- #
# The workers, assembled like entrypoints/consumer.py and entrypoints/relay.py
# --------------------------------------------------------------------------- #
class OutagePublisher(EventPublisher):
    """Stands in front of the real publisher; while ``down`` every publish fails like a lost connection."""

    def __init__(self, inner: EventPublisher) -> None:
        self.inner = inner
        self.down = False

    async def publish(
        self,
        *,
        routing_key: str,
        payload: Mapping[str, Any],
        message_id: str,
        correlation_id: str | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        if self.down:
            msg = "broker unreachable (simulated outage)"
            raise ConnectionError(msg)
        await self.inner.publish(
            routing_key=routing_key,
            payload=payload,
            message_id=message_id,
            correlation_id=correlation_id,
            headers=headers,
        )


@dataclass(frozen=True)
class Relay:
    """Handles of the running relay: the outage switch and the real publisher behind it."""

    outage: OutagePublisher
    publisher: RabbitEventPublisher


async def _stop_worker(stop_event: asyncio.Event, task: asyncio.Task[None]) -> None:
    stop_event.set()
    async with asyncio.timeout(WAIT_TIMEOUT):
        await task


@asynccontextmanager
async def running_relay(settings: Settings, database: Database) -> AsyncIterator[Relay]:
    """Assemble and run the outbox relay exactly like ``entrypoints.relay`` does; stop it on exit."""
    stop_event = asyncio.Event()
    async with AsyncExitStack() as resources:
        broker = create_broker(settings.rabbit)
        resources.push_async_callback(broker.stop)

        topology = Topology(settings.rabbit, settings.retry)
        await broker.connect()
        await topology.declare(broker)

        publisher = RabbitEventPublisher(broker, topology.payments_exchange)
        outage = OutagePublisher(publisher)
        relay = OutboxRelay(database.unit_of_work, outage, settings.outbox)
        task = asyncio.create_task(relay.run_forever(stop_event))
        # Registered last, so it runs first: the relay finishes its iteration before the broker goes away.
        resources.push_async_callback(_stop_worker, stop_event, task)
        yield Relay(outage, publisher)


@asynccontextmanager
async def running_consumer(settings: Settings, database: Database) -> AsyncIterator[None]:
    """Assemble and start the consumer exactly like ``entrypoints.consumer`` does; stop it on exit."""
    async with AsyncExitStack() as resources:
        webhook_sender = HttpWebhookSender(settings.webhook)
        resources.push_async_callback(webhook_sender.aclose)

        processor = PaymentProcessor(
            database.unit_of_work, EmulatedPaymentGateway(settings.gateway), webhook_sender
        )
        broker = create_broker(settings.rabbit)
        topology = Topology(settings.rabbit, settings.retry)
        policy = RetryPolicy.from_settings(settings.retry)
        router = RetryRouter(ConfirmedPublisher(broker), topology, policy)
        app = create_consumer_app(broker, topology, PaymentConsumer(processor, router, policy))

        await resources.enter_async_context(app.lifespan_context())
        await app.start()
        # Waits for in-flight handlers, so every finished message has been acknowledged by then.
        resources.push_async_callback(app.stop)
        yield


# --------------------------------------------------------------------------- #
# The system under test
# --------------------------------------------------------------------------- #
@dataclass
class Pipeline:
    """The running system and the handles the tests need to drive and observe it."""

    settings: Settings
    database: Database
    inspector: Inspector
    receiver: Receiver
    client: httpx.AsyncClient
    relay: Relay
    workers: AsyncExitStack

    @property
    def webhook_secret(self) -> str:
        """The secret the service signs webhooks with."""
        return self.settings.webhook.signing_secret.get_secret_value()

    async def create_payment(
        self, *, idempotency_key: str | None = None, path: str = "/ok", **fields: Any
    ) -> httpx.Response:
        """``POST /api/v1/payments`` with a valid body; ``fields`` override body fields."""
        body = {**PAYMENT_FIELDS, "webhook_url": self.receiver.url(path), **fields}
        headers = {"Idempotency-Key": idempotency_key or uuid.uuid4().hex}
        return await self.client.post(PAYMENTS_URL, json=body, headers=headers)

    async def get_payment(self, payment_id: uuid.UUID) -> dict[str, Any]:
        """``GET /api/v1/payments/{id}`` expecting 200."""
        response = await self.client.get(f"{PAYMENTS_URL}/{payment_id}")
        assert response.status_code == 200, response.text
        payment: dict[str, Any] = response.json()
        return payment

    async def wait_for_payment(
        self, payment_id: uuid.UUID, predicate: Callable[[Payment], bool], *, what: str
    ) -> Payment:
        """Poll the stored payment until ``predicate`` holds and return it."""
        return await wait_for(lambda: fetch_payment(self.database, payment_id), predicate, what=what)

    async def wait_until_webhook_delivered(self, payment_id: uuid.UUID) -> Payment:
        """Wait until the receiver's acknowledgement is stored (``webhook_delivered_at``)."""
        return await self.wait_for_payment(
            payment_id,
            lambda payment: payment.webhook_delivered_at is not None,
            what="the webhook to be delivered",
        )

    async def republish(self, event: OutboxEvent) -> None:
        """Put an outbox event on the work queue once more, exactly as the relay published it."""
        await self.relay.publisher.publish(
            routing_key=event.event_type,
            payload=event.payload,
            message_id=str(event.id),
            correlation_id=event.correlation_id,
        )

    async def stop_workers(self) -> None:
        """Stop the relay, then the consumer (waiting for in-flight messages); safe to call twice.

        Afterwards nothing moves any more, so counts of deliveries and queue contents are final.
        """
        await self.workers.aclose()


type StartPipeline = Callable[..., Awaitable[Pipeline]]


@asynccontextmanager
async def running_pipeline(
    settings: Settings, database: Database, inspector: Inspector
) -> AsyncIterator[Pipeline]:
    """Start receiver, consumer, relay and API; stop everything (workers first, receiver last) on exit."""
    async with AsyncExitStack() as stack:
        receiver = await stack.enter_async_context(
            running_receiver(settings.webhook.signing_secret.get_secret_value())
        )
        workers = AsyncExitStack()
        stack.push_async_callback(workers.aclose)
        await workers.enter_async_context(running_consumer(settings, database))
        relay = await workers.enter_async_context(running_relay(settings, database))

        # ASGI test clients do not run the lifespan; the injected collaborators are used as they are.
        app = create_app(settings, payment_service=PaymentService(database.unit_of_work), database=database)
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            headers={"X-API-Key": settings.api.key.get_secret_value()},
        )
        await stack.enter_async_context(client)
        yield Pipeline(settings, database, inspector, receiver, client, relay, workers)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture(autouse=True)
def _restore_logging_state() -> Iterator[None]:
    """``create_app`` and uvicorn configure logging process-wide; undo it so other tests stay independent."""
    root = logging.getLogger()
    handlers_before, level_before = list(root.handlers), root.level
    names = ("uvicorn", "uvicorn.error", "uvicorn.access", "uvicorn.asgi", "faststream", "aio_pika", "aiormq")
    saved = [
        (logger, list(logger.handlers), logger.propagate, logger.level)
        for logger in map(logging.getLogger, names)
    ]
    quiet = [(logger, logger.level) for logger in map(logging.getLogger, ("httpx", "httpcore"))]
    yield
    for handler in list(root.handlers):
        if handler not in handlers_before:
            root.removeHandler(handler)
    root.setLevel(level_before)
    for logger, handlers, propagate, level in saved:
        logger.handlers[:] = handlers
        logger.propagate = propagate
        logger.setLevel(level)
    for logger, level in quiet:
        logger.setLevel(level)
    set_correlation_id(None)


@pytest_asyncio.fixture
async def inspector(rabbit_url: str) -> AsyncIterator[Inspector]:
    """An :class:`Inspector` on a broker whose topology has just been wiped (and is wiped afterwards too)."""
    connection = await aio_pika.connect_robust(rabbit_url)
    async with connection:
        channel = await connection.channel()
        await delete_topology(channel)
        yield Inspector(connection)
        await delete_topology(channel)


@pytest_asyncio.fixture
async def start_pipeline(
    clean_database: Database,
    database_url: str,
    rabbit_url: str,
    make_settings: Callable[..., Settings],
    inspector: Inspector,
) -> AsyncIterator[StartPipeline]:
    """Factory that starts the whole pipeline with settings overrides, e.g. ``gateway={...}``."""
    async with AsyncExitStack() as stack:

        async def start(**overrides: dict[str, Any]) -> Pipeline:
            settings = make_settings(db_url=database_url, rabbit_url=rabbit_url, **overrides)
            return await stack.enter_async_context(running_pipeline(settings, clean_database, inspector))

        yield start


# --------------------------------------------------------------------------- #
# Small assertions shared by the tests
# --------------------------------------------------------------------------- #
def accepted_payment_id(response: httpx.Response) -> uuid.UUID:
    """The ``payment_id`` of a ``202 Accepted`` answer."""
    assert response.status_code == 202, response.text
    return uuid.UUID(response.json()["payment_id"])


def processor_runs(caplog: pytest.LogCaptureFixture, payment_id: uuid.UUID) -> int:
    """How many processing runs have finished for the payment (the processor logs one INFO line per run)."""
    return sum(
        1
        for record in caplog.records
        if record.name == PROCESSOR_LOGGER
        and record.levelno == logging.INFO
        and getattr(record, "payment_id", None) == str(payment_id)
    )


def instant(timestamp: str) -> datetime:
    """Parse an ISO 8601 timestamp (``Z`` or ``+00:00`` alike) into an aware datetime."""
    return datetime.fromisoformat(timestamp)


# --------------------------------------------------------------------------- #
# (1) happy path
# --------------------------------------------------------------------------- #
async def test_accepted_payment_is_charged_and_one_signed_webhook_reports_the_stored_status(
    start_pipeline: StartPipeline,
) -> None:
    pipeline = await start_pipeline()

    response = await pipeline.create_payment()
    payment_id = accepted_payment_id(response)
    assert response.json()["status"] == "pending"
    await pipeline.wait_until_webhook_delivered(payment_id)
    await pipeline.stop_workers()

    stored = await pipeline.get_payment(payment_id)
    assert stored["status"] == "succeeded"
    assert stored["processed_at"] is not None

    [delivery] = pipeline.receiver.deliveries_of(payment_id)
    assert delivery.path == "/ok"
    assert delivery.status_code == 200
    assert delivery.is_signed_with(pipeline.webhook_secret)
    payload = delivery.payload()
    assert payload["event"] == "payment.processed"
    assert payload["payment_id"] == str(payment_id)
    assert payload["status"] == stored["status"]
    assert payload["amount"] == "100.50"
    assert payload["currency"] == "RUB"
    assert payload["metadata"] == {"order_id": 42}
    assert instant(payload["processed_at"]) == instant(stored["processed_at"])

    [event] = await fetch_outbox(pipeline.database)
    assert event.aggregate_id == payment_id
    assert event.published_at is not None
    assert event.attempts == 1
    assert event.last_error is None
    await assert_queues_empty(pipeline.inspector, WORK_QUEUE, DEAD_LETTER_QUEUE, *RETRY_QUEUES)


# --------------------------------------------------------------------------- #
# (2) a declined payment is a result, not a failure of the pipeline
# --------------------------------------------------------------------------- #
async def test_declined_payment_is_stored_as_failed_and_still_notified_without_dead_lettering(
    start_pipeline: StartPipeline,
) -> None:
    pipeline = await start_pipeline(gateway={"success_rate": 0.0})

    payment_id = accepted_payment_id(await pipeline.create_payment())
    await pipeline.wait_until_webhook_delivered(payment_id)
    await pipeline.stop_workers()

    stored = await pipeline.get_payment(payment_id)
    assert stored["status"] == "failed"
    assert stored["processed_at"] is not None
    [delivery] = pipeline.receiver.deliveries_of(payment_id)
    assert delivery.status_code == 200
    assert delivery.is_signed_with(pipeline.webhook_secret)
    assert delivery.payload()["status"] == "failed"
    await assert_queues_empty(pipeline.inspector, WORK_QUEUE, DEAD_LETTER_QUEUE, *RETRY_QUEUES)


# --------------------------------------------------------------------------- #
# (3) idempotency from the HTTP request to the webhook
# --------------------------------------------------------------------------- #
async def test_replayed_request_with_the_same_idempotency_key_yields_one_payment_one_event_one_webhook(
    start_pipeline: StartPipeline,
) -> None:
    pipeline = await start_pipeline()
    key = "e2e-replay-key"

    first = await pipeline.create_payment(idempotency_key=key)
    second = await pipeline.create_payment(idempotency_key=key)
    payment_id = accepted_payment_id(first)
    assert accepted_payment_id(second) == payment_id
    assert "idempotent-replayed" not in first.headers
    assert second.headers["idempotent-replayed"] == "true"

    await pipeline.wait_until_webhook_delivered(payment_id)
    third = await pipeline.create_payment(idempotency_key=key)  # a replay after the payment was processed
    assert accepted_payment_id(third) == payment_id
    await pipeline.stop_workers()

    assert await count_rows(pipeline.database, Payment) == 1
    assert await count_rows(pipeline.database, OutboxEvent) == 1
    assert len(pipeline.receiver.deliveries) == 1
    assert len(pipeline.receiver.deliveries_of(payment_id)) == 1


async def test_idempotency_key_reused_with_another_body_is_rejected_and_the_first_payment_is_unaffected(
    start_pipeline: StartPipeline,
) -> None:
    pipeline = await start_pipeline()
    key = "e2e-reused-key"

    first = await pipeline.create_payment(idempotency_key=key)
    payment_id = accepted_payment_id(first)
    conflicting = await pipeline.create_payment(idempotency_key=key, amount="999.99")

    assert conflicting.status_code == 422
    assert conflicting.headers["content-type"] == "application/problem+json"
    assert conflicting.json()["code"] == "idempotency-key-reused"

    await pipeline.wait_until_webhook_delivered(payment_id)
    await pipeline.stop_workers()
    assert (await pipeline.get_payment(payment_id))["amount"] == "100.50"
    assert await count_rows(pipeline.database, Payment) == 1
    assert await count_rows(pipeline.database, OutboxEvent) == 1
    assert len(pipeline.receiver.deliveries) == 1


# --------------------------------------------------------------------------- #
# (4) duplicate delivery of one event
# --------------------------------------------------------------------------- #
async def test_duplicate_events_racing_the_original_finalize_the_payment_once(
    start_pipeline: StartPipeline, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=PROCESSOR_LOGGER)
    pipeline = await start_pipeline()
    payment_id = accepted_payment_id(await pipeline.create_payment())
    [event] = await fetch_outbox(pipeline.database)

    # Straight away, while the original is still on its way: three messages carry the same event.
    await pipeline.republish(event)
    await pipeline.republish(event)
    first_final = await pipeline.wait_for_payment(
        payment_id, lambda payment: payment.status is not PaymentStatus.PENDING, what="the first finalization"
    )
    await wait_until(
        lambda: processor_runs(caplog, payment_id) == 3, what="the original and both duplicates to be handled"
    )
    await pipeline.stop_workers()

    final = await fetch_payment(pipeline.database, payment_id)
    assert final.status is first_final.status
    assert final.processed_at == first_final.processed_at
    assert final.webhook_delivered_at is not None
    deliveries = pipeline.receiver.deliveries_of(payment_id)
    assert 1 <= len(deliveries) <= 3  # the webhook is at-least-once: racing duplicates may each send it
    assert {delivery.body for delivery in deliveries} == {deliveries[0].body}
    assert all(delivery.status_code == 200 for delivery in deliveries)
    assert all(delivery.is_signed_with(pipeline.webhook_secret) for delivery in deliveries)
    assert deliveries[0].payload()["status"] == final.status.value
    await assert_queues_empty(pipeline.inspector, WORK_QUEUE, DEAD_LETTER_QUEUE, *RETRY_QUEUES)


async def test_duplicate_event_after_completion_changes_nothing_and_sends_no_second_webhook(
    start_pipeline: StartPipeline, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=PROCESSOR_LOGGER)
    pipeline = await start_pipeline()
    payment_id = accepted_payment_id(await pipeline.create_payment())
    [event] = await fetch_outbox(pipeline.database)
    completed = await pipeline.wait_until_webhook_delivered(payment_id)
    await wait_until(lambda: processor_runs(caplog, payment_id) == 1, what="the original to be handled")

    await pipeline.republish(event)
    await pipeline.republish(event)
    await wait_until(lambda: processor_runs(caplog, payment_id) == 3, what="both duplicates to be handled")
    await pipeline.stop_workers()

    final = await fetch_payment(pipeline.database, payment_id)
    assert final.status is completed.status
    assert final.processed_at == completed.processed_at
    assert final.webhook_delivered_at == completed.webhook_delivered_at
    assert len(pipeline.receiver.deliveries_of(payment_id)) == 1
    await assert_queues_empty(pipeline.inspector, WORK_QUEUE, DEAD_LETTER_QUEUE, *RETRY_QUEUES)


# --------------------------------------------------------------------------- #
# (5) the receiver recovers: retry through the wait queues, then success
# --------------------------------------------------------------------------- #
async def test_receiver_failing_twice_gets_three_attempts_and_the_payment_ends_delivered(
    start_pipeline: StartPipeline,
) -> None:
    pipeline = await start_pipeline()

    payment_id = accepted_payment_id(await pipeline.create_payment(path="/flaky"))
    await pipeline.wait_until_webhook_delivered(payment_id)
    await pipeline.stop_workers()

    attempts = pipeline.receiver.deliveries_of(payment_id)
    assert [attempt.status_code for attempt in attempts] == [500, 500, 200]
    # The retries only repeat the webhook: the charge was stored once and every attempt reports it.
    assert {attempt.body for attempt in attempts} == {attempts[0].body}
    assert all(attempt.is_signed_with(pipeline.webhook_secret) for attempt in attempts)
    stored = await fetch_payment(pipeline.database, payment_id)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.webhook_delivered_at is not None
    await assert_queues_empty(pipeline.inspector, WORK_QUEUE, DEAD_LETTER_QUEUE, *RETRY_QUEUES)


# --------------------------------------------------------------------------- #
# (6) the receiver never recovers: backoff, then the dead-letter queue
# --------------------------------------------------------------------------- #
async def test_permanently_failing_receiver_exhausts_the_attempts_and_the_event_lands_in_the_dlq(
    start_pipeline: StartPipeline,
) -> None:
    pipeline = await start_pipeline()

    payment_id = accepted_payment_id(await pipeline.create_payment(path="/fail"))
    await wait_until(
        ready_count_is(pipeline.inspector, DEAD_LETTER_QUEUE, 1), what="the event to reach the DLQ"
    )
    await pipeline.stop_workers()

    attempts = pipeline.receiver.deliveries_of(payment_id)
    assert [attempt.status_code for attempt in attempts] == [500, 500, 500]
    # The wait queues hold the message for base * factor ** (n - 1): 0.2 s, then 0.4 s.
    first_gap, second_gap = (later.received_at - earlier.received_at for earlier, later in pairwise(attempts))
    assert first_gap >= 0.2 - TIMING_TOLERANCE
    assert second_gap >= 0.4 - TIMING_TOLERANCE

    assert await pipeline.inspector.ready(DEAD_LETTER_QUEUE) == 1
    dead = await pipeline.inspector.take(DEAD_LETTER_QUEUE)
    assert dead.headers["x-attempt"] == 3
    assert dead.headers["x-error-type"] == "WebhookDeliveryError"
    assert "HTTP 500" in str(dead.headers["x-error-message"])
    assert json.loads(dead.body)["payment_id"] == str(payment_id)

    stored = await fetch_payment(pipeline.database, payment_id)
    assert stored.status is PaymentStatus.SUCCEEDED  # the charge itself is final and stays
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is None
    await assert_queues_empty(pipeline.inspector, WORK_QUEUE, DEAD_LETTER_QUEUE, *RETRY_QUEUES)


# --------------------------------------------------------------------------- #
# (7) the broker is gone while payments are accepted
# --------------------------------------------------------------------------- #
async def test_broker_outage_keeps_the_event_in_the_outbox_and_the_payment_completes_after_recovery(
    start_pipeline: StartPipeline,
) -> None:
    pipeline = await start_pipeline()
    pipeline.relay.outage.down = True

    response = await pipeline.create_payment()
    payment_id = accepted_payment_id(response)  # accepting a payment does not need the broker
    stuck = await wait_for(
        lambda: fetch_outbox(pipeline.database),
        lambda rows: len(rows) == 1 and rows[0].attempts >= 2,
        what="the relay to try and fail repeatedly",
    )
    [event] = stuck
    assert event.published_at is None
    assert event.last_error is not None
    assert event.last_error.startswith("ConnectionError")
    pending = await pipeline.get_payment(payment_id)
    assert pending["status"] == "pending"
    assert pending["processed_at"] is None
    assert pipeline.receiver.deliveries == []

    pipeline.relay.outage.down = False
    await pipeline.wait_until_webhook_delivered(payment_id)
    await pipeline.stop_workers()

    [published] = await fetch_outbox(pipeline.database)
    assert published.published_at is not None
    assert published.attempts > event.attempts
    assert published.last_error is None
    assert (await pipeline.get_payment(payment_id))["status"] == "succeeded"
    [delivery] = pipeline.receiver.deliveries_of(payment_id)
    assert delivery.is_signed_with(pipeline.webhook_secret)
    await assert_queues_empty(pipeline.inspector, WORK_QUEUE, DEAD_LETTER_QUEUE, *RETRY_QUEUES)


# --------------------------------------------------------------------------- #
# (8) SSRF: the guard is part of the real request path
# --------------------------------------------------------------------------- #
async def test_loopback_webhook_target_is_rejected_by_the_api_when_private_networks_are_forbidden(
    start_pipeline: StartPipeline,
) -> None:
    pipeline = await start_pipeline(webhook={"allow_private_networks": False})

    response = await pipeline.create_payment()

    assert response.status_code == 422
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["code"] == "invalid-webhook-url"
    assert await count_rows(pipeline.database, Payment) == 0
    assert await count_rows(pipeline.database, OutboxEvent) == 0
    await assert_queues_empty(pipeline.inspector, WORK_QUEUE, DEAD_LETTER_QUEUE)
    assert pipeline.receiver.deliveries == []

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from faststream.exceptions import NackMessage

from payment_service.common.time import utcnow
from payment_service.domain.events import PaymentCreatedEvent
from payment_service.logging_config import get_correlation_id
from payment_service.messaging.consumer import PaymentConsumer
from payment_service.messaging.retry import (
    MAX_ERROR_MESSAGE_LENGTH,
    X_ATTEMPT,
    X_ERROR_MESSAGE,
    X_ERROR_TYPE,
    X_FAILED_AT,
    RetryPolicy,
    RetryRouter,
    parse_attempt,
)
from payment_service.messaging.topology import Topology

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from payment_service.config import Settings


# --------------------------------------------------------------------------- #
# RetryPolicy / parse_attempt
# --------------------------------------------------------------------------- #
def test_delay_seconds_follows_defaults_5_then_10() -> None:
    policy = RetryPolicy(max_attempts=3, base_delay_seconds=5.0, backoff_factor=2.0)

    assert [policy.delay_seconds(attempt) for attempt in (1, 2)] == [5.0, 10.0]


def test_delay_seconds_grows_by_custom_factor() -> None:
    policy = RetryPolicy(max_attempts=5, base_delay_seconds=0.5, backoff_factor=3.0)

    assert [policy.delay_seconds(attempt) for attempt in (1, 2, 3, 4)] == [0.5, 1.5, 4.5, 13.5]


def test_delay_seconds_is_constant_when_factor_is_one() -> None:
    policy = RetryPolicy(max_attempts=4, base_delay_seconds=2.0, backoff_factor=1.0)

    assert {policy.delay_seconds(attempt) for attempt in (1, 2, 3)} == {2.0}


@pytest.mark.parametrize("attempt", [0, -1])
def test_delay_seconds_rejects_attempts_below_one(attempt: int) -> None:
    policy = RetryPolicy(max_attempts=3, base_delay_seconds=5.0, backoff_factor=2.0)

    with pytest.raises(ValueError, match="attempt must be >= 1"):
        policy.delay_seconds(attempt)


@pytest.mark.parametrize(
    ("max_attempts", "attempt", "expected"),
    [
        (3, 1, True),
        (3, 2, True),
        (3, 3, False),
        (3, 4, False),
        (1, 1, False),
        (2, 1, True),
        (2, 2, False),
    ],
)
def test_has_attempts_left_is_true_only_below_the_maximum(
    max_attempts: int, attempt: int, expected: bool
) -> None:
    policy = RetryPolicy(max_attempts=max_attempts, base_delay_seconds=1.0, backoff_factor=2.0)

    assert policy.has_attempts_left(attempt) is expected


def test_from_settings_copies_every_retry_setting(make_settings: Callable[..., Settings]) -> None:
    settings = make_settings(retry={"max_attempts": 4, "base_delay_seconds": 7.0, "backoff_factor": 1.5})

    assert RetryPolicy.from_settings(settings.retry) == RetryPolicy(
        max_attempts=4, base_delay_seconds=7.0, backoff_factor=1.5
    )


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        (None, 1),
        ({}, 1),
        ({"x-attempt": None}, 1),
        ({"x-attempt": 2}, 2),
        ({"x-attempt": "3"}, 3),
        ({"x-attempt": b"3"}, 3),
        ({"x-attempt": 0}, 1),
        ({"x-attempt": -1}, 1),
        ({"x-attempt": "-7"}, 1),
        ({"x-attempt": "x"}, 1),
        ({"x-attempt": ""}, 1),
        ({"x-attempt": [3]}, 1),
        ({"x-attempt": float("nan")}, 1),
        ({"x-attempt": float("inf")}, 1),
        ({"x-attempt": 10**40}, 2**31 - 1),
    ],
)
def test_parse_attempt_falls_back_to_one_for_missing_or_invalid_values(
    headers: Mapping[str, Any] | None, expected: int
) -> None:
    assert parse_attempt(headers) == expected


# --------------------------------------------------------------------------- #
# RetryRouter
# --------------------------------------------------------------------------- #
class RecordingPublisher:
    """Stands in for ``ConfirmedPublisher``: records ``send`` calls, optionally failing."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.sent: list[dict[str, Any]] = []

    async def send(self, **kwargs: Any) -> None:
        if self.error is not None:
            raise self.error
        self.sent.append(kwargs)


class FakeProcessor:
    """Stands in for ``PaymentProcessor``; fails with ``error`` when given."""

    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.processed: list[uuid.UUID] = []
        self.correlation_ids_seen: list[str | None] = []

    async def process(self, payment_id: uuid.UUID) -> None:
        self.processed.append(payment_id)
        self.correlation_ids_seen.append(get_correlation_id())
        if self.error is not None:
            raise self.error


class RecordingRouter:
    """Stands in for ``RetryRouter``: records ``handle_failure`` calls."""

    def __init__(self, *, outcome: str = "retry", error: Exception | None = None) -> None:
        self.outcome = outcome
        self.error = error
        self.calls: list[dict[str, Any]] = []

    async def handle_failure(self, **kwargs: Any) -> str:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.outcome


def make_router(
    settings: Settings, publisher: RecordingPublisher, *, max_attempts: int | None = None
) -> tuple[RetryRouter, Topology]:
    topology = Topology(settings.rabbit, settings.retry)
    policy = RetryPolicy.from_settings(settings.retry)
    if max_attempts is not None:
        policy = RetryPolicy(max_attempts, policy.base_delay_seconds, policy.backoff_factor)
    return RetryRouter(cast("Any", publisher), topology, policy), topology


async def test_failure_with_attempts_left_goes_to_the_wait_queue_of_that_attempt(
    make_settings: Callable[..., Settings],
) -> None:
    publisher = RecordingPublisher()
    router, topology = make_router(make_settings(retry={"max_attempts": 3}), publisher)

    outcome = await router.handle_failure(
        body=b'{"k":1}',
        headers={"x-attempt": 2},
        message_id="m-1",
        correlation_id="c-1",
        attempt=2,
        error=RuntimeError("boom"),
    )

    assert outcome == "retry"
    [sent] = publisher.sent
    assert sent["exchange"] is topology.retry_exchange
    assert sent["routing_key"] == "payments.new.retry.2"
    assert sent["body"] == b'{"k":1}'
    assert sent["message_id"] == "m-1"
    assert sent["correlation_id"] == "c-1"
    assert sent["message_type"] == "payments.new"
    assert sent["headers"] == {X_ATTEMPT: 3}


async def test_first_failure_uses_the_first_wait_queue_and_counts_the_second_attempt(
    make_settings: Callable[..., Settings],
) -> None:
    publisher = RecordingPublisher()
    router, _ = make_router(make_settings(), publisher)

    await router.handle_failure(
        body=b"{}", headers={}, message_id=None, correlation_id=None, attempt=1, error=RuntimeError("x")
    )

    [sent] = publisher.sent
    assert sent["routing_key"] == "payments.new.retry.1"
    assert sent["headers"] == {X_ATTEMPT: 2}


async def test_last_failure_goes_to_the_dead_letter_queue_with_diagnostics(
    make_settings: Callable[..., Settings],
) -> None:
    publisher = RecordingPublisher()
    router, topology = make_router(make_settings(retry={"max_attempts": 3}), publisher)
    before = utcnow()

    outcome = await router.handle_failure(
        body=b'{"k":1}',
        headers={"x-attempt": 3},
        message_id="m-1",
        correlation_id="c-1",
        attempt=3,
        error=ValueError("bad thing"),
    )

    assert outcome == "dead-letter"
    [sent] = publisher.sent
    assert sent["exchange"] is topology.dead_letter_exchange
    assert sent["routing_key"] == "payments.new.dlq"
    assert sent["body"] == b'{"k":1}'
    assert sent["message_id"] == "m-1"
    assert sent["correlation_id"] == "c-1"
    headers = sent["headers"]
    assert headers[X_ATTEMPT] == 3
    assert headers[X_ERROR_TYPE] == "ValueError"
    assert headers[X_ERROR_MESSAGE] == "bad thing"
    failed_at = datetime.fromisoformat(headers[X_FAILED_AT])
    assert failed_at.utcoffset() == timedelta(0)
    assert before <= failed_at <= utcnow()


async def test_single_attempt_policy_dead_letters_the_first_failure(
    make_settings: Callable[..., Settings],
) -> None:
    publisher = RecordingPublisher()
    router, _ = make_router(make_settings(retry={"max_attempts": 1}), publisher)

    outcome = await router.handle_failure(
        body=b"{}", headers={}, message_id=None, correlation_id=None, attempt=1, error=RuntimeError("x")
    )

    assert outcome == "dead-letter"
    assert publisher.sent[0]["routing_key"] == "payments.new.dlq"
    assert publisher.sent[0]["headers"][X_ATTEMPT] == 1


async def test_dead_letter_error_message_is_truncated(make_settings: Callable[..., Settings]) -> None:
    publisher = RecordingPublisher()
    router, _ = make_router(make_settings(retry={"max_attempts": 1}), publisher)

    await router.handle_failure(
        body=b"{}",
        headers={},
        message_id=None,
        correlation_id=None,
        attempt=1,
        error=RuntimeError("e" * 5000),
    )

    assert publisher.sent[0]["headers"][X_ERROR_MESSAGE] == "e" * MAX_ERROR_MESSAGE_LENGTH


@pytest.mark.parametrize("max_attempts", [1, 3])
async def test_republish_drops_broker_managed_headers_and_keeps_application_headers(
    make_settings: Callable[..., Settings], max_attempts: int
) -> None:
    publisher = RecordingPublisher()
    router, _ = make_router(make_settings(retry={"max_attempts": max_attempts}), publisher)
    incoming = {
        "x-death": [{"count": 4, "queue": "payments.new.retry.1", "reason": "expired"}],
        "x-first-death-queue": "payments.new.retry.1",
        "x-first-death-reason": "expired",
        "x-last-death-exchange": "payments.retry",
        "X-Error-Type": "stale",
        "x-attempt": 1,
        "tenant": "acme",
    }

    await router.handle_failure(
        body=b"{}", headers=incoming, message_id=None, correlation_id=None, attempt=1, error=RuntimeError("x")
    )

    sent_headers = publisher.sent[0]["headers"]
    assert sent_headers["tenant"] == "acme"
    assert "X-Error-Type" not in sent_headers
    assert set(sent_headers) - {"tenant"} <= {X_ATTEMPT, X_ERROR_TYPE, X_ERROR_MESSAGE, X_FAILED_AT}
    assert sent_headers[X_ATTEMPT] == (2 if max_attempts == 3 else 1)


async def test_republish_failure_propagates_so_the_original_stays_unacknowledged(
    make_settings: Callable[..., Settings],
) -> None:
    publisher = RecordingPublisher(error=ConnectionError("broker gone"))
    router, _ = make_router(make_settings(), publisher)

    with pytest.raises(ConnectionError, match="broker gone"):
        await router.handle_failure(
            body=b"{}", headers={}, message_id=None, correlation_id=None, attempt=1, error=RuntimeError("x")
        )


# --------------------------------------------------------------------------- #
# PaymentConsumer.handle
# --------------------------------------------------------------------------- #
def make_event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(event_id=uuid.uuid4(), payment_id=uuid.uuid4(), occurred_at=utcnow())


def make_message(
    *, headers: dict[str, Any] | None = None, body: bytes = b'{"event":1}', correlation_id: str = "corr-1"
) -> Any:
    """The part of ``RabbitMessage`` the handler reads."""
    return SimpleNamespace(
        body=body, headers=headers or {}, message_id="msg-1", correlation_id=correlation_id
    )


POLICY = RetryPolicy(max_attempts=3, base_delay_seconds=1.0, backoff_factor=2.0)


def make_consumer(processor: FakeProcessor, router: RecordingRouter) -> PaymentConsumer:
    return PaymentConsumer(cast("Any", processor), cast("Any", router), POLICY)


async def test_handle_processes_the_payment_and_does_not_reroute_on_success() -> None:
    processor, router = FakeProcessor(), RecordingRouter()
    event = make_event()

    await make_consumer(processor, router).handle(event, make_message())

    assert processor.processed == [event.payment_id]
    assert router.calls == []


async def test_handle_binds_the_correlation_id_while_processing_and_clears_it_afterwards() -> None:
    processor = FakeProcessor()

    consumer = make_consumer(processor, RecordingRouter())

    await consumer.handle(make_event(), make_message(correlation_id="corr-42"))

    assert processor.correlation_ids_seen == ["corr-42"]
    assert get_correlation_id() is None


async def test_handle_returns_normally_and_reroutes_when_processing_fails() -> None:
    error = RuntimeError("webhook down")
    processor, router = FakeProcessor(error), RecordingRouter(outcome="retry")
    message = make_message(headers={"x-attempt": 2}, body=b'{"payload":"original"}')

    await make_consumer(processor, router).handle(make_event(), message)

    [call] = router.calls
    assert call["attempt"] == 2
    assert call["body"] == b'{"payload":"original"}'
    assert call["headers"] == {"x-attempt": 2}
    assert call["message_id"] == "msg-1"
    assert call["correlation_id"] == "corr-1"
    assert call["error"] is error


async def test_handle_treats_a_message_without_attempt_header_as_the_first_attempt() -> None:
    router = RecordingRouter()

    await make_consumer(FakeProcessor(RuntimeError("x")), router).handle(make_event(), make_message())

    assert router.calls[0]["attempt"] == 1


async def test_handle_nacks_with_requeue_when_the_failure_cannot_be_rerouted() -> None:
    router = RecordingRouter(error=ConnectionError("broker gone"))
    consumer = make_consumer(FakeProcessor(RuntimeError("x")), router)

    with pytest.raises(NackMessage) as raised:
        await consumer.handle(make_event(), make_message())

    assert raised.value.extra_options == {}  # no kwargs: aio-pika's default is requeue=True
    assert isinstance(raised.value.__cause__, ConnectionError)
    assert get_correlation_id() is None


async def test_handle_does_not_swallow_cancellation() -> None:
    router = RecordingRouter()
    consumer = make_consumer(FakeProcessor(asyncio.CancelledError()), router)

    with pytest.raises(asyncio.CancelledError):
        await consumer.handle(make_event(), make_message())

    assert router.calls == []
    assert get_correlation_id() is None

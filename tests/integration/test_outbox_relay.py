from __future__ import annotations

import asyncio
import logging
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from payment_service.common.time import utcnow
from payment_service.persistence import OutboxEvent
from payment_service.services.interfaces import EventPublisher
from payment_service.services.outbox import OutboxRelay

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from payment_service.config import OutboxSettings, Settings
    from payment_service.persistence import Database

WAIT_TIMEOUT = 10.0


@dataclass(frozen=True)
class PublishedMessage:
    routing_key: str
    payload: Mapping[str, Any]
    message_id: str
    correlation_id: str | None


class FakePublisher(EventPublisher):
    """Records successful publishes; raises ``ConnectionError`` from the Nth call on (1-based)."""

    def __init__(self, *, fail_from_call: int | None = None, delay_seconds: float = 0.0) -> None:
        self.fail_from_call = fail_from_call
        self.delay_seconds = delay_seconds
        self.calls = 0
        self.published: list[PublishedMessage] = []

    async def publish(
        self,
        *,
        routing_key: str,
        payload: Mapping[str, Any],
        message_id: str,
        correlation_id: str | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        self.calls += 1
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        if self.fail_from_call is not None and self.calls >= self.fail_from_call:
            msg = "broker down"
            raise ConnectionError(msg)
        self.published.append(PublishedMessage(routing_key, payload, message_id, correlation_id))

    @property
    def published_ids(self) -> list[str]:
        return [message.message_id for message in self.published]


class FakeClock:
    """Manually advanced replacement for ``time.monotonic``."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def wait_until(condition: Callable[[], bool]) -> None:
    """Poll ``condition`` until it holds; fail the test after ``WAIT_TIMEOUT`` seconds."""
    async with asyncio.timeout(WAIT_TIMEOUT):
        while not condition():  # noqa: ASYNC110 - the condition is a plain callable, there is nothing to await
            await asyncio.sleep(0.01)


def make_event(
    index: int = 0,
    *,
    created_at_offset: timedelta | None = None,
    published_ago: timedelta | None = None,
) -> OutboxEvent:
    now = utcnow()
    return OutboxEvent(
        id=uuid.uuid4(),
        event_type="payments.new",
        aggregate_id=uuid.uuid4(),
        payload={"version": 1, "index": index},
        correlation_id=f"req-{index}",
        created_at=now
        + (created_at_offset if created_at_offset is not None else timedelta(milliseconds=index)),
        published_at=now - published_ago if published_ago is not None else None,
    )


async def add_events(database: Database, events: Sequence[OutboxEvent]) -> None:
    async with database.unit_of_work() as uow:
        for event in events:
            uow.outbox.add(event)
        await uow.commit()


async def load_rows(database: Database) -> dict[uuid.UUID, Any]:
    async with database.engine.connect() as connection:
        result = await connection.execute(select(OutboxEvent))
        return {row.id: row for row in result}


@pytest.fixture
def outbox_settings(make_settings: Callable[..., Settings]) -> Callable[..., OutboxSettings]:
    def factory(**overrides: Any) -> OutboxSettings:
        return make_settings(outbox=overrides).outbox

    return factory


def make_relay(
    database: Database,
    publisher: EventPublisher,
    settings: OutboxSettings,
    **kwargs: Any,
) -> OutboxRelay:
    return OutboxRelay(database.unit_of_work, publisher, settings, **kwargs)


async def test_run_once_publishes_everything_and_marks_events_published(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    events = [make_event(index) for index in range(3)]
    await add_events(clean_database, events)
    publisher = FakePublisher()
    relay = make_relay(clean_database, publisher, outbox_settings())

    assert await relay.run_once() is True

    assert publisher.published == [
        PublishedMessage("payments.new", {"version": 1, "index": i}, str(event.id), f"req-{i}")
        for i, event in enumerate(events)
    ]
    rows = await load_rows(clean_database)
    assert all(row.published_at is not None for row in rows.values())
    assert all(row.attempts == 1 for row in rows.values())
    assert all(row.last_error is None for row in rows.values())


async def test_run_once_returns_false_and_does_not_publish_when_the_outbox_is_empty(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    publisher = FakePublisher()
    relay = make_relay(clean_database, publisher, outbox_settings())

    assert await relay.run_once() is False

    assert publisher.calls == 0


async def test_run_once_does_not_publish_an_event_twice(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    await add_events(clean_database, [make_event(0), make_event(1)])
    publisher = FakePublisher()
    relay = make_relay(clean_database, publisher, outbox_settings())

    assert await relay.run_once() is True
    assert await relay.run_once() is False

    assert publisher.calls == 2


async def test_run_once_publishes_in_creation_order(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    events = [make_event(index) for index in range(5)]
    await add_events(clean_database, [events[3], events[0], events[4], events[2], events[1]])
    publisher = FakePublisher()
    relay = make_relay(clean_database, publisher, outbox_settings())

    await relay.run_once()

    assert publisher.published_ids == [str(event.id) for event in events]


async def test_run_once_respects_the_batch_size(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    events = [make_event(index) for index in range(5)]
    await add_events(clean_database, events)
    publisher = FakePublisher()
    relay = make_relay(clean_database, publisher, outbox_settings(batch_size=2))

    assert await relay.run_once() is True
    assert publisher.published_ids == [str(e.id) for e in events[:2]]
    assert await relay.run_once() is True
    assert await relay.run_once() is True
    assert await relay.run_once() is False
    assert publisher.published_ids == [str(e.id) for e in events]


async def test_failing_publish_commits_earlier_events_records_the_failure_and_raises(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    events = [make_event(index) for index in range(4)]
    await add_events(clean_database, events)
    publisher = FakePublisher(fail_from_call=3)
    relay = make_relay(clean_database, publisher, outbox_settings())

    with pytest.raises(ConnectionError, match="broker down"):
        await relay.run_once()

    rows = await load_rows(clean_database)
    first, second, failing, untouched = (rows[event.id] for event in events)
    assert first.published_at is not None
    assert first.attempts == 1
    assert second.published_at is not None
    assert second.attempts == 1
    assert failing.published_at is None
    assert failing.attempts == 1
    assert failing.last_error == "ConnectionError: broker down"
    assert untouched.published_at is None
    assert untouched.attempts == 0
    assert untouched.last_error is None


async def test_remaining_events_are_published_after_the_publisher_recovers(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    events = [make_event(index) for index in range(4)]
    await add_events(clean_database, events)
    publisher = FakePublisher(fail_from_call=3)
    relay = make_relay(clean_database, publisher, outbox_settings())
    with pytest.raises(ConnectionError):
        await relay.run_once()

    publisher.fail_from_call = None
    assert await relay.run_once() is True
    assert await relay.run_once() is False

    assert publisher.published_ids == [str(event.id) for event in events]
    rows = await load_rows(clean_database)
    assert all(row.published_at is not None for row in rows.values())
    retried = rows[events[2].id]
    assert retried.attempts == 2
    assert retried.last_error is None


async def test_failure_of_the_first_event_keeps_the_whole_batch_unpublished(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    events = [make_event(index) for index in range(2)]
    await add_events(clean_database, events)
    relay = make_relay(clean_database, FakePublisher(fail_from_call=1), outbox_settings())

    with pytest.raises(ConnectionError):
        await relay.run_once()

    rows = await load_rows(clean_database)
    assert all(row.published_at is None for row in rows.values())
    assert rows[events[0].id].attempts == 1
    assert rows[events[1].id].attempts == 0


async def test_two_relays_running_concurrently_publish_each_event_exactly_once(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    events = [make_event(index) for index in range(20)]
    await add_events(clean_database, events)
    publisher = FakePublisher(delay_seconds=0.05)  # hold the row locks long enough for the relays to overlap
    settings = outbox_settings(batch_size=15)
    relays = [make_relay(clean_database, publisher, settings) for _ in range(2)]

    results = await asyncio.wait_for(asyncio.gather(*(relay.run_once() for relay in relays)), timeout=60)

    assert results == [True, True]
    assert Counter(publisher.published_ids) == Counter(str(event.id) for event in events)
    rows = await load_rows(clean_database)
    assert all(row.published_at is not None and row.attempts == 1 for row in rows.values())


async def test_purge_published_removes_only_old_published_rows(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    old_published = make_event(0, published_ago=timedelta(hours=5))
    recent_published = make_event(1, published_ago=timedelta(minutes=10))
    old_unpublished = make_event(2, created_at_offset=-timedelta(days=30))
    await add_events(clean_database, [old_published, recent_published, old_unpublished])
    relay = make_relay(clean_database, FakePublisher(), outbox_settings(retention_hours=1.0))

    deleted = await relay.purge_published()

    assert deleted == 1
    assert set(await load_rows(clean_database)) == {recent_published.id, old_unpublished.id}


async def test_purge_published_logs_the_number_of_removed_rows(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings], caplog: pytest.LogCaptureFixture
):
    await add_events(clean_database, [make_event(0, published_ago=timedelta(hours=5))])
    relay = make_relay(clean_database, FakePublisher(), outbox_settings(retention_hours=1.0))

    with caplog.at_level(logging.INFO, logger="payment_service.services.outbox"):
        await relay.purge_published()

    assert any(r.levelno == logging.INFO and "Purged 1 " in r.getMessage() for r in caplog.records)


async def test_run_once_purges_only_when_the_purge_interval_has_elapsed(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    clock = FakeClock()
    relay = make_relay(
        clean_database,
        FakePublisher(),
        outbox_settings(retention_hours=1.0, purge_interval_seconds=100.0),
        monotonic=clock,
    )
    stale = make_event(0, published_ago=timedelta(hours=5))
    await add_events(clean_database, [stale])

    clock.now += 99  # the first purge is due one interval after start, not at startup
    await relay.run_once()
    assert stale.id in await load_rows(clean_database)

    clock.now += 2
    await relay.run_once()
    assert stale.id not in await load_rows(clean_database)

    another_stale = make_event(1, published_ago=timedelta(hours=5))
    await add_events(clean_database, [another_stale])
    clock.now += 50  # next purge is a full interval after the previous one
    await relay.run_once()
    assert another_stale.id in await load_rows(clean_database)


async def test_run_once_purges_even_when_the_outbox_is_busy(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    clock = FakeClock()
    publisher = FakePublisher()
    relay = make_relay(
        clean_database,
        publisher,
        outbox_settings(retention_hours=1.0, purge_interval_seconds=100.0),
        monotonic=clock,
    )
    stale = make_event(0, published_ago=timedelta(hours=5))
    fresh = make_event(1)
    await add_events(clean_database, [stale, fresh])

    clock.now += 101
    assert await relay.run_once() is True

    assert publisher.published_ids == [str(fresh.id)]
    assert stale.id not in await load_rows(clean_database)


async def test_failing_purge_does_not_stop_publishing_and_is_retried_only_after_an_interval(
    clean_database: Database,
    outbox_settings: Callable[..., OutboxSettings],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    clock = FakeClock()
    publisher = FakePublisher()
    relay = make_relay(
        clean_database, publisher, outbox_settings(purge_interval_seconds=100.0), monotonic=clock
    )
    purge = AsyncMock(side_effect=RuntimeError("purge failed"))
    monkeypatch.setattr(relay, "purge_published", purge)
    await add_events(clean_database, [make_event(0), make_event(1)])

    clock.now += 101
    with caplog.at_level(logging.ERROR, logger="payment_service.services.outbox"):
        assert await relay.run_once() is True
    await relay.run_once()

    assert len(publisher.published) == 2
    assert purge.await_count == 1
    assert any("Purging" in r.getMessage() and r.exc_info for r in caplog.records)


async def test_run_forever_publishes_events_inserted_while_it_runs(
    clean_database: Database, outbox_settings: Callable[..., OutboxSettings]
):
    publisher = FakePublisher()
    relay = make_relay(clean_database, publisher, outbox_settings(poll_interval_seconds=0.02))
    stop_event = asyncio.Event()
    task = asyncio.create_task(relay.run_forever(stop_event))
    try:
        first_wave = [make_event(index) for index in range(2)]
        await add_events(clean_database, first_wave)
        await wait_until(lambda: len(publisher.published) == 2)

        second_wave = [make_event(index) for index in range(2, 5)]
        await add_events(clean_database, second_wave)
        await wait_until(lambda: len(publisher.published) == 5)
    finally:
        stop_event.set()
        await asyncio.wait_for(task, timeout=WAIT_TIMEOUT)

    assert publisher.published_ids == [str(e.id) for e in [*first_wave, *second_wave]]
    rows = await load_rows(clean_database)
    assert all(row.published_at is not None for row in rows.values())


async def test_run_forever_recovers_once_the_publisher_comes_back(
    clean_database: Database,
    outbox_settings: Callable[..., OutboxSettings],
    caplog: pytest.LogCaptureFixture,
):
    publisher = FakePublisher(fail_from_call=1)
    relay = make_relay(
        clean_database,
        publisher,
        outbox_settings(poll_interval_seconds=0.02, error_backoff_seconds=0.05),
    )
    event = make_event(0)
    await add_events(clean_database, [event])
    stop_event = asyncio.Event()

    with caplog.at_level(logging.ERROR, logger="payment_service.common.worker"):
        task = asyncio.create_task(relay.run_forever(stop_event))
        try:
            await wait_until(lambda: publisher.calls >= 2)  # retried after the backoff
            publisher.fail_from_call = None
            await wait_until(lambda: len(publisher.published) == 1)
        finally:
            stop_event.set()
            await asyncio.wait_for(task, timeout=WAIT_TIMEOUT)

    assert publisher.published_ids == [str(event.id)]
    stored = (await load_rows(clean_database))[event.id]
    assert stored.published_at is not None
    assert stored.attempts >= 2
    assert stored.last_error is None
    assert any(r.levelno == logging.ERROR and r.exc_info for r in caplog.records)

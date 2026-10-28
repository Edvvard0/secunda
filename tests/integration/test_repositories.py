from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from payment_service.common.time import utcnow
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.persistence import OutboxEvent, Payment

if TYPE_CHECKING:
    from collections.abc import Sequence

    from payment_service.persistence import Database

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
LOCK_TIMEOUT = 5.0  # SKIP LOCKED must never block; fail the test instead of hanging


async def insert_payment(database: Database, key: str = "key-1") -> Payment:
    async with database.unit_of_work() as uow:
        payment = await uow.payments.insert_if_absent(
            idempotency_key=key,
            request_fingerprint="f" * 64,
            amount=Decimal("100.50"),
            currency=Currency.RUB,
            description="Order #42",
            metadata={"order_id": 42},
            webhook_url="https://client.example.com/hook",
        )
        await uow.commit()
    assert payment is not None
    return payment


async def load_payment(database: Database, payment_id: uuid.UUID) -> Payment:
    async with database.unit_of_work() as uow:
        payment = await uow.payments.get(payment_id)
    assert payment is not None
    return payment


def make_event(
    *,
    created_at: datetime | None = None,
    published_at: datetime | None = None,
    event_id: uuid.UUID | None = None,
) -> OutboxEvent:
    return OutboxEvent(
        id=event_id or uuid.uuid4(),
        event_type="payments.new",
        aggregate_id=uuid.uuid4(),
        payload={"version": 1},
        correlation_id="req-1",
        created_at=created_at or utcnow(),
        published_at=published_at,
    )


async def add_events(database: Database, events: Sequence[OutboxEvent]) -> None:
    async with database.unit_of_work() as uow:
        for event in events:
            uow.outbox.add(event)
        await uow.commit()


async def load_event(database: Database, event_id: uuid.UUID) -> OutboxEvent:
    async with database.unit_of_work() as uow:
        event = await uow.outbox.get(event_id)
    assert event is not None
    return event


async def outbox_ids(database: Database) -> set[uuid.UUID]:
    async with database.engine.connect() as connection:
        result = await connection.execute(select(OutboxEvent.id))
        return set(result.scalars())


def raw_payment_params(**overrides: Any) -> dict[str, Any]:
    params: dict[str, Any] = {
        "id": uuid.uuid4(),
        "amount": Decimal("10.00"),
        "currency": "RUB",
        "status": "pending",
        "key": f"key-{uuid.uuid4()}",
    }
    params.update(overrides)
    return params


async def raw_insert_payment(database: Database, **overrides: Any) -> None:
    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO payments (id, amount, currency, status, description, idempotency_key,"
                " request_fingerprint, webhook_url)"
                " VALUES (:id, :amount, :currency, :status, 'd', :key, 'fp', 'https://example.com/h')"
            ),
            raw_payment_params(**overrides),
        )


class TestPaymentInsertIfAbsent:
    async def test_returns_the_row_the_first_time_and_none_the_second(self, clean_database: Database):
        first = await insert_payment(clean_database, key="same-key")

        async with clean_database.unit_of_work() as uow:
            second = await uow.payments.insert_if_absent(
                idempotency_key="same-key",
                request_fingerprint="a" * 64,
                amount=Decimal("1.00"),
                currency=Currency.USD,
                description="other",
                metadata={},
                webhook_url="https://other.example.com/hook",
            )
            await uow.commit()

        assert second is None
        stored = await load_payment(clean_database, first.id)
        assert stored.amount == Decimal("100.50")
        assert stored.request_fingerprint == "f" * 64

    async def test_stores_all_fields_and_database_defaults(self, clean_database: Database):
        payment = await insert_payment(clean_database)

        stored = await load_payment(clean_database, payment.id)
        assert stored.amount == Decimal("100.50")
        assert stored.currency is Currency.RUB
        assert stored.description == "Order #42"
        assert stored.metadata_ == {"order_id": 42}
        assert stored.status is PaymentStatus.PENDING
        assert stored.webhook_url == "https://client.example.com/hook"
        assert stored.created_at.tzinfo is not None
        assert stored.processed_at is None
        assert stored.webhook_delivered_at is None

    async def test_ten_concurrent_inserts_with_one_key_create_exactly_one_row(self, clean_database: Database):
        async def attempt() -> Payment | None:
            async with clean_database.unit_of_work() as uow:
                payment = await uow.payments.insert_if_absent(
                    idempotency_key="race-key",
                    request_fingerprint="f" * 64,
                    amount=Decimal("5.00"),
                    currency=Currency.EUR,
                    description="race",
                    metadata={},
                    webhook_url="https://client.example.com/hook",
                )
                await uow.commit()
                return payment

        results = await asyncio.wait_for(asyncio.gather(*(attempt() for _ in range(10))), timeout=30)

        winners = [result for result in results if result is not None]
        assert len(winners) == 1
        async with clean_database.engine.connect() as connection:
            count = await connection.scalar(select(func.count()).select_from(Payment))
        assert count == 1


class TestPaymentFinalize:
    async def test_moves_pending_payment_to_the_final_status(self, clean_database: Database):
        payment = await insert_payment(clean_database)

        async with clean_database.unit_of_work() as uow:
            finalized = await uow.payments.finalize(payment.id, PaymentStatus.SUCCEEDED, T0)
            await uow.commit()

        assert finalized is not None
        assert finalized.status is PaymentStatus.SUCCEEDED
        assert finalized.processed_at == T0
        stored = await load_payment(clean_database, payment.id)
        assert stored.status is PaymentStatus.SUCCEEDED
        assert stored.processed_at == T0

    async def test_second_finalize_returns_none_and_keeps_the_first_result(self, clean_database: Database):
        payment = await insert_payment(clean_database)
        async with clean_database.unit_of_work() as uow:
            await uow.payments.finalize(payment.id, PaymentStatus.SUCCEEDED, T0)
            await uow.commit()

        async with clean_database.unit_of_work() as uow:
            second = await uow.payments.finalize(payment.id, PaymentStatus.FAILED, T0 + timedelta(hours=1))
            await uow.commit()

        assert second is None
        stored = await load_payment(clean_database, payment.id)
        assert stored.status is PaymentStatus.SUCCEEDED
        assert stored.processed_at == T0

    async def test_finalize_of_an_already_final_payment_returns_none(self, clean_database: Database):
        payment = await insert_payment(clean_database)
        async with clean_database.engine.begin() as connection:
            await connection.execute(
                text("UPDATE payments SET status = 'failed' WHERE id = :id"), {"id": payment.id}
            )

        async with clean_database.unit_of_work() as uow:
            result = await uow.payments.finalize(payment.id, PaymentStatus.SUCCEEDED, T0)
            await uow.commit()

        assert result is None
        stored = await load_payment(clean_database, payment.id)
        assert stored.status is PaymentStatus.FAILED
        assert stored.processed_at is None

    async def test_finalize_of_an_unknown_payment_returns_none(self, clean_database: Database):
        async with clean_database.unit_of_work() as uow:
            result = await uow.payments.finalize(uuid.uuid4(), PaymentStatus.SUCCEEDED, T0)

        assert result is None

    async def test_concurrent_finalizers_have_exactly_one_winner(self, clean_database: Database):
        payment = await insert_payment(clean_database)

        async def finalize(status: PaymentStatus) -> Payment | None:
            async with clean_database.unit_of_work() as uow:
                result = await uow.payments.finalize(payment.id, status, utcnow())
                await uow.commit()
                return result

        results = await asyncio.wait_for(
            asyncio.gather(*(finalize(s) for s in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED))),
            timeout=30,
        )

        winners = [r for r in results if r is not None]
        assert len(winners) == 1
        stored = await load_payment(clean_database, payment.id)
        assert stored.status is winners[0].status


class TestMarkWebhookDelivered:
    async def test_sets_the_delivery_time(self, clean_database: Database):
        payment = await insert_payment(clean_database)

        async with clean_database.unit_of_work() as uow:
            await uow.payments.mark_webhook_delivered(payment.id, T0)
            await uow.commit()

        assert (await load_payment(clean_database, payment.id)).webhook_delivered_at == T0

    async def test_does_not_overwrite_an_existing_value(self, clean_database: Database):
        payment = await insert_payment(clean_database)
        async with clean_database.unit_of_work() as uow:
            await uow.payments.mark_webhook_delivered(payment.id, T0)
            await uow.commit()

        async with clean_database.unit_of_work() as uow:
            await uow.payments.mark_webhook_delivered(payment.id, T0 + timedelta(hours=1))
            await uow.commit()

        assert (await load_payment(clean_database, payment.id)).webhook_delivered_at == T0


class TestLockUnpublished:
    async def test_returns_oldest_first_and_respects_the_limit(self, clean_database: Database):
        events = [make_event(created_at=T0 + timedelta(minutes=minute)) for minute in range(5)]
        shuffled = [events[3], events[0], events[4], events[2], events[1]]
        await add_events(clean_database, shuffled)

        async with clean_database.unit_of_work() as uow:
            claimed = await uow.outbox.lock_unpublished(3)

        assert [event.id for event in claimed] == [event.id for event in events[:3]]

    async def test_skips_already_published_events(self, clean_database: Database):
        published = make_event(created_at=T0, published_at=T0 + timedelta(minutes=1))
        pending = make_event(created_at=T0 + timedelta(minutes=2))
        await add_events(clean_database, [published, pending])

        async with clean_database.unit_of_work() as uow:
            claimed = await uow.outbox.lock_unpublished(10)

        assert [event.id for event in claimed] == [pending.id]

    async def test_returns_nothing_for_an_empty_outbox(self, clean_database: Database):
        async with clean_database.unit_of_work() as uow:
            assert list(await uow.outbox.lock_unpublished(10)) == []

    async def test_second_transaction_skips_locked_rows_without_blocking(self, clean_database: Database):
        events = [make_event(created_at=T0 + timedelta(minutes=minute)) for minute in range(4)]
        await add_events(clean_database, events)

        async with clean_database.unit_of_work() as first, clean_database.unit_of_work() as second:
            locked_by_first = await first.outbox.lock_unpublished(2)
            claimed_by_second = await asyncio.wait_for(second.outbox.lock_unpublished(10), LOCK_TIMEOUT)

        assert [e.id for e in locked_by_first] == [e.id for e in events[:2]]
        assert [e.id for e in claimed_by_second] == [e.id for e in events[2:]]

    async def test_second_transaction_gets_nothing_when_every_row_is_locked(self, clean_database: Database):
        await add_events(clean_database, [make_event(created_at=T0 + timedelta(minutes=m)) for m in range(3)])

        async with clean_database.unit_of_work() as first, clean_database.unit_of_work() as second:
            assert len(await first.outbox.lock_unpublished(10)) == 3
            claimed_by_second = await asyncio.wait_for(second.outbox.lock_unpublished(10), LOCK_TIMEOUT)

        assert list(claimed_by_second) == []

    async def test_rows_become_available_again_once_the_lock_holder_rolls_back(
        self, clean_database: Database
    ):
        await add_events(clean_database, [make_event(), make_event()])
        async with clean_database.unit_of_work() as first:
            await first.outbox.lock_unpublished(10)

        async with clean_database.unit_of_work() as second:
            claimed = await second.outbox.lock_unpublished(10)

        assert len(claimed) == 2


class TestOutboxBookkeeping:
    async def test_mark_published_sets_time_counts_the_attempt_and_clears_the_error(
        self, clean_database: Database
    ):
        event = make_event()
        await add_events(clean_database, [event])
        async with clean_database.unit_of_work() as uow:
            await uow.outbox.record_failure(event.id, "boom")
            await uow.outbox.mark_published(event.id, T0)
            await uow.commit()

        stored = await load_event(clean_database, event.id)
        assert stored.published_at == T0
        assert stored.attempts == 2
        assert stored.last_error is None

    async def test_record_failure_counts_attempts_and_keeps_the_latest_error(self, clean_database: Database):
        event = make_event()
        await add_events(clean_database, [event])

        for message in ("first", "second"):
            async with clean_database.unit_of_work() as uow:
                await uow.outbox.record_failure(event.id, message)
                await uow.commit()

        stored = await load_event(clean_database, event.id)
        assert stored.attempts == 2
        assert stored.last_error == "second"
        assert stored.published_at is None

    async def test_record_failure_truncates_a_huge_error_message(self, clean_database: Database):
        event = make_event()
        await add_events(clean_database, [event])

        async with clean_database.unit_of_work() as uow:
            await uow.outbox.record_failure(event.id, "x" * 10_000)
            await uow.commit()

        stored = await load_event(clean_database, event.id)
        assert stored.last_error is not None
        assert len(stored.last_error) == 2000


class TestPurgePublishedBefore:
    async def test_removes_only_published_rows_older_than_the_cutoff(self, clean_database: Database):
        now = utcnow()
        old_published = make_event(created_at=now - timedelta(days=11), published_at=now - timedelta(days=10))
        recent_published = make_event(
            created_at=now - timedelta(hours=2), published_at=now - timedelta(hours=1)
        )
        old_unpublished = make_event(created_at=now - timedelta(days=30))
        await add_events(clean_database, [old_published, recent_published, old_unpublished])

        async with clean_database.unit_of_work() as uow:
            deleted = await uow.outbox.purge_published_before(now - timedelta(days=1))
            await uow.commit()

        assert deleted == 1
        assert await outbox_ids(clean_database) == {recent_published.id, old_unpublished.id}

    async def test_returns_zero_when_nothing_matches(self, clean_database: Database):
        await add_events(clean_database, [make_event()])

        async with clean_database.unit_of_work() as uow:
            deleted = await uow.outbox.purge_published_before(utcnow())

        assert deleted == 0


class TestDatabaseConstraints:
    @pytest.mark.parametrize("amount", [Decimal("0"), Decimal("0.00"), Decimal("-1.00")])
    async def test_non_positive_amount_is_rejected(self, clean_database: Database, amount: Decimal):
        with pytest.raises(IntegrityError, match="ck_payments_amount_positive"):
            await raw_insert_payment(clean_database, amount=amount)

    @pytest.mark.parametrize("currency", ["GBP", "rub", "R"])
    async def test_unknown_currency_is_rejected(self, clean_database: Database, currency: str):
        with pytest.raises(IntegrityError, match="check constraint"):
            await raw_insert_payment(clean_database, currency=currency)

    @pytest.mark.parametrize("status", ["bogus", "PENDING", ""])
    async def test_unknown_status_is_rejected(self, clean_database: Database, status: str):
        with pytest.raises(IntegrityError, match="check constraint"):
            await raw_insert_payment(clean_database, status=status)

    async def test_valid_row_is_accepted(self, clean_database: Database):
        await raw_insert_payment(clean_database, amount=Decimal("0.01"), currency="EUR", status="failed")

    async def test_duplicate_idempotency_key_violates_the_named_unique_constraint(
        self, clean_database: Database
    ):
        await raw_insert_payment(clean_database, key="dup")

        with pytest.raises(IntegrityError, match="uq_payments_idempotency_key"):
            await raw_insert_payment(clean_database, key="dup")

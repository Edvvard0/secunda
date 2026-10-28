from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest
from sqlalchemy import select

from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.exceptions import (
    IdempotencyKeyReuseError,
    InvalidIdempotencyKeyError,
    PaymentNotFoundError,
)
from payment_service.persistence import OutboxEvent, OutboxRepository, Payment
from payment_service.services.dto import CreatePaymentCommand
from payment_service.services.payments import PaymentService

if TYPE_CHECKING:
    from payment_service.persistence import Database

KEY = "key-0001"


def make_command(**overrides: Any) -> CreatePaymentCommand:
    fields: dict[str, Any] = {
        "amount": Decimal("100.50"),
        "currency": Currency.RUB,
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://client.example.com/hooks/payments",
    }
    fields.update(overrides)
    return CreatePaymentCommand(**fields)


async def fetch_payments(database: Database) -> list[Payment]:
    async with database.session_factory() as session:
        return list((await session.scalars(select(Payment))).all())


async def fetch_outbox(database: Database) -> list[OutboxEvent]:
    async with database.session_factory() as session:
        return list((await session.scalars(select(OutboxEvent))).all())


@pytest.fixture
def service(clean_database: Database) -> PaymentService:
    return PaymentService(clean_database.unit_of_work)


async def test_create_stores_pending_payment(service: PaymentService, clean_database: Database):
    result = await service.create(make_command(), idempotency_key=KEY, correlation_id="req-1")

    [payment] = await fetch_payments(clean_database)
    assert result.created is True
    assert result.payment.id == payment.id
    assert result.payment.status is PaymentStatus.PENDING
    assert payment.status is PaymentStatus.PENDING
    assert payment.amount == Decimal("100.50")
    assert payment.currency is Currency.RUB
    assert payment.description == "Order #42"
    assert payment.metadata_ == {"order_id": 42}
    assert payment.webhook_url == "https://client.example.com/hooks/payments"
    assert payment.idempotency_key == KEY
    assert payment.request_fingerprint == make_command().fingerprint()
    assert payment.created_at is not None
    assert payment.created_at.tzinfo is not None
    assert payment.processed_at is None
    assert payment.webhook_delivered_at is None
    assert result.payment.created_at == payment.created_at


async def test_create_writes_outbox_event_in_the_same_transaction(
    service: PaymentService, clean_database: Database
):
    result = await service.create(make_command(), idempotency_key=KEY, correlation_id="req-1")

    [outbox] = await fetch_outbox(clean_database)
    assert outbox.event_type == "payments.new"
    assert outbox.aggregate_id == result.payment.id
    assert outbox.correlation_id == "req-1"
    assert outbox.published_at is None
    assert outbox.attempts == 0
    assert outbox.payload["event_id"] == str(outbox.id)
    assert outbox.payload["payment_id"] == str(result.payment.id)
    assert outbox.payload["version"] == 1
    assert datetime.fromisoformat(outbox.payload["occurred_at"]).tzinfo is not None


async def test_create_without_correlation_id_stores_null(service: PaymentService, clean_database: Database):
    await service.create(make_command(), idempotency_key=KEY)

    [outbox] = await fetch_outbox(clean_database)
    assert outbox.correlation_id is None


async def test_create_truncates_correlation_id_to_column_width(
    service: PaymentService, clean_database: Database
):
    await service.create(make_command(), idempotency_key=KEY, correlation_id="r" * 100)

    [outbox] = await fetch_outbox(clean_database)
    assert outbox.correlation_id == "r" * 64


async def test_replay_with_same_key_and_body_returns_original_payment(
    service: PaymentService, clean_database: Database
):
    first = await service.create(make_command(), idempotency_key=KEY, correlation_id="req-1")

    second = await service.create(make_command(), idempotency_key=KEY, correlation_id="req-2")

    assert second.created is False
    assert second.payment == first.payment
    assert len(await fetch_payments(clean_database)) == 1
    [outbox] = await fetch_outbox(clean_database)
    assert outbox.correlation_id == "req-1"


async def test_replay_ignores_metadata_key_order_and_decimal_representation(
    service: PaymentService, clean_database: Database
):
    first = await service.create(
        make_command(amount=Decimal("10"), metadata={"a": 1, "b": 2}), idempotency_key=KEY
    )

    second = await service.create(
        make_command(amount=Decimal("10.00"), metadata={"b": 2, "a": 1}), idempotency_key=KEY
    )

    assert second.created is False
    assert second.payment.id == first.payment.id


async def test_same_key_with_different_body_is_rejected_and_writes_nothing(
    service: PaymentService, clean_database: Database
):
    await service.create(make_command(), idempotency_key=KEY)

    with pytest.raises(IdempotencyKeyReuseError):
        await service.create(make_command(amount=Decimal("999.00")), idempotency_key=KEY)

    [payment] = await fetch_payments(clean_database)
    assert payment.amount == Decimal("100.50")
    assert len(await fetch_outbox(clean_database)) == 1


async def test_different_keys_create_independent_payments(service: PaymentService, clean_database: Database):
    first = await service.create(make_command(), idempotency_key="key-a")
    second = await service.create(make_command(), idempotency_key="key-b")

    assert first.created is True
    assert second.created is True
    assert first.payment.id != second.payment.id
    assert len(await fetch_payments(clean_database)) == 2
    assert len(await fetch_outbox(clean_database)) == 2


async def test_concurrent_creates_with_same_key_produce_exactly_one_payment(
    service: PaymentService, clean_database: Database
):
    results = await asyncio.gather(*(service.create(make_command(), idempotency_key=KEY) for _ in range(10)))

    assert len(await fetch_payments(clean_database)) == 1
    assert len(await fetch_outbox(clean_database)) == 1
    assert len({result.payment.id for result in results}) == 1
    assert sum(result.created for result in results) == 1


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("", id="empty"),
        pytest.param("  ", id="blank"),
        pytest.param("a b", id="inner-space"),
        pytest.param(" lead", id="leading-space"),
        pytest.param("trail ", id="trailing-space"),
        pytest.param("a" * 256, id="256-chars"),
        pytest.param("ключ-идемпотентности", id="non-ascii"),
        pytest.param("tab\tkey", id="tab"),
        pytest.param("key\n", id="trailing-newline"),
        pytest.param("bell\x07", id="control-char"),
    ],
)
async def test_create_rejects_invalid_idempotency_key(
    service: PaymentService, clean_database: Database, key: str
):
    with pytest.raises(InvalidIdempotencyKeyError) as exc_info:
        await service.create(make_command(), idempotency_key=key)

    assert exc_info.value.detail
    assert await fetch_payments(clean_database) == []
    assert await fetch_outbox(clean_database) == []


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("a", id="single-char"),
        pytest.param("a" * 255, id="255-chars"),
        pytest.param("!~", id="range-bounds"),
        pytest.param("Order/42:retry#3", id="punctuation"),
        pytest.param(str(uuid.UUID(int=1)), id="uuid"),
    ],
)
async def test_create_accepts_valid_idempotency_key(service: PaymentService, key: str):
    result = await service.create(make_command(), idempotency_key=key)

    assert result.created is True
    assert result.payment.idempotency_key == key


async def test_create_leaves_no_payment_behind_when_outbox_write_fails(
    service: PaymentService, clean_database: Database, monkeypatch: pytest.MonkeyPatch
):
    def failing_add(self: OutboxRepository, entity: OutboxEvent) -> None:
        raise RuntimeError("outbox is down")

    monkeypatch.setattr(OutboxRepository, "add", failing_add)

    with pytest.raises(RuntimeError, match="outbox is down"):
        await service.create(make_command(), idempotency_key=KEY)

    assert await fetch_payments(clean_database) == []
    assert await fetch_outbox(clean_database) == []


async def test_get_returns_stored_payment(service: PaymentService):
    created = await service.create(make_command(), idempotency_key=KEY)

    assert await service.get(created.payment.id) == created.payment


async def test_get_raises_not_found_for_unknown_id(service: PaymentService):
    unknown = uuid.uuid4()

    with pytest.raises(PaymentNotFoundError, match=str(unknown)):
        await service.get(unknown)

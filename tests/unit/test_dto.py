from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.persistence.models import Payment
from payment_service.services.dto import CreatePaymentCommand, PaymentDTO


def make_command(**overrides: Any) -> CreatePaymentCommand:
    fields: dict[str, Any] = {
        "amount": Decimal("100.50"),
        "currency": Currency.RUB,
        "description": "Order #42",
        "metadata": {"order_id": 42, "tags": ["a", "b"]},
        "webhook_url": "https://client.example.com/hooks/payments",
    }
    fields.update(overrides)
    return CreatePaymentCommand(**fields)


def test_fingerprint_is_stable_for_equal_commands():
    assert make_command().fingerprint() == make_command().fingerprint()


def test_fingerprint_is_a_sha256_hex_digest():
    fingerprint = make_command().fingerprint()

    assert len(fingerprint) == 64
    assert int(fingerprint, 16) >= 0


def test_fingerprint_ignores_metadata_key_order():
    first = make_command(metadata={"a": 1, "b": {"x": 1, "y": 2}})
    second = make_command(metadata={"b": {"y": 2, "x": 1}, "a": 1})

    assert first.fingerprint() == second.fingerprint()


def test_fingerprint_keeps_list_order_in_metadata():
    first = make_command(metadata={"items": [1, 2]})
    second = make_command(metadata={"items": [2, 1]})

    assert first.fingerprint() != second.fingerprint()


@pytest.mark.parametrize("equivalent", [Decimal("10"), Decimal("10.0"), Decimal("10.00"), Decimal("1E+1")])
def test_fingerprint_treats_equal_amounts_alike(equivalent: Decimal):
    assert (
        make_command(amount=equivalent).fingerprint() == make_command(amount=Decimal("10.00")).fingerprint()
    )


@pytest.mark.parametrize(
    "change",
    [
        {"amount": Decimal("100.51")},
        {"currency": Currency.USD},
        {"description": "Order #43"},
        {"metadata": {"order_id": 43}},
        {"metadata": {}},
        {"webhook_url": "https://client.example.com/hooks/other"},
    ],
    ids=lambda change: next(iter(change)),
)
def test_fingerprint_differs_when_any_field_differs(change: dict[str, Any]):
    base = make_command()

    assert replace(base, **change).fingerprint() != base.fingerprint()


def test_fingerprint_handles_non_ascii_text():
    command = make_command(description="Заказ №42 ☃", metadata={"имя": "значение"})

    assert (
        command.fingerprint()
        == make_command(description="Заказ №42 ☃", metadata={"имя": "значение"}).fingerprint()
    )
    assert command.fingerprint() != make_command().fingerprint()


def test_payment_dto_maps_every_field_of_the_model():
    created_at = datetime(2026, 10, 27, 12, 0, 0, tzinfo=UTC)
    processed_at = datetime(2026, 10, 27, 12, 0, 4, tzinfo=UTC)
    delivered_at = datetime(2026, 10, 27, 12, 0, 5, tzinfo=UTC)
    payment_id = uuid.uuid4()
    payment = Payment(
        id=payment_id,
        amount=Decimal("100.50"),
        currency=Currency.EUR,
        description="Order #42",
        metadata_={"order_id": 42},
        status=PaymentStatus.SUCCEEDED,
        idempotency_key="key-1",
        request_fingerprint="f" * 64,
        webhook_url="https://client.example.com/hook",
        created_at=created_at,
        processed_at=processed_at,
        webhook_delivered_at=delivered_at,
    )

    dto = PaymentDTO.from_model(payment)

    assert dto == PaymentDTO(
        id=payment_id,
        amount=Decimal("100.50"),
        currency=Currency.EUR,
        description="Order #42",
        metadata={"order_id": 42},
        status=PaymentStatus.SUCCEEDED,
        idempotency_key="key-1",
        webhook_url="https://client.example.com/hook",
        created_at=created_at,
        processed_at=processed_at,
        webhook_delivered_at=delivered_at,
    )


def test_payment_dto_keeps_optional_timestamps_empty_for_pending_payment():
    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("1.00"),
        currency=Currency.USD,
        description="x",
        metadata_={},
        status=PaymentStatus.PENDING,
        idempotency_key="key-2",
        request_fingerprint="0" * 64,
        webhook_url="https://client.example.com/hook",
        created_at=datetime(2026, 10, 27, tzinfo=UTC),
    )

    dto = PaymentDTO.from_model(payment)

    assert dto.status is PaymentStatus.PENDING
    assert dto.processed_at is None
    assert dto.webhook_delivered_at is None


def test_payment_dto_does_not_share_metadata_with_the_model():
    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("1.00"),
        currency=Currency.USD,
        description="x",
        metadata_={"k": "v"},
        status=PaymentStatus.PENDING,
        idempotency_key="key-3",
        request_fingerprint="0" * 64,
        webhook_url="https://client.example.com/hook",
        created_at=datetime(2026, 10, 27, tzinfo=UTC),
    )

    dto = PaymentDTO.from_model(payment)
    payment.metadata_["k"] = "changed"

    assert dto.metadata == {"k": "v"}

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest
from pydantic import ValidationError

from payment_service.api.schemas import (
    CreatePaymentRequest,
    PaymentAcceptedResponse,
    PaymentResponse,
    ProblemDetails,
)
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.services.dto import PaymentDTO

if TYPE_CHECKING:
    import httpx


@pytest.fixture
def payment() -> PaymentDTO:
    return PaymentDTO(
        id=uuid.UUID("0b5f3d0e-6a77-4c41-9a43-2f9e1c8f7a10"),
        amount=Decimal(7),
        currency=Currency.EUR,
        description="Coffee",
        metadata={"table": 3},
        status=PaymentStatus.FAILED,
        idempotency_key="secret-key",
        webhook_url="https://client.example.com/hook",
        created_at=datetime(2026, 1, 15, 10, 30, tzinfo=UTC),
        processed_at=datetime(2026, 1, 15, 10, 30, 3, tzinfo=UTC),
        webhook_delivered_at=None,
    )


def test_request_normalises_the_amount_to_two_decimals(payment_body: dict[str, Any]) -> None:
    request = CreatePaymentRequest.model_validate({**payment_body, "amount": "7.5"})

    assert str(request.amount) == "7.50"


def test_request_builds_the_use_case_command(payment_body: dict[str, Any]) -> None:
    command = CreatePaymentRequest.model_validate(payment_body).to_command()

    assert command.amount == Decimal("100.50")
    assert command.currency is Currency.RUB
    assert command.webhook_url == payment_body["webhook_url"]


def test_command_can_carry_a_normalised_webhook_url(payment_body: dict[str, Any]) -> None:
    command = CreatePaymentRequest.model_validate(payment_body).to_command(
        webhook_url="https://normalised.example/"
    )

    assert command.webhook_url == "https://normalised.example/"


def test_request_rejects_unknown_fields(payment_body: dict[str, Any]) -> None:
    with pytest.raises(ValidationError) as raised:
        CreatePaymentRequest.model_validate({**payment_body, "extra": 1})

    assert raised.value.errors()[0]["type"] == "extra_forbidden"


def test_metadata_default_is_not_shared_between_instances(payment_body: dict[str, Any]) -> None:
    del payment_body["metadata"]
    first = CreatePaymentRequest.model_validate(payment_body)
    second = CreatePaymentRequest.model_validate(payment_body)

    first.metadata["leak"] = True

    assert second.metadata == {}


def test_accepted_response_exposes_only_id_status_and_creation_time(payment: PaymentDTO) -> None:
    body = PaymentAcceptedResponse.from_dto(payment).model_dump(mode="json")

    assert body == {
        "payment_id": "0b5f3d0e-6a77-4c41-9a43-2f9e1c8f7a10",
        "status": "failed",
        "created_at": "2026-01-15T10:30:00Z",
    }


def test_payment_response_serialises_money_as_a_string_but_keeps_the_decimal_in_python(
    payment: PaymentDTO,
) -> None:
    response = PaymentResponse.from_dto(payment)

    assert response.model_dump(mode="json")["amount"] == "7.00"
    assert response.model_dump()["amount"] == Decimal(7)
    assert '"amount":"7.00"' in response.model_dump_json()


def test_payment_response_hides_internal_fields(payment: PaymentDTO) -> None:
    assert set(PaymentResponse.from_dto(payment).model_dump()) == {
        "payment_id",
        "amount",
        "currency",
        "description",
        "metadata",
        "status",
        "webhook_url",
        "created_at",
        "processed_at",
    }


@pytest.mark.parametrize(
    "make_request",
    [
        pytest.param(lambda c: c.get("/missing"), id="404"),
        pytest.param(lambda c: c.get("/api/v1/payments/not-a-uuid"), id="422"),
        pytest.param(
            lambda c: c.get("/api/v1/payments/00000000-0000-0000-0000-000000000000"), id="domain-404"
        ),
    ],
)
async def test_real_problem_documents_match_the_documented_model(
    client: httpx.AsyncClient, make_request: Any
) -> None:
    response = await make_request(client)

    problem = ProblemDetails.model_validate(response.json())
    assert problem.status == response.status_code
    assert problem.request_id == response.headers["x-request-id"]
    assert problem.type.endswith(f"/{problem.code}")


async def test_real_validation_problem_matches_the_documented_model(
    client: httpx.AsyncClient, payment_body: dict[str, Any]
) -> None:
    payment_body["amount"] = "0"

    response = await client.post("/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "k"})

    problem = ProblemDetails.model_validate(response.json())
    assert problem.errors is not None
    assert problem.errors[0].loc == ["body", "amount"]

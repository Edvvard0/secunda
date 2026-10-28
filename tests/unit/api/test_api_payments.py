from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

from payment_service.domain.enums import Currency, PaymentStatus

if TYPE_CHECKING:
    import httpx

PAYMENTS_URL = "/api/v1/payments"
PROBLEM_JSON = "application/problem+json"


async def test_create_returns_202_with_payment_fields(
    client: httpx.AsyncClient, payment_body: dict[str, Any], idempotency_headers: dict[str, str]
) -> None:
    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 202
    assert response.headers["content-type"] == "application/json"
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert uuid.UUID(body["payment_id"])
    assert body["status"] == "pending"
    assert datetime.fromisoformat(body["created_at"]).tzinfo is not None


async def test_create_sets_location_header_pointing_at_the_payment(
    client: httpx.AsyncClient, payment_body: dict[str, Any], idempotency_headers: dict[str, str]
) -> None:
    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.headers["location"] == f"/api/v1/payments/{response.json()['payment_id']}"
    assert "idempotent-replayed" not in response.headers


async def test_create_echoes_a_valid_request_id(
    client: httpx.AsyncClient, payment_body: dict[str, Any], idempotency_headers: dict[str, str]
) -> None:
    response = await client.post(
        PAYMENTS_URL, json=payment_body, headers={**idempotency_headers, "X-Request-ID": "req-123.abc_DEF"}
    )

    assert response.headers["x-request-id"] == "req-123.abc_DEF"


async def test_create_passes_request_id_key_and_command_to_the_service(
    client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
) -> None:
    await client.post(
        PAYMENTS_URL, json=payment_body, headers={**idempotency_headers, "X-Request-ID": "trace-1"}
    )

    (call,) = fake_service.calls
    assert call.idempotency_key == "key-0001"
    assert call.correlation_id == "trace-1"
    assert call.command.amount == Decimal("100.50")
    assert call.command.currency is Currency.RUB
    assert call.command.description == "Order #42"
    assert call.command.metadata == {"order_id": 42}
    assert call.command.webhook_url == "https://client.example.com/hooks/payments"


async def test_replay_of_the_same_request_returns_the_same_payment_and_flags_it(
    client: httpx.AsyncClient, payment_body: dict[str, Any], idempotency_headers: dict[str, str]
) -> None:
    first = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)
    second = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert second.status_code == 202
    assert second.headers["idempotent-replayed"] == "true"
    assert second.json() == first.json()
    assert second.headers["location"] == first.headers["location"]
    assert "idempotent-replayed" not in first.headers


async def test_replay_with_an_equivalent_body_is_still_a_replay(
    client: httpx.AsyncClient, payment_body: dict[str, Any], idempotency_headers: dict[str, str]
) -> None:
    first = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)
    equivalent = {**payment_body, "amount": 100.5, "description": "  Order #42  "}

    second = await client.post(PAYMENTS_URL, json=equivalent, headers=idempotency_headers)

    assert second.headers["idempotent-replayed"] == "true"
    assert second.json() == first.json()


async def test_same_key_with_another_body_is_rejected_with_422(
    client: httpx.AsyncClient, payment_body: dict[str, Any], idempotency_headers: dict[str, str]
) -> None:
    await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    response = await client.post(
        PAYMENTS_URL, json={**payment_body, "amount": "999.00"}, headers=idempotency_headers
    )

    assert response.status_code == 422
    assert response.headers["content-type"] == PROBLEM_JSON
    assert response.json()["code"] == "idempotency-key-reused"
    assert "errors" not in response.json()


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param({}, id="missing"),
        pytest.param({"Idempotency-Key": ""}, id="empty"),
        pytest.param({"Idempotency-Key": "k" * 256}, id="too-long"),
        pytest.param({"Idempotency-Key": "has space"}, id="space"),
        pytest.param({"Idempotency-Key": "ключ".encode()}, id="non-ascii"),
    ],
)
async def test_create_rejects_a_bad_idempotency_key_with_400(
    client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    headers: dict[str, str | bytes],
) -> None:
    response = await client.post(PAYMENTS_URL, json=payment_body, headers=headers)

    assert response.status_code == 400
    assert response.headers["content-type"] == PROBLEM_JSON
    body = response.json()
    assert body["code"] == "invalid-idempotency-key"
    assert body["status"] == 400
    assert body["type"] == "https://payment-service/errors/invalid-idempotency-key"
    assert len(fake_service.calls) == 1  # the use case, not the route, owns the rule


async def test_create_accepts_a_key_of_maximum_length(
    client: httpx.AsyncClient, payment_body: dict[str, Any]
) -> None:
    response = await client.post(PAYMENTS_URL, json=payment_body, headers={"Idempotency-Key": "k" * 255})

    assert response.status_code == 202


@pytest.mark.parametrize(
    ("sent", "stored"),
    [
        pytest.param("100", Decimal("100.00"), id="integer-string"),
        pytest.param("100.5", Decimal("100.50"), id="one-decimal"),
        pytest.param(100, Decimal("100.00"), id="json-integer"),
        pytest.param(100.5, Decimal("100.50"), id="json-number"),
        pytest.param("0.01", Decimal("0.01"), id="smallest"),
        pytest.param("9999999999999999.99", Decimal("9999999999999999.99"), id="largest"),
    ],
)
async def test_create_accepts_amount_forms_and_normalises_to_two_decimals(
    client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
    *,
    sent: object,
    stored: Decimal,
) -> None:
    payment_body["amount"] = sent

    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 202
    assert fake_service.calls[0].command.amount == stored
    assert str(fake_service.calls[0].command.amount) == str(stored)


async def test_create_defaults_metadata_to_an_empty_object_and_trims_the_description(
    client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
) -> None:
    del payment_body["metadata"]
    payment_body["description"] = "  padded  "

    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 202
    assert fake_service.calls[0].command.metadata == {}
    assert fake_service.calls[0].command.description == "padded"


async def test_create_accepts_values_at_every_documented_limit(
    client: httpx.AsyncClient, payment_body: dict[str, Any], idempotency_headers: dict[str, str]
) -> None:
    def compact(value: object) -> int:
        return len(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode())

    prefix = "https://client.example.com/"
    payment_body["description"] = "d" * 1000
    payment_body["webhook_url"] = prefix + "a" * (2048 - len(prefix))
    # Five levels of nesting and exactly 4096 bytes of compact JSON (multi-byte characters count as bytes).
    payment_body["metadata"] = {"a": {"b": {"c": {"d": {"e": ""}}}}}
    padding = 4096 - compact(payment_body["metadata"])
    payment_body["metadata"]["a"]["b"]["c"]["d"]["e"] = "x" * (padding - 2) + "é"
    assert compact(payment_body["metadata"]) == 4096

    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 202, response.text


async def test_create_normalises_the_webhook_url_before_it_reaches_the_service(
    client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
) -> None:
    payment_body["webhook_url"] = "HTTPS://Client.Example.COM:443/hooks?a=b#fragment"

    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 202
    assert fake_service.calls[0].command.webhook_url == "https://client.example.com/hooks?a=b"


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8080/hook",
        "http://localhost/hook",
        "http://10.0.0.5/hook",
        "http://169.254.169.254/latest/meta-data",
        "http://[::1]/hook",
    ],
)
async def test_create_rejects_private_webhook_targets_when_they_are_not_allowed(
    strict_client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
    url: str,
) -> None:
    payment_body["webhook_url"] = url

    response = await strict_client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 422
    assert response.headers["content-type"] == PROBLEM_JSON
    body = response.json()
    assert body["code"] == "invalid-webhook-url"
    assert body["title"] == "webhook_url is not allowed"
    assert fake_service.calls == []


async def test_create_accepts_public_webhook_targets_when_private_ones_are_not_allowed(
    strict_client: httpx.AsyncClient, payment_body: dict[str, Any], idempotency_headers: dict[str, str]
) -> None:
    response = await strict_client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 202


async def test_create_accepts_private_webhook_targets_when_they_are_allowed(
    client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
) -> None:
    # The default test settings allow private networks (like the compose demo does).
    payment_body["webhook_url"] = "http://127.0.0.1:8080/hook"

    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 202
    assert fake_service.calls[0].command.webhook_url == "http://127.0.0.1:8080/hook"


async def test_get_returns_the_full_payment(client: httpx.AsyncClient, fake_service: Any) -> None:
    created_at = datetime(2026, 1, 15, 10, 30, tzinfo=UTC)
    payment = fake_service.add_payment(
        amount=Decimal("100.5"),
        status=PaymentStatus.SUCCEEDED,
        created_at=created_at,
        processed_at=datetime(2026, 1, 15, 10, 30, 4, tzinfo=UTC),
    )

    response = await client.get(f"{PAYMENTS_URL}/{payment.id}")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert response.json() == {
        "payment_id": str(payment.id),
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "status": "succeeded",
        "webhook_url": "https://client.example.com/hooks/payments",
        "created_at": "2026-01-15T10:30:00Z",
        "processed_at": "2026-01-15T10:30:04Z",
    }


async def test_get_returns_amount_as_a_string_with_two_decimals(
    client: httpx.AsyncClient, fake_service: Any
) -> None:
    payment = fake_service.add_payment(amount=Decimal(100))

    response = await client.get(f"{PAYMENTS_URL}/{payment.id}")

    assert response.json()["amount"] == "100.00"
    assert '"amount":"100.00"' in response.text.replace(" ", "")


async def test_get_reports_null_processed_at_while_pending(
    client: httpx.AsyncClient, fake_service: Any
) -> None:
    payment = fake_service.add_payment()

    body = (await client.get(f"{PAYMENTS_URL}/{payment.id}")).json()

    assert body["status"] == "pending"
    assert body["processed_at"] is None


async def test_get_does_not_expose_internal_fields(client: httpx.AsyncClient, fake_service: Any) -> None:
    payment = fake_service.add_payment()

    body = (await client.get(f"{PAYMENTS_URL}/{payment.id}")).json()

    assert "idempotency_key" not in body
    assert "webhook_delivered_at" not in body


async def test_get_unknown_payment_returns_404_problem(client: httpx.AsyncClient) -> None:
    missing = uuid.uuid4()

    response = await client.get(f"{PAYMENTS_URL}/{missing}", headers={"X-Request-ID": "lookup-1"})

    assert response.status_code == 404
    assert response.headers["content-type"] == PROBLEM_JSON
    assert response.json() == {
        "type": "https://payment-service/errors/payment-not-found",
        "title": "Payment not found",
        "status": 404,
        "detail": f"Payment {missing} not found",
        "code": "payment-not-found",
        "request_id": "lookup-1",
    }


async def test_get_with_an_invalid_uuid_returns_422_without_echoing_it(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS_URL}/not-a-uuid-SENTINEL")

    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "validation-error"
    assert body["errors"][0]["loc"] == ["path", "payment_id"]
    assert body["errors"][0]["type"] == "uuid_parsing"
    assert "SENTINEL" not in response.text
    assert "not-a-uuid" not in response.text

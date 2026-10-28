from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    import httpx

PAYMENTS_URL = "/api/v1/payments"
PROBLEM_JSON = "application/problem+json"
SENTINEL = "SENTINEL-must-never-be-echoed"


def _nested(depth: int) -> dict[str, Any]:
    """An object with ``depth`` levels of nesting (the top-level object is level one)."""
    value: dict[str, Any] = {}
    for _ in range(depth - 1):
        value = {"k": value}
    return value


INVALID_FIELDS = [
    pytest.param("amount", "0", id="amount-zero"),
    pytest.param("amount", 0, id="amount-zero-number"),
    pytest.param("amount", "-5.00", id="amount-negative"),
    pytest.param("amount", -5, id="amount-negative-number"),
    pytest.param("amount", "100.505", id="amount-three-decimals"),
    pytest.param("amount", "abc" + SENTINEL, id="amount-not-a-number"),
    pytest.param("amount", "1e2", id="amount-exponent"),
    pytest.param("amount", " 100.50", id="amount-whitespace"),
    pytest.param("amount", "1_000", id="amount-underscore"),
    pytest.param("amount", "NaN", id="amount-nan-string"),
    pytest.param("amount", "1234567890123456789", id="amount-19-digits"),
    pytest.param("amount", "99999999999999999.99", id="amount-too-many-digits"),
    pytest.param("amount", 0.30000000000000004, id="amount-float-noise"),
    pytest.param("amount", 12345678901234567.89, id="amount-float-loses-precision"),
    pytest.param("amount", True, id="amount-boolean"),
    pytest.param("amount", None, id="amount-null"),
    pytest.param("amount", [1], id="amount-list"),
    pytest.param("currency", "GBP", id="currency-unsupported"),
    pytest.param("currency", "rub", id="currency-lower-case"),
    pytest.param("currency", 643, id="currency-number"),
    pytest.param("currency", None, id="currency-null"),
    pytest.param("description", "", id="description-empty"),
    pytest.param("description", "   ", id="description-blank"),
    pytest.param("description", SENTINEL + "x" * 1001, id="description-too-long"),
    pytest.param("description", 42, id="description-number"),
    pytest.param("description", None, id="description-null"),
    pytest.param("metadata", {"blob": SENTINEL + "x" * 4096}, id="metadata-too-big"),
    pytest.param("metadata", {"é": "é" * 2100}, id="metadata-too-big-in-bytes-not-characters"),
    pytest.param("metadata", _nested(6), id="metadata-too-deep"),
    pytest.param("metadata", {"list": [[[[[1]]]]]}, id="metadata-too-deep-through-lists"),
    pytest.param("metadata", [1, 2], id="metadata-list"),
    pytest.param("metadata", SENTINEL, id="metadata-string"),
    pytest.param("metadata", None, id="metadata-null"),
    pytest.param("webhook_url", "ftp://" + SENTINEL + ".example.com/hook", id="url-ftp"),
    pytest.param("webhook_url", "javascript:alert(1)", id="url-javascript"),
    pytest.param("webhook_url", "example.com/" + SENTINEL, id="url-without-scheme"),
    pytest.param("webhook_url", "https:///" + SENTINEL, id="url-without-host"),
    pytest.param("webhook_url", "https://user:" + SENTINEL + "@example.com/hook", id="url-userinfo"),
    pytest.param("webhook_url", "https://" + SENTINEL + "@example.com/hook", id="url-user-only"),
    pytest.param("webhook_url", "https://example.com/" + SENTINEL + "a" * 2048, id="url-too-long"),
    pytest.param("webhook_url", " https://example.com/hook", id="url-leading-space"),
    pytest.param("webhook_url", "https://example.com/ho\nok", id="url-newline"),
    pytest.param("webhook_url", "https://example.com:99999/hook", id="url-port-out-of-range"),
    pytest.param("webhook_url", "https://example.com:" + SENTINEL + "/hook", id="url-port-not-numeric"),
    pytest.param("webhook_url", 42, id="url-number"),
    pytest.param("webhook_url", None, id="url-null"),
]


@pytest.mark.parametrize(("field", "value"), INVALID_FIELDS)
async def test_invalid_field_is_rejected_with_a_validation_problem(
    client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
    *,
    field: str,
    value: object,
) -> None:
    payment_body[field] = value

    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 422, response.text
    assert response.headers["content-type"] == PROBLEM_JSON
    body = response.json()
    assert body["code"] == "validation-error"
    assert body["type"] == "https://payment-service/errors/validation-error"
    assert body["status"] == 422
    assert body["request_id"] == response.headers["x-request-id"]
    assert body["errors"]
    for error in body["errors"]:
        assert set(error) == {"loc", "msg", "type"}
        assert error["loc"][:2] == ["body", field]
    assert SENTINEL not in response.text
    assert fake_service.calls == []


@pytest.mark.parametrize("field", ["amount", "currency", "description", "webhook_url"])
async def test_missing_required_field_is_rejected(
    client: httpx.AsyncClient,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
    field: str,
) -> None:
    del payment_body[field]

    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 422
    error = response.json()["errors"][0]
    assert error["loc"] == ["body", field]
    assert error["type"] == "missing"


async def test_unknown_field_is_rejected_without_echoing_its_value(
    client: httpx.AsyncClient, payment_body: dict[str, Any], idempotency_headers: dict[str, str]
) -> None:
    payment_body["unexpected"] = SENTINEL

    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 422
    assert response.json()["errors"] == [
        {"loc": ["body", "unexpected"], "msg": "Extra inputs are not permitted", "type": "extra_forbidden"}
    ]
    assert SENTINEL not in response.text


async def test_all_problems_are_reported_at_once(
    client: httpx.AsyncClient, payment_body: dict[str, Any], idempotency_headers: dict[str, str]
) -> None:
    payment_body.update(amount="0", currency="GBP", description="")

    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert {error["loc"][1] for error in response.json()["errors"]} == {"amount", "currency", "description"}


async def test_malformed_json_is_rejected_without_echoing_the_body(
    client: httpx.AsyncClient, idempotency_headers: dict[str, str]
) -> None:
    response = await client.post(
        PAYMENTS_URL,
        content=b'{"amount": "' + SENTINEL.encode() + b'"',
        headers={**idempotency_headers, "Content-Type": "application/json"},
    )

    assert response.status_code == 422
    assert response.headers["content-type"] == PROBLEM_JSON
    body = response.json()
    assert body["code"] == "validation-error"
    assert body["errors"][0]["type"] == "json_invalid"
    assert set(body["errors"][0]) == {"loc", "msg", "type"}
    assert SENTINEL not in response.text


async def test_empty_body_is_rejected(client: httpx.AsyncClient, idempotency_headers: dict[str, str]) -> None:
    response = await client.post(
        PAYMENTS_URL, content=b"", headers={**idempotency_headers, "Content-Type": "application/json"}
    )

    assert response.status_code == 422
    assert response.json()["errors"][0]["type"] == "missing"


async def test_non_json_content_type_is_rejected(
    client: httpx.AsyncClient, payment_body: dict[str, Any], idempotency_headers: dict[str, str]
) -> None:
    response = await client.post(
        PAYMENTS_URL,
        content=json.dumps(payment_body),
        headers={**idempotency_headers, "Content-Type": "text/plain"},
    )

    assert response.status_code == 422
    assert response.headers["content-type"] == PROBLEM_JSON


async def test_pathologically_nested_json_does_not_crash_the_service(
    client: httpx.AsyncClient, idempotency_headers: dict[str, str]
) -> None:
    response = await client.post(
        PAYMENTS_URL,
        content=b"[" * 60_000,  # below the 64 KiB size limit, far above the parser recursion limit
        headers={**idempotency_headers, "Content-Type": "application/json"},
    )

    assert response.status_code in {400, 422}
    assert response.headers["content-type"] == PROBLEM_JSON


async def test_non_finite_number_literals_are_rejected(
    client: httpx.AsyncClient, idempotency_headers: dict[str, str]
) -> None:
    body = (
        b'{"amount": NaN, "currency": "RUB", "description": "d", "webhook_url": "https://example.com/h",'
        b' "metadata": {"x": Infinity}}'
    )

    response = await client.post(
        PAYMENTS_URL, content=body, headers={**idempotency_headers, "Content-Type": "application/json"}
    )

    assert response.status_code == 422
    assert response.headers["content-type"] == PROBLEM_JSON

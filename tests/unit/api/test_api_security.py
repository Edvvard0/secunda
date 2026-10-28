from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pytest
from pydantic import SecretStr

from payment_service.api import security
from payment_service.api.security import ApiKeyAuth
from payment_service.domain.exceptions import UnauthorizedError

if TYPE_CHECKING:
    import httpx

PAYMENTS_URL = "/api/v1/payments"
SECRET = "unit-test-key-0123456789"


async def test_missing_key_is_rejected_with_401_problem(
    anonymous_client: httpx.AsyncClient, payment_body: dict[str, Any], idempotency_headers: dict[str, str]
) -> None:
    response = await anonymous_client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 401
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["code"] == "unauthorized"
    assert body["status"] == 401
    assert body["type"] == "https://payment-service/errors/unauthorized"
    assert body["request_id"] == response.headers["x-request-id"]


async def test_wrong_key_is_rejected_exactly_like_a_missing_one(
    client: httpx.AsyncClient,
    anonymous_client: httpx.AsyncClient,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
) -> None:
    missing = await anonymous_client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)
    wrong = await client.post(
        PAYMENTS_URL, json=payment_body, headers={**idempotency_headers, "X-API-Key": "WRONG-KEY-SENTINEL"}
    )

    assert wrong.status_code == 401
    assert {k: v for k, v in wrong.json().items() if k != "request_id"} == {
        k: v for k, v in missing.json().items() if k != "request_id"
    }
    assert "WRONG-KEY-SENTINEL" not in wrong.text


@pytest.mark.parametrize("sent", ["", "x", "k" * 10_000], ids=["empty", "too-short", "huge"])
async def test_keys_of_any_shape_are_rejected(client: httpx.AsyncClient, sent: str) -> None:
    response = await client.get(
        f"{PAYMENTS_URL}/00000000-0000-0000-0000-000000000000", headers={"X-API-Key": sent}
    )

    assert response.status_code == 401


async def test_a_prefix_of_the_key_is_not_enough(client: httpx.AsyncClient, api_key: str) -> None:
    response = await client.get(
        f"{PAYMENTS_URL}/00000000-0000-0000-0000-000000000000", headers={"X-API-Key": api_key[:-1]}
    )

    assert response.status_code == 401


async def test_401_does_not_challenge_with_basic_or_bearer(anonymous_client: httpx.AsyncClient) -> None:
    response = await anonymous_client.get(f"{PAYMENTS_URL}/00000000-0000-0000-0000-000000000000")

    assert response.status_code == 401
    assert "www-authenticate" not in response.headers


async def test_correct_key_passes_on_both_endpoints(
    client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
) -> None:
    created = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)
    fetched = await client.get(f"{PAYMENTS_URL}/{created.json()['payment_id']}")

    assert created.status_code == 202
    assert fetched.status_code == 200


async def test_header_name_is_case_insensitive(anonymous_client: httpx.AsyncClient, api_key: str) -> None:
    response = await anonymous_client.get(
        f"{PAYMENTS_URL}/00000000-0000-0000-0000-000000000000", headers={"x-api-key": api_key}
    )

    assert response.status_code == 404  # authenticated, then "payment not found"


async def test_authentication_runs_before_the_use_case(
    anonymous_client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
) -> None:
    await anonymous_client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert fake_service.calls == []


@pytest.mark.parametrize("url", ["/health/live", "/health/ready"])
async def test_health_endpoints_need_no_key(anonymous_client: httpx.AsyncClient, url: str) -> None:
    response = await anonymous_client.get(url)

    assert response.status_code == 200


@pytest.mark.parametrize("url", ["/docs", "/redoc", "/openapi.json"])
async def test_documentation_needs_no_key(anonymous_client: httpx.AsyncClient, url: str) -> None:
    response = await anonymous_client.get(url)

    assert response.status_code == 200


async def test_the_key_is_never_written_to_the_logs(
    client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture, api_key: str
) -> None:
    caplog.set_level(logging.DEBUG)

    await client.get(
        f"{PAYMENTS_URL}/00000000-0000-0000-0000-000000000000", headers={"X-API-Key": "WRONG-SENTINEL"}
    )
    await client.get(f"{PAYMENTS_URL}/00000000-0000-0000-0000-000000000000")

    assert "WRONG-SENTINEL" not in caplog.text
    assert api_key not in caplog.text


async def test_auth_accepts_the_configured_key() -> None:
    auth = ApiKeyAuth(SecretStr(SECRET))

    assert await auth(SECRET) is None


@pytest.mark.parametrize("provided", [None, "", SECRET + "x", SECRET[:-1], SECRET.upper(), "ключ"])
async def test_auth_rejects_everything_else(provided: str | None) -> None:
    auth = ApiKeyAuth(SecretStr(SECRET))

    with pytest.raises(UnauthorizedError):
        await auth(provided)


async def test_auth_compares_utf8_bytes_with_compare_digest(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[object, object]] = []

    def spy(left: bytes, right: bytes) -> bool:
        calls.append((left, right))
        return left == right

    monkeypatch.setattr(security.secrets, "compare_digest", spy)

    await ApiKeyAuth(SecretStr(SECRET))(SECRET)

    assert calls == [(SECRET.encode(), SECRET.encode())]


async def test_auth_does_not_expose_the_key_through_repr_or_errors() -> None:
    auth = ApiKeyAuth(SecretStr(SECRET))

    with pytest.raises(UnauthorizedError) as raised:
        await auth("another-key-0123456789")

    assert SECRET not in repr(auth)
    assert SECRET not in str(raised.value)
    assert "another-key-0123456789" not in str(raised.value)

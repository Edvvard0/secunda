from __future__ import annotations

import io
import json
import logging
from typing import TYPE_CHECKING, Any, ClassVar

from payment_service.api.app import create_app
from payment_service.domain.exceptions import AppError
from payment_service.logging_config import JsonFormatter

if TYPE_CHECKING:
    import httpx
    from fastapi import FastAPI

PAYMENTS_URL = "/api/v1/payments"
PROBLEM_JSON = "application/problem+json"
PROBLEM_KEYS = {"type", "title", "status", "detail", "code", "request_id"}


class TeapotError(AppError):
    """An error nobody registered a handler for: the generic handler must render it."""

    status_code: ClassVar[int] = 418
    code: ClassVar[str] = "teapot"
    title: ClassVar[str] = "I am a teapot"


async def test_unhandled_exception_becomes_a_bare_500_problem(
    client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
) -> None:
    fake_service.failure = RuntimeError("boom-SENTINEL at /secret/path.py")

    response = await client.post(
        PAYMENTS_URL, json=payment_body, headers={**idempotency_headers, "X-Request-ID": "crash-1"}
    )

    assert response.status_code == 500
    assert response.headers["content-type"] == PROBLEM_JSON
    assert response.headers["x-request-id"] == "crash-1"
    body = response.json()
    assert set(body) == PROBLEM_KEYS
    assert body["type"] == "https://payment-service/errors/internal-error"
    assert body["code"] == "internal-error"
    assert body["status"] == 500
    assert body["request_id"] == "crash-1"
    for leaked in ("SENTINEL", "RuntimeError", "Traceback", "secret", ".py"):
        assert leaked not in response.text


async def test_unhandled_exception_is_logged_with_traceback_and_request_id(
    app: FastAPI,
    *,
    api_key: str,
    make_client: Any,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
) -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    errors_logger = logging.getLogger("payment_service.api.errors")
    errors_logger.addHandler(handler)
    fake_service.failure = RuntimeError("boom-SENTINEL")
    try:
        async with make_client(app, headers={"X-API-Key": api_key}) as client:
            await client.post(
                PAYMENTS_URL, json=payment_body, headers={**idempotency_headers, "X-Request-ID": "crash-2"}
            )
    finally:
        errors_logger.removeHandler(handler)

    (line,) = stream.getvalue().splitlines()
    record = json.loads(line)
    assert record["level"] == "ERROR"
    assert record["request_id"] == "crash-2"
    assert "RuntimeError: boom-SENTINEL" in record["exception"]
    assert "Traceback" in record["exception"]


async def test_unknown_route_is_a_404_problem(client: httpx.AsyncClient) -> None:
    response = await client.get("/no/such/route", headers={"X-Request-ID": "lost-1"})

    assert response.status_code == 404
    assert response.headers["content-type"] == PROBLEM_JSON
    assert response.json() == {
        "type": "https://payment-service/errors/not-found",
        "title": "Not Found",
        "status": 404,
        "detail": "Not Found",
        "code": "not-found",
        "request_id": "lost-1",
    }


async def test_wrong_method_is_a_405_problem_that_keeps_the_allow_header(client: httpx.AsyncClient) -> None:
    response = await client.delete(PAYMENTS_URL)

    assert response.status_code == 405
    assert response.headers["content-type"] == PROBLEM_JSON
    assert response.json()["code"] == "method-not-allowed"
    assert "POST" in response.headers["allow"]


async def test_app_errors_are_rendered_generically_from_their_class_attributes(
    client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
) -> None:
    fake_service.failure = TeapotError("short and stout")

    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.status_code == 418
    assert response.headers["content-type"] == PROBLEM_JSON
    body = response.json()
    assert set(body) == PROBLEM_KEYS
    assert body["type"] == "https://payment-service/errors/teapot"
    assert body["title"] == "I am a teapot"
    assert body["detail"] == "short and stout"
    assert body["code"] == "teapot"


async def test_app_error_without_detail_falls_back_to_its_title(
    client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    idempotency_headers: dict[str, str],
) -> None:
    fake_service.failure = TeapotError()

    response = await client.post(PAYMENTS_URL, json=payment_body, headers=idempotency_headers)

    assert response.json()["detail"] == "I am a teapot"


async def test_every_error_response_carries_the_request_id_header(
    client: httpx.AsyncClient, anonymous_client: httpx.AsyncClient
) -> None:
    responses = [
        await anonymous_client.get(f"{PAYMENTS_URL}/00000000-0000-0000-0000-000000000000"),  # 401
        await client.get("/missing"),  # 404
        await client.get(f"{PAYMENTS_URL}/nope"),  # 422
        await client.get(f"{PAYMENTS_URL}/00000000-0000-0000-0000-000000000000"),  # 404 domain
        await client.delete(PAYMENTS_URL),  # 405
    ]

    for response in responses:
        assert response.status_code >= 400
        assert response.json()["request_id"] == response.headers["x-request-id"]


async def test_disabled_docs_answer_with_a_problem_not_with_html(
    make_settings: Any, make_client: Any, fake_service: Any, fake_database: Any
) -> None:
    settings = make_settings(api={"docs_enabled": False})
    app = create_app(settings, payment_service=fake_service, database=fake_database)

    async with make_client(app) as client:
        responses = [await client.get(url) for url in ("/docs", "/redoc", "/openapi.json")]

    for response in responses:
        assert response.status_code == 404
        assert response.headers["content-type"] == PROBLEM_JSON

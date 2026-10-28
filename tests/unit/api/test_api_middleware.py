from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

import pytest
from fastapi import FastAPI, Request, Response

from payment_service.api.middleware import RequestContextMiddleware
from payment_service.logging_config import get_correlation_id

if TYPE_CHECKING:
    from collections.abc import Callable

    import httpx
    from starlette.types import Message, Receive, Scope, Send

HEX_ID = re.compile(r"[0-9a-f]{32}")
ACCESS_LOGGER = "payment_service.access"


@pytest.fixture
def probe_app(app: FastAPI) -> FastAPI:
    """The real app plus a route that reports what the middleware set up for the handler."""

    @app.get("/_probe")
    async def probe(request: Request) -> dict[str, Any]:
        return {"state": request.state.request_id, "context": get_correlation_id()}

    @app.get("/_probe/own-header")
    async def own_header() -> Response:
        return Response("x", headers={"X-Request-ID": "set-by-handler"})

    return app


async def test_a_request_id_is_generated_when_the_client_sends_none(client: httpx.AsyncClient) -> None:
    first = await client.get("/health/live")
    second = await client.get("/health/live")

    assert HEX_ID.fullmatch(first.headers["x-request-id"])
    assert first.headers["x-request-id"] != second.headers["x-request-id"]


@pytest.mark.parametrize(
    "request_id",
    ["abc", "req-123.abc_DEF", "A" * 64, "0", "a.b-c_d"],
)
async def test_a_well_formed_request_id_is_kept(client: httpx.AsyncClient, request_id: str) -> None:
    response = await client.get("/health/live", headers={"X-Request-ID": request_id})

    assert response.headers["x-request-id"] == request_id


@pytest.mark.parametrize(
    "request_id",
    [
        pytest.param("", id="empty"),
        pytest.param("A" * 65, id="too-long"),
        pytest.param("has space", id="space"),
        pytest.param("slash/slash", id="slash"),
        pytest.param("semi;colon", id="semicolon"),
        pytest.param('quote"', id="quote"),
        pytest.param("<script>", id="markup"),
        pytest.param("ключ".encode(), id="non-ascii"),
        pytest.param(b"abc\r\nX-Injected: yes", id="header-injection"),
        pytest.param(b"abc\n", id="trailing-newline"),
    ],
)
async def test_a_malformed_request_id_is_replaced(client: httpx.AsyncClient, request_id: str | bytes) -> None:
    response = await client.get("/health/live", headers={"X-Request-ID": request_id})  # type: ignore[dict-item]

    assert HEX_ID.fullmatch(response.headers["x-request-id"])
    assert "x-injected" not in response.headers


async def test_the_request_id_reaches_handlers_through_state_and_the_logging_context(
    probe_app: FastAPI, api_key: str, make_client: Callable[..., httpx.AsyncClient]
) -> None:
    async with make_client(probe_app) as client:
        response = await client.get("/_probe", headers={"X-Request-ID": "ctx-1"})

    assert response.json() == {"state": "ctx-1", "context": "ctx-1"}


async def test_the_logging_context_is_cleared_after_the_request(client: httpx.AsyncClient) -> None:
    await client.get("/health/live", headers={"X-Request-ID": "ctx-2"})

    assert get_correlation_id() is None


async def test_the_logging_context_is_cleared_after_a_crash(
    client: httpx.AsyncClient, fake_service: Any, payment_body: dict[str, Any]
) -> None:
    fake_service.failure = RuntimeError("boom")

    await client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "k", "X-Request-ID": "ctx-3"}
    )

    assert get_correlation_id() is None


async def test_a_header_set_by_the_handler_is_overwritten_not_duplicated(
    probe_app: FastAPI, make_client: Callable[..., httpx.AsyncClient]
) -> None:
    async with make_client(probe_app) as client:
        response = await client.get("/_probe/own-header", headers={"X-Request-ID": "mine-1"})

    assert response.headers.get_list("x-request-id") == ["mine-1"]


async def test_one_access_log_line_is_written_per_request(
    client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=ACCESS_LOGGER)

    await client.get("/health/live")
    await client.get("/missing")

    records = [r for r in caplog.records if r.name == ACCESS_LOGGER]
    assert [(r.method, r.path, r.status) for r in records] == [
        ("GET", "/health/live", 200),
        ("GET", "/missing", 404),
    ]
    assert all(isinstance(r.duration_ms, float) and r.duration_ms >= 0 for r in records)
    assert records[0].getMessage().startswith("GET /health/live -> 200 in ")


async def test_the_access_log_never_contains_the_query_string(
    client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=ACCESS_LOGGER)

    await client.get("/health/live?token=QUERY-SENTINEL")

    assert "QUERY-SENTINEL" not in caplog.text
    (record,) = [r for r in caplog.records if r.name == ACCESS_LOGGER]
    assert record.path == "/health/live"


async def test_the_access_log_neutralises_control_characters_in_the_path(
    client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=ACCESS_LOGGER)

    await client.get("/line%0Abreak%1B[31m")

    (record,) = [r for r in caplog.records if r.name == ACCESS_LOGGER]
    assert "\n" not in record.getMessage()
    assert "\x1b" not in record.getMessage()
    assert record.path == "/line?break?[31m"


async def test_the_access_log_records_a_crash_as_500(
    client: httpx.AsyncClient,
    fake_service: Any,
    payment_body: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=ACCESS_LOGGER)
    fake_service.failure = RuntimeError("boom")

    await client.post("/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "k"})

    (record,) = [r for r in caplog.records if r.name == ACCESS_LOGGER]
    assert (record.method, record.status) == ("POST", 500)


async def test_non_http_scopes_pass_through_untouched() -> None:
    seen: list[Scope] = []

    async def inner(scope: Scope, receive: Receive, send: Send) -> None:
        seen.append(scope)

    middleware = RequestContextMiddleware(inner)
    lifespan: Scope = {"type": "lifespan"}
    websocket: Scope = {"type": "websocket", "headers": [(b"x-request-id", b"abc")]}

    async def receive() -> Message:
        return {"type": "lifespan.startup"}

    async def send(message: Message) -> None:
        return None

    await middleware(lifespan, receive, send)
    await middleware(websocket, receive, send)

    assert seen == [{"type": "lifespan"}, {"type": "websocket", "headers": [(b"x-request-id", b"abc")]}]

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import pytest

from payment_service.api.app import create_app
from payment_service.api.middleware import MaxBodySizeMiddleware

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    import httpx
    from starlette.types import Message, Receive, Scope, Send

    from payment_service.config import Settings

PAYMENTS_URL = "/api/v1/payments"
LIMIT = 1024
JSON_HEADERS = {"Idempotency-Key": "key-0001", "Content-Type": "application/json"}


def padded(body: dict[str, Any], size: int) -> bytes:
    """``body`` as JSON followed by insignificant whitespace, ``size`` bytes in total."""
    raw = json.dumps(body, separators=(",", ":")).encode()
    assert len(raw) <= size
    return raw + b" " * (size - len(raw))


async def chunked(data: bytes, piece: int = 300) -> AsyncIterator[bytes]:
    for start in range(0, len(data), piece):
        yield data[start : start + piece]


@pytest.fixture
def limited_client(
    make_settings: Callable[..., Settings],
    fake_service: Any,
    fake_database: Any,
    make_client: Callable[..., httpx.AsyncClient],
) -> httpx.AsyncClient:
    settings = make_settings(api={"max_body_bytes": LIMIT})
    app = create_app(settings, payment_service=fake_service, database=fake_database)
    return make_client(app, headers={"X-API-Key": settings.api.key.get_secret_value()})


async def test_a_declared_length_over_the_limit_is_rejected_with_413(
    limited_client: httpx.AsyncClient, fake_service: Any, payment_body: dict[str, Any]
) -> None:
    async with limited_client as client:
        response = await client.post(
            PAYMENTS_URL,
            content=padded(payment_body, LIMIT + 1),
            headers={**JSON_HEADERS, "X-Request-ID": "big-1"},
        )

    assert response.status_code == 413
    assert response.headers["content-type"] == "application/problem+json"
    assert response.headers["x-request-id"] == "big-1"
    assert response.json() == {
        "type": "https://payment-service/errors/payload-too-large",
        "title": "Request body is too large",
        "status": 413,
        "detail": f"The request body must not exceed {LIMIT} bytes.",
        "code": "payload-too-large",
        "request_id": "big-1",
    }
    assert fake_service.calls == []


async def test_a_streamed_body_over_the_limit_is_rejected_with_413(
    limited_client: httpx.AsyncClient, fake_service: Any, payment_body: dict[str, Any]
) -> None:
    async with limited_client as client:
        request = client.build_request(
            "POST", PAYMENTS_URL, content=chunked(padded(payment_body, LIMIT + 1)), headers=JSON_HEADERS
        )
        assert "content-length" not in request.headers  # really chunked, nothing declared
        assert request.headers["transfer-encoding"] == "chunked"

        response = await client.send(request)

    assert response.status_code == 413
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["code"] == "payload-too-large"
    assert response.json()["request_id"] == response.headers["x-request-id"]
    assert fake_service.calls == []


async def test_a_body_of_exactly_the_limit_passes_with_a_declared_length(
    limited_client: httpx.AsyncClient, fake_service: Any, payment_body: dict[str, Any]
) -> None:
    async with limited_client as client:
        response = await client.post(PAYMENTS_URL, content=padded(payment_body, LIMIT), headers=JSON_HEADERS)

    assert response.status_code == 202, response.text
    assert len(fake_service.calls) == 1


async def test_a_streamed_body_of_exactly_the_limit_passes(
    limited_client: httpx.AsyncClient, fake_service: Any, payment_body: dict[str, Any]
) -> None:
    async with limited_client as client:
        response = await client.post(
            PAYMENTS_URL, content=chunked(padded(payment_body, LIMIT)), headers=JSON_HEADERS
        )

    assert response.status_code == 202, response.text
    assert fake_service.calls[0].command.description == payment_body["description"]


async def test_one_byte_decides_between_accepted_and_rejected_for_both_framings(
    limited_client: httpx.AsyncClient, payment_body: dict[str, Any]
) -> None:
    sizes = (LIMIT, LIMIT + 1)
    async with limited_client as client:
        declared = [
            (
                await client.post(PAYMENTS_URL, content=padded(payment_body, size), headers=JSON_HEADERS)
            ).status_code
            for size in sizes
        ]
        streamed = [
            (
                await client.post(
                    PAYMENTS_URL, content=chunked(padded(payment_body, size)), headers=JSON_HEADERS
                )
            ).status_code
            for size in sizes
        ]

    assert (declared, streamed) == ([202, 413], [202, 413])


async def test_the_default_limit_is_64_kib(
    app: Any, api_key: str, make_client: Callable[..., httpx.AsyncClient], payment_body: dict[str, Any]
) -> None:
    async with make_client(app, headers={"X-API-Key": api_key}) as client:
        at_limit = await client.post(PAYMENTS_URL, content=padded(payment_body, 65536), headers=JSON_HEADERS)
        over = await client.post(PAYMENTS_URL, content=padded(payment_body, 65537), headers=JSON_HEADERS)

    assert (at_limit.status_code, over.status_code) == (202, 413)


async def test_the_size_limit_applies_before_authentication(
    make_settings: Callable[..., Settings],
    fake_service: Any,
    fake_database: Any,
    make_client: Callable[..., httpx.AsyncClient],
    payment_body: dict[str, Any],
) -> None:
    app = create_app(
        make_settings(api={"max_body_bytes": LIMIT}), payment_service=fake_service, database=fake_database
    )
    async with make_client(app) as anonymous:
        response = await anonymous.post(
            PAYMENTS_URL, content=padded(payment_body, LIMIT + 1), headers=JSON_HEADERS
        )

    assert response.status_code == 413


async def test_the_413_is_written_to_the_access_log(
    limited_client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture, payment_body: dict[str, Any]
) -> None:
    caplog.set_level(logging.INFO, logger="payment_service.access")

    async with limited_client as client:
        await client.post(PAYMENTS_URL, content=padded(payment_body, LIMIT + 1), headers=JSON_HEADERS)

    (record,) = [r for r in caplog.records if r.name == "payment_service.access"]
    assert (record.method, record.status) == ("POST", 413)


# --- the middleware on its own ------------------------------------------------------------------------------


class Channel:
    """Scripted ASGI receive/send pair that records what the middleware does with them."""

    def __init__(self, *incoming: Message) -> None:
        self.incoming = list(incoming)
        self.receive_calls = 0
        self.sent: list[Message] = []

    async def receive(self) -> Message:
        self.receive_calls += 1
        if self.incoming:
            return self.incoming.pop(0)
        return {"type": "http.disconnect"}

    async def send(self, message: Message) -> None:
        self.sent.append(message)


def request_message(body: bytes, *, more: bool) -> Message:
    return {"type": "http.request", "body": body, "more_body": more}


def http_scope(*headers: tuple[bytes, bytes]) -> Scope:
    return {"type": "http", "method": "POST", "path": "/", "headers": list(headers), "state": {}}


class RecordingApp:
    """Inner application: reads the whole body, then answers 204."""

    def __init__(self) -> None:
        self.calls = 0
        self.bodies: list[bytes] = []
        self.after_body: list[Message] = []

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        self.calls += 1
        body = b""
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body", False):
                break
        self.bodies.append(body)
        self.after_body.append(await receive())
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})


async def test_a_declared_oversized_body_is_refused_without_reading_it() -> None:
    app = RecordingApp()
    channel = Channel(request_message(b"x" * 10, more=False))

    await MaxBodySizeMiddleware(app, max_body_bytes=5)(
        http_scope((b"content-length", b"6")), channel.receive, channel.send
    )

    assert app.calls == 0
    assert channel.receive_calls == 0
    assert [m["type"] for m in channel.sent] == ["http.response.start", "http.response.body"]
    assert channel.sent[0]["status"] == 413


async def test_a_streamed_oversized_body_stops_being_read_and_is_answered_once() -> None:
    app = RecordingApp()
    channel = Channel(
        request_message(b"abc", more=True),
        request_message(b"def", more=True),
        request_message(b"ghi", more=True),
        request_message(b"never read", more=False),
    )

    await MaxBodySizeMiddleware(app, max_body_bytes=5)(http_scope(), channel.receive, channel.send)

    assert app.calls == 0
    assert channel.receive_calls == 2  # the second chunk crossed the limit; nothing was read after it
    assert [m["type"] for m in channel.sent] == ["http.response.start", "http.response.body"]
    assert channel.sent[0]["status"] == 413


async def test_a_body_within_the_limit_is_replayed_to_the_application_in_one_piece() -> None:
    app = RecordingApp()
    channel = Channel(request_message(b"abc", more=True), request_message(b"de", more=False))

    await MaxBodySizeMiddleware(app, max_body_bytes=5)(http_scope(), channel.receive, channel.send)

    assert app.bodies == [b"abcde"]
    assert app.after_body == [{"type": "http.disconnect"}]  # later reads reach the real channel
    assert channel.sent[0]["status"] == 204


async def test_a_disconnect_in_the_middle_of_the_body_reaches_the_application() -> None:
    seen: list[Message] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        seen.append(await receive())

    channel = Channel(request_message(b"abc", more=True), {"type": "http.disconnect"})

    await MaxBodySizeMiddleware(app, max_body_bytes=100)(http_scope(), channel.receive, channel.send)

    assert seen == [{"type": "http.disconnect"}]


@pytest.mark.parametrize("declared", [b"abc", b"-1", b"1e3", b"", b"\xd9\xa1"])
async def test_a_malformed_content_length_does_not_disable_the_limit(declared: bytes) -> None:
    app = RecordingApp()
    channel = Channel(request_message(b"x" * 10, more=False))

    await MaxBodySizeMiddleware(app, max_body_bytes=5)(
        http_scope((b"content-length", declared)), channel.receive, channel.send
    )

    assert app.calls == 0
    assert channel.sent[0]["status"] == 413


async def test_an_absurdly_long_content_length_is_rejected_not_a_crash() -> None:
    app = RecordingApp()
    channel = Channel()

    await MaxBodySizeMiddleware(app, max_body_bytes=5)(
        http_scope((b"content-length", b"9" * 5000)), channel.receive, channel.send
    )

    assert app.calls == 0
    assert channel.sent[0]["status"] == 413


async def test_leading_zeros_in_content_length_are_not_a_bypass() -> None:
    app = RecordingApp()
    channel = Channel()

    await MaxBodySizeMiddleware(app, max_body_bytes=5)(
        http_scope((b"content-length", b"0000000000000000000000006")), channel.receive, channel.send
    )

    assert app.calls == 0
    assert channel.sent[0]["status"] == 413


async def test_non_http_scopes_pass_through() -> None:
    seen: list[Scope] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        seen.append(scope)

    channel = Channel()
    lifespan: Scope = {"type": "lifespan"}

    await MaxBodySizeMiddleware(app, max_body_bytes=5)(lifespan, channel.receive, channel.send)

    assert seen == [lifespan]
    assert channel.receive_calls == 0

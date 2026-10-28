from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from fastapi import FastAPI

from payment_service.config import WebhookSettings
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.services.dto import PaymentDTO
from payment_service.services.webhook import HttpWebhookSender, sign
from payment_service.tools import webhook_sink
from payment_service.tools.webhook_sink import MAX_BODY_BYTES, create_app, create_app_from_env

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

SECRET = "sink-test-secret-0123456789abcdef"
SINK_LOGGER = "payment_service.tools.webhook_sink"


@pytest.fixture
async def make_client() -> AsyncIterator[Callable[..., httpx.AsyncClient]]:
    """Factory of clients talking to a fresh sink app through ASGI (no sockets, no network)."""
    clients: list[httpx.AsyncClient] = []

    def factory(app: FastAPI | None = None, **app_options: Any) -> httpx.AsyncClient:
        transport = httpx.ASGITransport(app=app if app is not None else create_app(**app_options))
        client = httpx.AsyncClient(transport=transport, base_url="http://sink")
        clients.append(client)
        return client

    yield factory
    for client in clients:
        await client.aclose()


@pytest.fixture
def client(make_client: Callable[..., httpx.AsyncClient]) -> httpx.AsyncClient:
    return make_client()


def signed_headers(body: bytes, *, payment_id: str = "p-1", timestamp: int | None = None) -> dict[str, str]:
    ts = int(datetime.now(UTC).timestamp()) if timestamp is None else timestamp
    return {"X-Payment-Id": payment_id, "X-Timestamp": str(ts), "X-Signature": sign(SECRET, ts, body)}


async def received(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    response = await client.get("/received")
    assert response.status_code == 200
    return response.json()


# --------------------------------------------------------------------------- #
# routes
# --------------------------------------------------------------------------- #
async def test_ok_answers_200(client: httpx.AsyncClient) -> None:
    response = await client.post("/ok", json={"event": "payment.processed"}, headers={"X-Payment-Id": "p-1"})

    assert response.status_code == 200
    assert response.json() == {"status": "accepted"}


async def test_ok_answers_200_every_time(client: httpx.AsyncClient) -> None:
    codes = [(await client.post("/ok", headers={"X-Payment-Id": "p-1"})).status_code for _ in range(5)]

    assert codes == [200] * 5


async def test_fail_answers_500_every_time(client: httpx.AsyncClient) -> None:
    codes = [(await client.post("/fail", headers={"X-Payment-Id": "p-1"})).status_code for _ in range(5)]

    assert codes == [500] * 5


async def test_flaky_fails_the_first_two_deliveries_of_a_payment_then_succeeds(
    client: httpx.AsyncClient,
) -> None:
    codes = [(await client.post("/flaky", headers={"X-Payment-Id": "p-1"})).status_code for _ in range(5)]

    assert codes == [500, 500, 200, 200, 200]


async def test_flaky_counts_each_payment_separately(client: httpx.AsyncClient) -> None:
    sequence = [("a", 500), ("b", 500), ("a", 500), ("a", 200), ("b", 500), ("b", 200), ("c", 500)]

    for payment_id, expected in sequence:
        response = await client.post("/flaky", headers={"X-Payment-Id": payment_id})
        assert response.status_code == expected, (payment_id, expected)


@pytest.mark.parametrize(
    ("failures", "expected"), [(0, [200, 200]), (1, [500, 200]), (3, [500, 500, 500, 200])]
)
async def test_flaky_failures_option_controls_how_many_deliveries_fail(
    make_client: Callable[..., httpx.AsyncClient], failures: int, expected: list[int]
) -> None:
    flaky = make_client(flaky_failures=failures)

    codes = [(await flaky.post("/flaky", headers={"X-Payment-Id": "p"})).status_code for _ in expected]

    assert codes == expected


async def test_flaky_attempt_counters_are_independent_per_app(
    make_client: Callable[..., httpx.AsyncClient],
) -> None:
    first, second = make_client(), make_client()

    assert (await first.post("/flaky", headers={"X-Payment-Id": "p"})).status_code == 500
    assert (await second.post("/flaky", headers={"X-Payment-Id": "p"})).status_code == 500


async def test_flaky_counter_is_per_path(client: httpx.AsyncClient) -> None:
    await client.post("/ok", headers={"X-Payment-Id": "p"})
    await client.post("/ok", headers={"X-Payment-Id": "p"})

    assert (await client.post("/flaky", headers={"X-Payment-Id": "p"})).status_code == 500


async def test_deliveries_without_payment_id_are_accepted_and_grouped_under_a_placeholder(
    client: httpx.AsyncClient,
) -> None:
    codes = [(await client.post("/flaky")).status_code for _ in range(3)]

    assert codes == [500, 500, 200]
    assert {entry["payment_id"] for entry in await received(client)} == {"-"}


@pytest.mark.parametrize(
    ("method", "path"),
    [("GET", "/ok"), ("PUT", "/ok"), ("GET", "/fail"), ("GET", "/flaky"), ("POST", "/received")],
)
async def test_routes_only_accept_their_documented_method(
    client: httpx.AsyncClient, method: str, path: str
) -> None:
    response = await client.request(method, path)

    assert response.status_code == 405


async def test_unknown_path_is_404_and_not_recorded(client: httpx.AsyncClient) -> None:
    assert (await client.post("/nope")).status_code == 404
    assert await received(client) == []


async def test_no_interactive_docs_are_exposed(client: httpx.AsyncClient) -> None:
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert (await client.get(path)).status_code == 404


# --------------------------------------------------------------------------- #
# GET /received
# --------------------------------------------------------------------------- #
async def test_received_is_empty_initially(client: httpx.AsyncClient) -> None:
    assert await received(client) == []


async def test_received_lists_deliveries_oldest_first_with_all_fields(client: httpx.AsyncClient) -> None:
    await client.post("/flaky", json={"n": 1}, headers={"X-Payment-Id": "p-1"})
    await client.post("/flaky", json={"n": 2}, headers={"X-Payment-Id": "p-1"})
    await client.post("/flaky", json={"n": 3}, headers={"X-Payment-Id": "p-1"})
    await client.post("/ok", content=b"plain text", headers={"X-Payment-Id": "p-2"})

    entries = await received(client)

    assert entries == [
        {
            "payment_id": "p-1",
            "path": "/flaky",
            "status_code": 500,
            "signature_valid": None,
            "attempt": 1,
            "body": {"n": 1},
        },
        {
            "payment_id": "p-1",
            "path": "/flaky",
            "status_code": 500,
            "signature_valid": None,
            "attempt": 2,
            "body": {"n": 2},
        },
        {
            "payment_id": "p-1",
            "path": "/flaky",
            "status_code": 200,
            "signature_valid": None,
            "attempt": 3,
            "body": {"n": 3},
        },
        {
            "payment_id": "p-2",
            "path": "/ok",
            "status_code": 200,
            "signature_valid": None,
            "attempt": 1,
            "body": "plain text",
        },
    ]


async def test_received_keeps_only_the_last_hundred_deliveries(client: httpx.AsyncClient) -> None:
    for index in range(105):
        await client.post("/ok", json={"index": index}, headers={"X-Payment-Id": f"p-{index}"})

    entries = await received(client)

    assert len(entries) == 100
    assert entries[0]["body"] == {"index": 5}
    assert entries[-1]["body"] == {"index": 104}


async def test_received_decodes_invalid_utf8_bodies_leniently(client: httpx.AsyncClient) -> None:
    await client.post("/ok", content=b"\xff\xfe broken", headers={"X-Payment-Id": "p"})

    (entry,) = await received(client)

    assert isinstance(entry["body"], str)
    assert "broken" in entry["body"]


async def test_received_does_not_expose_request_headers(client: httpx.AsyncClient) -> None:
    await client.post("/ok", json={}, headers={"X-Payment-Id": "p", "Authorization": "Bearer top-secret"})

    assert "top-secret" not in (await client.get("/received")).text


# --------------------------------------------------------------------------- #
# signature verification
# --------------------------------------------------------------------------- #
async def test_signed_delivery_is_accepted_and_marked_valid(
    make_client: Callable[..., httpx.AsyncClient],
) -> None:
    sink = make_client(signing_secret=SECRET)
    body = b'{"payment_id":"p-1"}'

    response = await sink.post("/ok", content=body, headers=signed_headers(body))

    assert response.status_code == 200
    (entry,) = await received(sink)
    assert entry["signature_valid"] is True


@pytest.mark.parametrize("path", ["/ok", "/fail", "/flaky"])
async def test_missing_signature_is_rejected_with_401_on_every_route(
    make_client: Callable[..., httpx.AsyncClient], path: str
) -> None:
    sink = make_client(signing_secret=SECRET)

    response = await sink.post(path, content=b"{}", headers={"X-Payment-Id": "p"})

    assert response.status_code == 401
    (entry,) = await received(sink)
    assert (entry["status_code"], entry["signature_valid"]) == (401, False)


@pytest.mark.parametrize(
    "tamper",
    [
        "body",
        "signature",
        "timestamp-header",
        "wrong-secret",
        "stale",
        "future",
        "no-timestamp",
        "bad-timestamp",
    ],
)
async def test_invalid_signature_is_rejected_with_401(
    make_client: Callable[..., httpx.AsyncClient], tamper: str
) -> None:
    sink = make_client(signing_secret=SECRET)
    now = int(datetime.now(UTC).timestamp())
    body = b'{"payment_id":"p-1"}'
    headers = signed_headers(body, timestamp=now)
    sent_body = body
    if tamper == "body":
        sent_body = b'{"payment_id":"p-2"}'
    elif tamper == "signature":
        headers["X-Signature"] = headers["X-Signature"][:-1] + (
            "0" if headers["X-Signature"][-1] != "0" else "1"
        )
    elif tamper == "timestamp-header":
        headers["X-Timestamp"] = str(now + 1)
    elif tamper == "wrong-secret":
        headers["X-Signature"] = sign("not-the-secret-not-the-secret", now, body)
    elif tamper == "stale":
        headers = signed_headers(body, timestamp=now - 3600)
    elif tamper == "future":
        headers = signed_headers(body, timestamp=now + 3600)
    elif tamper == "no-timestamp":
        del headers["X-Timestamp"]
    elif tamper == "bad-timestamp":
        headers["X-Timestamp"] = "yesterday"

    response = await sink.post("/ok", content=sent_body, headers=headers)

    assert response.status_code == 401
    (entry,) = await received(sink)
    assert entry["signature_valid"] is False


async def test_without_a_secret_signatures_are_not_checked(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/ok", content=b"{}", headers={"X-Signature": "sha256=garbage", "X-Timestamp": "1"}
    )

    assert response.status_code == 200
    (entry,) = await received(client)
    assert entry["signature_valid"] is None


async def test_unauthenticated_flaky_delivery_is_401_not_a_flaky_failure(
    make_client: Callable[..., httpx.AsyncClient],
) -> None:
    sink = make_client(signing_secret=SECRET, flaky_failures=0)

    assert (await sink.post("/flaky", content=b"{}", headers={"X-Payment-Id": "p"})).status_code == 401
    body = b"{}"
    assert (
        await sink.post("/flaky", content=body, headers=signed_headers(body, payment_id="p"))
    ).status_code == 200


async def test_http_webhook_sender_and_sink_agree_on_the_signature_format(
    make_client: Callable[..., httpx.AsyncClient],
) -> None:
    sink = make_client(signing_secret=SECRET)
    settings = WebhookSettings(signing_secret=SECRET, allow_private_networks=True)

    async def resolve(host: str, port: int, /) -> list[str]:
        return ["127.0.0.1"]

    payment = PaymentDTO(
        id=uuid.UUID("3f1f8f0e-6a3b-4c53-9d2e-0a1b2c3d4e5f"),
        amount=Decimal("100.50"),
        currency=Currency.RUB,
        description="Order #42",
        metadata={"order_id": 42},
        status=PaymentStatus.SUCCEEDED,
        idempotency_key="k",
        webhook_url="http://sink.example.com/ok",
        created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        processed_at=datetime(2026, 1, 2, 3, 4, 9, tzinfo=UTC),
        webhook_delivered_at=None,
    )

    await HttpWebhookSender(settings, client=sink, resolver=resolve).send(payment)

    (entry,) = await received(sink)
    assert entry["signature_valid"] is True
    assert entry["payment_id"] == "3f1f8f0e-6a3b-4c53-9d2e-0a1b2c3d4e5f"
    assert entry["body"]["amount"] == "100.50"


# --------------------------------------------------------------------------- #
# body size limit
# --------------------------------------------------------------------------- #
async def test_body_of_exactly_one_mebibyte_is_accepted(client: httpx.AsyncClient) -> None:
    response = await client.post("/ok", content=b"x" * MAX_BODY_BYTES, headers={"X-Payment-Id": "p"})

    assert response.status_code == 200


async def test_body_over_one_mebibyte_is_refused_with_413_without_being_stored(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post("/ok", content=b"x" * (MAX_BODY_BYTES + 1), headers={"X-Payment-Id": "p"})

    assert response.status_code == 413
    (entry,) = await received(client)
    assert (entry["status_code"], entry["body"]) == (413, None)


async def test_streamed_body_over_one_mebibyte_is_refused_even_without_content_length(
    client: httpx.AsyncClient,
) -> None:
    async def chunks() -> AsyncIterator[bytes]:
        for _ in range(MAX_BODY_BYTES // 65536 + 2):
            yield b"x" * 65536

    request = client.build_request("POST", "/ok", content=chunks(), headers={"X-Payment-Id": "p"})
    assert "content-length" not in request.headers

    response = await client.send(request)

    assert response.status_code == 413


async def test_oversized_signed_body_is_not_verified(make_client: Callable[..., httpx.AsyncClient]) -> None:
    sink = make_client(signing_secret=SECRET)
    body = b"x" * (MAX_BODY_BYTES + 1)

    response = await sink.post("/ok", content=body, headers=signed_headers(body))

    assert response.status_code == 413
    (entry,) = await received(sink)
    assert entry["signature_valid"] is None


# --------------------------------------------------------------------------- #
# logging and hardening
# --------------------------------------------------------------------------- #
async def test_each_delivery_is_logged_without_print(
    client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    caplog.set_level(logging.INFO, logger=SINK_LOGGER)

    await client.post("/flaky", json={}, headers={"X-Payment-Id": "p-77"})

    messages = [r.getMessage() for r in caplog.records if r.name == SINK_LOGGER]
    assert len(messages) == 1
    assert "path=/flaky" in messages[0]
    assert "payment_id=p-77" in messages[0]
    assert "attempt=1" in messages[0]
    assert "status=500" in messages[0]
    assert capsys.readouterr().out == ""


async def test_control_characters_in_payment_id_are_neutralised(
    client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=SINK_LOGGER)

    await client.post("/ok", headers={"X-Payment-Id": "p\x1b[31m\x07-1"})

    (entry,) = await received(client)
    assert entry["payment_id"] == "p?[31m?-1"
    assert "\x1b" not in caplog.text


async def test_overlong_payment_id_is_truncated(client: httpx.AsyncClient) -> None:
    await client.post("/ok", headers={"X-Payment-Id": "x" * 500})

    (entry,) = await received(client)
    assert entry["payment_id"] == "x" * 128


async def test_attempt_counters_are_bounded(
    monkeypatch: pytest.MonkeyPatch, client: httpx.AsyncClient
) -> None:
    monkeypatch.setattr(webhook_sink, "_MAX_TRACKED_ATTEMPT_KEYS", 3)

    for payment_id in ("a", "b", "c", "d"):  # "a" is evicted when "d" arrives
        await client.post("/flaky", headers={"X-Payment-Id": payment_id})
    response = await client.post("/flaky", headers={"X-Payment-Id": "a"})

    assert response.status_code == 500  # its counter started over


# --------------------------------------------------------------------------- #
# configuration and entry point
# --------------------------------------------------------------------------- #
def test_create_app_rejects_negative_flaky_failures() -> None:
    with pytest.raises(ValueError, match="flaky_failures"):
        create_app(flaky_failures=-1)


async def test_create_app_from_env_reads_secret_and_failure_count(
    monkeypatch: pytest.MonkeyPatch, make_client: Callable[..., httpx.AsyncClient]
) -> None:
    monkeypatch.setenv("SINK_SIGNING_SECRET", SECRET)
    monkeypatch.setenv("SINK_FLAKY_FAILURES", "1")
    sink = make_client(create_app_from_env())
    body = b"{}"

    first = await sink.post("/flaky", content=body, headers=signed_headers(body))
    second = await sink.post("/flaky", content=body, headers=signed_headers(body))
    unsigned = await sink.post("/ok", content=body)

    assert (first.status_code, second.status_code, unsigned.status_code) == (500, 200, 401)


async def test_create_app_from_env_defaults_to_two_failures_and_no_verification(
    monkeypatch: pytest.MonkeyPatch, make_client: Callable[..., httpx.AsyncClient]
) -> None:
    monkeypatch.delenv("SINK_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("SINK_FLAKY_FAILURES", raising=False)
    sink = make_client(create_app_from_env())

    codes = [(await sink.post("/flaky", headers={"X-Payment-Id": "p"})).status_code for _ in range(3)]

    assert codes == [500, 500, 200]


async def test_create_app_from_env_treats_an_empty_secret_as_unset(
    monkeypatch: pytest.MonkeyPatch, make_client: Callable[..., httpx.AsyncClient]
) -> None:
    monkeypatch.setenv("SINK_SIGNING_SECRET", "")
    sink = make_client(create_app_from_env())

    assert (await sink.post("/ok")).status_code == 200


@pytest.mark.parametrize("value", ["abc", "1.5", ""])
def test_create_app_from_env_rejects_a_non_integer_failure_count(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("SINK_FLAKY_FAILURES", value)

    with pytest.raises(ValueError, match="SINK_FLAKY_FAILURES"):
        create_app_from_env()


def test_module_exposes_an_asgi_app() -> None:
    assert isinstance(webhook_sink.app, FastAPI)


def test_main_runs_uvicorn_on_port_9000(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    monkeypatch.setattr(webhook_sink.uvicorn, "run", lambda *args, **kwargs: calls.append((args, kwargs)))
    monkeypatch.setattr(logging, "basicConfig", lambda **_kwargs: None)

    webhook_sink.main()

    assert calls == [((webhook_sink.app,), {"host": "0.0.0.0", "port": 9000})]  # noqa: S104

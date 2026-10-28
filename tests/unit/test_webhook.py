from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import shutil
import ssl
import subprocess
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import respx

from payment_service.common.time import utcnow
from payment_service.config import WebhookSettings
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.exceptions import WebhookDeliveryError
from payment_service.services.dto import PaymentDTO
from payment_service.services.webhook import (
    EVENT_TYPE,
    HttpWebhookSender,
    serialize_payment_event,
    sign,
    verify_signature,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator
    from pathlib import Path

SECRET = "unit-test-signing-secret-5f3a9c71"
PUBLIC_IP = "93.184.216.34"
PUBLIC_IPV6 = "2606:4700:4700::1111"
SENDER_LOGGER = "payment_service.services.webhook"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def make_settings(**overrides: Any) -> WebhookSettings:
    values: dict[str, Any] = {
        "signing_secret": SECRET,
        "timeout_seconds": 5.0,
        "allow_private_networks": False,
        "user_agent": "payment-service-webhook/1.0",
    }
    values.update(overrides)
    return WebhookSettings(**values)


def make_payment(**overrides: Any) -> PaymentDTO:
    payment = PaymentDTO(
        id=uuid.UUID("3f1f8f0e-6a3b-4c53-9d2e-0a1b2c3d4e5f"),
        amount=Decimal("100.5"),
        currency=Currency.RUB,
        description="Order #42",
        metadata={"order_id": 42, "tags": ["a", "b"]},
        status=PaymentStatus.SUCCEEDED,
        idempotency_key="idem-1",
        webhook_url="https://hooks.example.com/payments",
        created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        processed_at=datetime(2026, 1, 2, 3, 4, 9, tzinfo=UTC),
        webhook_delivered_at=None,
    )
    return replace(payment, **overrides)


class StaticResolver:
    """Resolver returning fixed addresses and recording what it was asked."""

    def __init__(self, *addresses: str) -> None:
        self.addresses = list(addresses)
        self.calls: list[tuple[str, int]] = []

    async def __call__(self, host: str, port: int, /) -> list[str]:
        self.calls.append((host, port))
        return self.addresses


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as mock:
        yield mock


@pytest.fixture(scope="module")
async def http_client() -> AsyncIterator[httpx.AsyncClient]:
    """One client for the module: building an SSL context (CA bundle) costs ~0.5 s per client."""
    async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
        yield client


@pytest.fixture
def resolver() -> StaticResolver:
    return StaticResolver(PUBLIC_IP)


@pytest.fixture
def make_sender(http_client: httpx.AsyncClient, resolver: StaticResolver) -> Callable[..., HttpWebhookSender]:
    """Factory of senders sharing ``http_client``; ``aclose`` is a no-op for an injected client."""

    def factory(use_resolver: StaticResolver | None = None, **settings: Any) -> HttpWebhookSender:
        return HttpWebhookSender(
            make_settings(**settings),
            client=http_client,
            resolver=resolver if use_resolver is None else use_resolver,
        )

    return factory


@pytest.fixture
def sender(make_sender: Callable[..., HttpWebhookSender]) -> HttpWebhookSender:
    return make_sender()


# --------------------------------------------------------------------------- #
# sign / verify_signature
# --------------------------------------------------------------------------- #
def test_sign_is_hmac_sha256_over_timestamp_dot_body_with_prefix() -> None:
    body = b'{"a":1}'

    expected = hmac.new(SECRET.encode(), b"1700000000." + body, hashlib.sha256).hexdigest()

    assert sign(SECRET, 1700000000, body) == f"sha256={expected}"


def test_sign_is_deterministic_and_depends_on_every_input() -> None:
    base = sign(SECRET, 1700000000, b"body")

    assert sign(SECRET, 1700000000, b"body") == base
    assert sign(SECRET, 1700000001, b"body") != base
    assert sign(SECRET, 1700000000, b"body2") != base
    assert sign(SECRET + "x", 1700000000, b"body") != base


def test_verify_signature_accepts_roundtrip() -> None:
    signature = sign(SECRET, 1700000000, b"payload")

    assert verify_signature(SECRET, 1700000000, b"payload", signature, now=1700000000)


def test_verify_signature_uses_the_real_clock_by_default() -> None:
    timestamp = int(utcnow().timestamp())

    assert verify_signature(SECRET, timestamp, b"x", sign(SECRET, timestamp, b"x"))
    assert not verify_signature(SECRET, timestamp - 3600, b"x", sign(SECRET, timestamp - 3600, b"x"))


def test_verify_signature_rejects_tampered_body() -> None:
    signature = sign(SECRET, 1700000000, b"payload")

    assert not verify_signature(SECRET, 1700000000, b"payload!", signature, now=1700000000)


def test_verify_signature_rejects_tampered_timestamp() -> None:
    signature = sign(SECRET, 1700000000, b"payload")

    assert not verify_signature(SECRET, 1700000001, b"payload", signature, now=1700000001)


def test_verify_signature_rejects_wrong_secret() -> None:
    signature = sign("another-secret-another-secret", 1700000000, b"payload")

    assert not verify_signature(SECRET, 1700000000, b"payload", signature, now=1700000000)


@pytest.mark.parametrize(
    "mangle", [lambda s: s.removeprefix("sha256="), lambda s: s.upper(), lambda s: s[:-1], str]
)
def test_verify_signature_rejects_malformed_signatures(mangle: Callable[[str], str]) -> None:
    signature = sign(SECRET, 1700000000, b"payload")
    mangled = mangle(signature) if mangle is not str else ""

    assert not verify_signature(SECRET, 1700000000, b"payload", mangled, now=1700000000)


def test_verify_signature_rejects_non_ascii_signature_without_raising() -> None:
    assert not verify_signature(SECRET, 1700000000, b"payload", "sha256=éé", now=1700000000)


def test_verify_signature_rejects_stale_timestamp() -> None:
    signature = sign(SECRET, 1700000000, b"payload")

    assert not verify_signature(SECRET, 1700000000, b"payload", signature, now=1700000301)


def test_verify_signature_rejects_timestamp_from_the_future() -> None:
    signature = sign(SECRET, 1700000301, b"payload")

    assert not verify_signature(SECRET, 1700000301, b"payload", signature, now=1700000000)


def test_verify_signature_accepts_timestamp_exactly_at_the_tolerance_boundary() -> None:
    signature = sign(SECRET, 1700000000, b"payload")

    assert verify_signature(SECRET, 1700000000, b"payload", signature, now=1700000300)
    assert not verify_signature(
        SECRET, 1700000000, b"payload", signature, now=1700000300, tolerance_seconds=299
    )


def test_verify_signature_honours_custom_tolerance() -> None:
    signature = sign(SECRET, 1700000000, b"payload")

    assert verify_signature(SECRET, 1700000000, b"payload", signature, now=1700003600, tolerance_seconds=3600)


# --------------------------------------------------------------------------- #
# serialize_payment_event
# --------------------------------------------------------------------------- #
def test_serialize_payment_event_has_the_documented_fields() -> None:
    event = json.loads(serialize_payment_event(make_payment()))

    assert event == {
        "event": EVENT_TYPE,
        "payment_id": "3f1f8f0e-6a3b-4c53-9d2e-0a1b2c3d4e5f",
        "status": "succeeded",
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42, "tags": ["a", "b"]},
        "created_at": "2026-01-02T03:04:05+00:00",
        "processed_at": "2026-01-02T03:04:09+00:00",
    }
    assert EVENT_TYPE == "payment.processed"


def test_serialize_payment_event_is_compact_and_key_sorted() -> None:
    body = serialize_payment_event(make_payment(metadata={"b": 1, "a": {"d": 2, "c": 3}}))

    assert b": " not in body
    assert b", " not in body
    assert body.startswith(b'{"amount":"100.50","created_at":')
    assert b'"metadata":{"a":{"c":3,"d":2},"b":1}' in body


def test_serialize_payment_event_is_independent_of_metadata_insertion_order() -> None:
    first = serialize_payment_event(make_payment(metadata={"a": 1, "b": 2}))
    second = serialize_payment_event(make_payment(metadata={"b": 2, "a": 1}))

    assert first == second


@pytest.mark.parametrize(
    ("amount", "rendered"),
    [
        (Decimal(100), "100.00"),
        (Decimal("0.1"), "0.10"),
        (Decimal("100.50"), "100.50"),
        (Decimal("1234567890123456.78"), "1234567890123456.78"),
    ],
)
def test_serialize_payment_event_renders_amount_as_two_decimal_string(amount: Decimal, rendered: str) -> None:
    event = json.loads(serialize_payment_event(make_payment(amount=amount)))

    assert event["amount"] == rendered
    assert isinstance(event["amount"], str)


def test_serialize_payment_event_uses_null_for_missing_processed_at() -> None:
    event = json.loads(serialize_payment_event(make_payment(processed_at=None)))

    assert event["processed_at"] is None


def test_serialize_payment_event_is_pure_ascii_and_roundtrips_unicode() -> None:
    body = serialize_payment_event(make_payment(description="Заказ ☃"))

    assert body.isascii()
    assert json.loads(body)["description"] == "Заказ ☃"


# --------------------------------------------------------------------------- #
# HttpWebhookSender - success
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("status", [200, 201, 202, 204])
async def test_send_returns_none_on_2xx(
    sender: HttpWebhookSender, router: respx.MockRouter, status: int
) -> None:
    route = router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(status))

    assert await sender.send(make_payment()) is None
    assert route.call_count == 1


async def test_send_posts_documented_headers(sender: HttpWebhookSender, router: respx.MockRouter) -> None:
    route = router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(204))
    payment = make_payment()

    before = int(utcnow().timestamp())
    await sender.send(payment)
    after = int(utcnow().timestamp())

    request = route.calls.last.request
    assert request.method == "POST"
    assert request.headers["content-type"] == "application/json"
    assert request.headers["user-agent"] == "payment-service-webhook/1.0"
    assert request.headers["x-payment-id"] == str(payment.id)
    assert request.headers["x-timestamp"].isdigit()
    assert before <= int(request.headers["x-timestamp"]) <= after
    assert request.headers["x-signature"].startswith("sha256=")
    assert request.headers["connection"] == "close"
    unexpected = set(request.headers) - {
        "accept",
        "accept-encoding",
        "connection",
        "content-length",
        "content-type",
        "host",
        "user-agent",
        "x-payment-id",
        "x-signature",
        "x-timestamp",
    }
    assert not unexpected


async def test_send_signs_the_exact_body_bytes(sender: HttpWebhookSender, router: respx.MockRouter) -> None:
    route = router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(200))

    await sender.send(make_payment())

    request = route.calls.last.request
    timestamp = int(request.headers["x-timestamp"])
    assert request.content == serialize_payment_event(make_payment())
    assert verify_signature(SECRET, timestamp, request.content, request.headers["x-signature"])
    assert request.headers["x-signature"] == sign(SECRET, timestamp, request.content)
    assert request.headers["content-length"] == str(len(request.content))


async def test_send_body_has_event_fields_and_string_amount(
    sender: HttpWebhookSender, router: respx.MockRouter
) -> None:
    route = router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(200))

    await sender.send(make_payment(status=PaymentStatus.FAILED, amount=Decimal(7)))

    body = json.loads(route.calls.last.request.content)
    assert body["event"] == "payment.processed"
    assert body["status"] == "failed"
    assert body["amount"] == "7.00"
    assert body["payment_id"] == "3f1f8f0e-6a3b-4c53-9d2e-0a1b2c3d4e5f"
    assert body["currency"] == "RUB"
    assert body["metadata"] == {"order_id": 42, "tags": ["a", "b"]}


async def test_send_uses_configured_user_agent(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter
) -> None:
    route = router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(200))
    custom = make_sender(user_agent="acme-hooks/9")

    await custom.send(make_payment())

    assert route.calls.last.request.headers["user-agent"] == "acme-hooks/9"


# --------------------------------------------------------------------------- #
# HttpWebhookSender - connecting to the vetted address
# --------------------------------------------------------------------------- #
async def test_send_connects_to_the_resolved_ip_and_keeps_the_host_name_in_host_header(
    sender: HttpWebhookSender, router: respx.MockRouter, resolver: StaticResolver
) -> None:
    route = router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(200))

    await sender.send(make_payment(webhook_url="http://hooks.example.com:8080/a/b?x=1&y=2"))

    request = route.calls.last.request
    assert str(request.url) == f"http://{PUBLIC_IP}:8080/a/b?x=1&y=2"
    assert request.headers["host"] == "hooks.example.com:8080"
    assert resolver.calls == [("hooks.example.com", 8080)]
    assert "sni_hostname" not in request.extensions


async def test_send_https_passes_original_host_name_as_sni_hostname(
    sender: HttpWebhookSender, router: respx.MockRouter
) -> None:
    route = router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(200))

    await sender.send(make_payment(webhook_url="https://Hooks.Example.com/payments"))

    request = route.calls.last.request
    assert str(request.url) == f"https://{PUBLIC_IP}/payments"
    assert request.headers["host"] == "hooks.example.com"
    assert request.extensions["sni_hostname"] == "hooks.example.com"


async def test_send_brackets_ipv6_addresses(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter
) -> None:
    v6_sender = make_sender(StaticResolver(PUBLIC_IPV6))
    route = router.route(host=PUBLIC_IPV6).mock(return_value=httpx.Response(200))

    await v6_sender.send(make_payment(webhook_url="https://hooks.example.com:8443/p"))

    request = route.calls.last.request
    assert str(request.url) == f"https://[{PUBLIC_IPV6}]:8443/p"
    assert request.headers["host"] == "hooks.example.com:8443"


async def test_send_to_an_ip_literal_url_resolves_and_vets_it_too(
    sender: HttpWebhookSender, router: respx.MockRouter, resolver: StaticResolver
) -> None:
    route = router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(200))

    await sender.send(make_payment(webhook_url=f"https://{PUBLIC_IP}/hook"))

    assert resolver.calls == [(PUBLIC_IP, 443)]
    assert route.calls.last.request.headers["host"] == PUBLIC_IP


async def test_send_resolves_exactly_once_per_delivery(
    sender: HttpWebhookSender, router: respx.MockRouter, resolver: StaticResolver
) -> None:
    router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(200))

    await sender.send(make_payment())

    assert len(resolver.calls) == 1


async def test_send_tries_the_next_address_after_a_failed_connect(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter
) -> None:
    multi = make_sender(StaticResolver(PUBLIC_IPV6, PUBLIC_IP))
    first = router.route(host=PUBLIC_IPV6).mock(side_effect=httpx.ConnectError("unreachable"))
    second = router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(200))

    await multi.send(make_payment())

    assert (first.call_count, second.call_count) == (1, 1)


async def test_send_tries_the_next_address_after_a_connect_timeout(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter
) -> None:
    multi = make_sender(StaticResolver(PUBLIC_IPV6, PUBLIC_IP))
    router.route(host=PUBLIC_IPV6).mock(side_effect=httpx.ConnectTimeout("slow"))
    second = router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(200))

    await multi.send(make_payment())

    assert second.call_count == 1


async def test_send_fails_when_every_address_refuses_the_connection(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter
) -> None:
    multi = make_sender(StaticResolver(PUBLIC_IPV6, PUBLIC_IP))
    first = router.route(host=PUBLIC_IPV6).mock(side_effect=httpx.ConnectError("unreachable"))
    second = router.route(host=PUBLIC_IP).mock(side_effect=httpx.ConnectError("refused"))

    with pytest.raises(WebhookDeliveryError, match="ConnectError"):
        await multi.send(make_payment())

    assert (first.call_count, second.call_count) == (1, 1)


@pytest.mark.parametrize(
    "failure",
    [httpx.ReadTimeout("slow"), httpx.RemoteProtocolError("bad"), httpx.Response(500)],
    ids=["read-timeout", "protocol-error", "http-500"],
)
async def test_send_does_not_try_another_address_once_the_request_may_have_been_delivered(
    make_sender: Callable[..., HttpWebhookSender],
    router: respx.MockRouter,
    failure: Exception | httpx.Response,
) -> None:
    multi = make_sender(StaticResolver(PUBLIC_IPV6, PUBLIC_IP))
    if isinstance(failure, httpx.Response):
        first = router.route(host=PUBLIC_IPV6).mock(return_value=failure)
    else:
        first = router.route(host=PUBLIC_IPV6).mock(side_effect=failure)
    second = router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(200))

    with pytest.raises(WebhookDeliveryError):
        await multi.send(make_payment())

    assert (first.call_count, second.call_count) == (1, 0)


# --------------------------------------------------------------------------- #
# HttpWebhookSender - failures
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("status", [400, 401, 404, 408, 429, 500, 502, 503])
async def test_send_raises_delivery_error_on_non_2xx(
    sender: HttpWebhookSender, router: respx.MockRouter, status: int
) -> None:
    router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(status, text="secret body of the receiver"))

    with pytest.raises(WebhookDeliveryError) as excinfo:
        await sender.send(make_payment())

    message = str(excinfo.value)
    assert f"HTTP {status}" in message
    assert "hooks.example.com" in message
    assert "secret body" not in message


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
async def test_send_does_not_follow_redirects(
    sender: HttpWebhookSender, router: respx.MockRouter, status: int
) -> None:
    redirect = router.route(host=PUBLIC_IP).mock(
        return_value=httpx.Response(status, headers={"Location": "http://169.254.169.254/latest/meta-data/"})
    )

    with pytest.raises(WebhookDeliveryError, match=f"HTTP {status}"):
        await sender.send(make_payment())

    assert redirect.call_count == 1  # the metadata address is not even mocked: following would blow up


async def test_send_does_not_follow_redirects_even_if_an_injected_client_would(
    router: respx.MockRouter, resolver: StaticResolver
) -> None:
    # Following would reach 169.254.169.254, which is not mocked and would fail the test loudly.
    redirect = router.route(host=PUBLIC_IP).mock(
        return_value=httpx.Response(302, headers={"Location": "http://169.254.169.254/"})
    )
    async with httpx.AsyncClient(follow_redirects=True) as client:
        injected = HttpWebhookSender(make_settings(), client=client, resolver=resolver)

        with pytest.raises(WebhookDeliveryError, match="HTTP 302"):
            await injected.send(make_payment())

    assert redirect.call_count == 1


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectTimeout("t"),
        httpx.ReadTimeout("t"),
        httpx.WriteTimeout("t"),
        httpx.PoolTimeout("t"),
        httpx.ConnectError("refused"),
        httpx.ReadError("reset"),
        httpx.RemoteProtocolError("garbage"),
    ],
    ids=lambda error: type(error).__name__,
)
async def test_send_wraps_network_errors_and_timeouts(
    sender: HttpWebhookSender, router: respx.MockRouter, error: httpx.HTTPError
) -> None:
    router.route(host=PUBLIC_IP).mock(side_effect=error)

    with pytest.raises(WebhookDeliveryError) as excinfo:
        await sender.send(make_payment())

    assert excinfo.value.__cause__ is error
    assert type(error).__name__ in str(excinfo.value)
    assert "hooks.example.com" in str(excinfo.value)


async def test_send_enforces_an_overall_deadline(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter
) -> None:
    async def never_answers(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(30)
        return httpx.Response(200)

    router.route(host=PUBLIC_IP).mock(side_effect=never_answers)
    quick = make_sender(timeout_seconds=0.02)

    with pytest.raises(WebhookDeliveryError, match="timed out"):
        await quick.send(make_payment())


async def test_send_propagates_cancellation(sender: HttpWebhookSender, router: respx.MockRouter) -> None:
    started = asyncio.Event()

    async def hangs(request: httpx.Request) -> httpx.Response:
        started.set()
        await asyncio.sleep(30)
        return httpx.Response(200)

    router.route(host=PUBLIC_IP).mock(side_effect=hangs)
    task = asyncio.create_task(sender.send(make_payment()))
    await started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# --------------------------------------------------------------------------- #
# HttpWebhookSender - SSRF guard
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "private", ["10.0.0.5", "127.0.0.1", "169.254.169.254", "192.168.1.1", "::1", "100.64.0.1"]
)
async def test_send_blocks_hosts_that_resolve_to_private_addresses(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter, private: str
) -> None:
    anything = router.route().mock(return_value=httpx.Response(200))
    guarded = make_sender(StaticResolver(private))

    with pytest.raises(WebhookDeliveryError, match="SSRF guard") as excinfo:
        await guarded.send(make_payment())

    assert "hooks.example.com" in str(excinfo.value)
    assert not anything.called


async def test_send_blocks_dns_answers_that_mix_public_and_private_records(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter
) -> None:
    anything = router.route().mock(return_value=httpx.Response(200))
    guarded = make_sender(StaticResolver(PUBLIC_IP, "10.0.0.5"))

    with pytest.raises(WebhookDeliveryError, match="SSRF guard"):
        await guarded.send(make_payment())

    assert not anything.called


async def test_send_blocks_hosts_that_do_not_resolve(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter
) -> None:
    anything = router.route().mock(return_value=httpx.Response(200))
    guarded = make_sender(StaticResolver())

    with pytest.raises(WebhookDeliveryError, match="SSRF guard"):
        await guarded.send(make_payment())

    assert not anything.called


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/hook",
        "http://127.0.0.1:9000/hook",
        "http://169.254.169.254/latest/meta-data/",
        "http://[::1]/hook",
        "http://2130706433/hook",
        "http://redis/",
        "http://user:pass@hooks.example.com/hook",
        "ftp://hooks.example.com/hook",
        "file:///etc/passwd",
        "http://",
        "not a url",
    ],
)
async def test_send_revalidates_the_url_before_resolving_or_connecting(
    sender: HttpWebhookSender, router: respx.MockRouter, resolver: StaticResolver, url: str
) -> None:
    anything = router.route().mock(return_value=httpx.Response(200))

    with pytest.raises(WebhookDeliveryError, match="SSRF guard"):
        await sender.send(make_payment(webhook_url=url))

    assert not anything.called
    assert resolver.calls == []


async def test_send_with_allow_private_networks_connects_to_private_addresses(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter
) -> None:
    route = router.route(host="10.0.0.5").mock(return_value=httpx.Response(200))
    relaxed = make_sender(StaticResolver("10.0.0.5"), allow_private_networks=True)

    await relaxed.send(make_payment(webhook_url="http://webhook-sink:9000/ok"))

    request = route.calls.last.request
    assert str(request.url) == "http://10.0.0.5:9000/ok"
    assert request.headers["host"] == "webhook-sink:9000"


@pytest.mark.parametrize(
    "answer",
    [
        ["169.254.169.254"],
        ["169.254.170.2"],
        ["100.100.100.200"],
        ["fd00:ec2::254"],
        ["fe80::1"],
        ["::ffff:169.254.169.254"],
        ["0.0.0.0"],  # noqa: S104 - an address under test, not a bind
        ["10.0.0.5", "169.254.169.254"],
        ["169.254.169.254", "10.0.0.5"],
        [PUBLIC_IP, "127.0.0.1", "169.254.169.254"],
    ],
)
async def test_send_with_allow_private_networks_never_sends_to_a_name_resolving_to_a_metadata_address(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter, answer: list[str]
) -> None:
    anything = router.route().mock(return_value=httpx.Response(200))
    resolving = StaticResolver(*answer)
    relaxed = make_sender(resolving, allow_private_networks=True)

    with pytest.raises(WebhookDeliveryError, match="SSRF guard") as excinfo:
        await relaxed.send(make_payment(webhook_url="http://rebind.example.com/hook"))

    assert "rebind.example.com" in str(excinfo.value)
    assert resolving.calls == [("rebind.example.com", 80)]
    assert not anything.called


@pytest.mark.parametrize(
    "url",
    [
        "http://169.254.169.254/latest/meta-data/",
        "http://100.100.100.200/latest/meta-data/",
        "http://[fd00:ec2::254]/latest/meta-data/",
        "http://[::ffff:169.254.169.254]/",
        "http://metadata.google.internal/computeMetadata/v1/",
        "http://metadata/",
        "http://2852039166/",
    ],
)
async def test_send_with_allow_private_networks_rejects_metadata_urls_before_resolving(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter, url: str
) -> None:
    anything = router.route().mock(return_value=httpx.Response(200))
    resolving = StaticResolver("10.0.0.5")
    relaxed = make_sender(resolving, allow_private_networks=True)

    with pytest.raises(WebhookDeliveryError, match="SSRF guard"):
        await relaxed.send(make_payment(webhook_url=url))

    assert resolving.calls == []
    assert not anything.called


async def test_send_with_allow_private_networks_still_rejects_bad_schemes_and_credentials(
    make_sender: Callable[..., HttpWebhookSender], router: respx.MockRouter
) -> None:
    anything = router.route().mock(return_value=httpx.Response(200))
    relaxed = make_sender(allow_private_networks=True)

    for url in ("ftp://localhost/x", "http://user:pass@localhost/x", "http://2130706433/x"):
        with pytest.raises(WebhookDeliveryError):
            await relaxed.send(make_payment(webhook_url=url))

    assert not anything.called


# --------------------------------------------------------------------------- #
# HttpWebhookSender - secrecy and logging
# --------------------------------------------------------------------------- #
LEAKY_URL = "https://hooks.example.com/h/PATH-TOKEN-123?api_key=QUERY-TOKEN-456"


@pytest.mark.parametrize(
    "scenario",
    ["http-500", "redirect", "connect-error", "read-timeout", "blocked-by-guard", "bad-url"],
)
async def test_secret_and_url_details_never_reach_exceptions_or_logs(
    make_sender: Callable[..., HttpWebhookSender],
    router: respx.MockRouter,
    caplog: pytest.LogCaptureFixture,
    scenario: str,
) -> None:
    caplog.set_level(logging.DEBUG)
    # httpx itself logs "HTTP Request: POST <full url>" at INFO. The application's logging configuration
    # must keep the "httpx" logger at WARNING or above (see the report); this test checks our own records.
    caplog.set_level(logging.WARNING, logger="httpx")
    resolver = StaticResolver("10.0.0.5" if scenario == "blocked-by-guard" else PUBLIC_IP)
    url = "http://user:pass-9999@hooks.example.com/x?q=1" if scenario == "bad-url" else LEAKY_URL
    behaviours: dict[str, dict[str, Any]] = {
        "http-500": {"return_value": httpx.Response(500, text=SECRET)},
        "redirect": {"return_value": httpx.Response(302, headers={"Location": f"https://evil/{SECRET}"})},
        "connect-error": {"side_effect": httpx.ConnectError(f"cannot connect {SECRET}")},
        "read-timeout": {"side_effect": httpx.ReadTimeout("slow")},
        "blocked-by-guard": {"return_value": httpx.Response(200)},
        "bad-url": {"return_value": httpx.Response(200)},
    }
    router.route(host=PUBLIC_IP).mock(**behaviours[scenario])
    leaky = make_sender(resolver)

    with pytest.raises(WebhookDeliveryError) as excinfo:
        await leaky.send(make_payment(webhook_url=url))

    message = str(excinfo.value)
    # A rejected URL is not trusted enough to be echoed, not even its host.
    assert ("unknown" if scenario == "bad-url" else "hooks.example.com") in message
    for forbidden in (
        SECRET,
        "PATH-TOKEN-123",
        "QUERY-TOKEN-456",
        "api_key",
        "pass-9999",
        "?q=1",
        "https://",
        "http://",
    ):
        assert forbidden not in message
    assert caplog.records, "a failure must be logged"
    for record in caplog.records:
        rendered = f"{record.getMessage()} {record.__dict__}"
        assert SECRET not in rendered
        assert "PATH-TOKEN-123" not in rendered
        assert "QUERY-TOKEN-456" not in rendered
        assert "pass-9999" not in rendered


async def test_send_logs_success_at_info_with_payment_and_host(
    sender: HttpWebhookSender, router: respx.MockRouter, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(204))

    await sender.send(make_payment())

    records = [r for r in caplog.records if r.name == SENDER_LOGGER]
    assert [r.levelno for r in records] == [logging.INFO]
    assert records[0].payment_id == "3f1f8f0e-6a3b-4c53-9d2e-0a1b2c3d4e5f"
    assert records[0].webhook_host == "hooks.example.com"
    assert SECRET not in caplog.text


async def test_send_logs_failure_at_warning_with_payment_and_host(
    sender: HttpWebhookSender, router: respx.MockRouter, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    router.route(host=PUBLIC_IP).mock(return_value=httpx.Response(503))

    with pytest.raises(WebhookDeliveryError):
        await sender.send(make_payment())

    records = [r for r in caplog.records if r.name == SENDER_LOGGER]
    assert [r.levelno for r in records] == [logging.WARNING]
    assert records[0].payment_id == "3f1f8f0e-6a3b-4c53-9d2e-0a1b2c3d4e5f"
    assert records[0].webhook_host == "hooks.example.com"
    assert "503" in records[0].getMessage()


def test_settings_repr_hides_the_secret() -> None:
    assert SECRET not in repr(make_settings())


# --------------------------------------------------------------------------- #
# HttpWebhookSender - client configuration and lifecycle
# --------------------------------------------------------------------------- #
async def test_default_client_is_locked_down(
    monkeypatch: pytest.MonkeyPatch, resolver: StaticResolver
) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid:3128")
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.invalid:3128")
    monkeypatch.setenv("ALL_PROXY", "http://proxy.invalid:3128")

    locked = HttpWebhookSender(make_settings(timeout_seconds=2.5), resolver=resolver)
    client = locked._client

    assert client.follow_redirects is False
    assert client.trust_env is False
    assert client._mounts == {}
    assert client.timeout == httpx.Timeout(2.5)
    await locked.aclose()


async def test_aclose_closes_the_client_it_created(resolver: StaticResolver) -> None:
    owner = HttpWebhookSender(make_settings(), resolver=resolver)
    client = owner._client

    await owner.aclose()

    assert client.is_closed


async def test_aclose_leaves_an_injected_client_open(
    http_client: httpx.AsyncClient, make_sender: Callable[..., HttpWebhookSender]
) -> None:
    borrower = make_sender()

    await borrower.aclose()

    assert not http_client.is_closed


# --------------------------------------------------------------------------- #
# HttpWebhookSender over a real socket (loopback only, no external network)
# --------------------------------------------------------------------------- #
@contextlib.asynccontextmanager
async def serve(
    response: bytes,
    *,
    ssl_context: ssl.SSLContext | None = None,
) -> AsyncIterator[tuple[int, list[bytes]]]:
    """A one-shot-per-connection HTTP server on 127.0.0.1; yields its port and the raw requests it got."""
    received: list[bytes] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            length = 0
            for line in head.split(b"\r\n"):
                if line.lower().startswith(b"content-length:"):
                    length = int(line.split(b":", 1)[1])
            received.append(head + await reader.readexactly(length))
            writer.write(response)
            await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError, ssl.SSLError):
                await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=ssl_context)
    try:
        yield server.sockets[0].getsockname()[1], received
    finally:
        server.close()
        await server.wait_closed()


NO_CONTENT = b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"


async def test_wire_request_goes_to_the_vetted_ip_with_the_original_host_header(
    make_sender: Callable[..., HttpWebhookSender],
) -> None:
    async with serve(NO_CONTENT) as (port, received):
        wire = make_sender(StaticResolver("127.0.0.1"), allow_private_networks=True)

        await wire.send(make_payment(webhook_url=f"http://hooks.example.com:{port}/payments?x=1"))

    head, _, body = received[0].partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    headers = {line.split(":", 1)[0].lower(): line.split(":", 1)[1].strip() for line in lines[1:]}
    assert lines[0] == "POST /payments?x=1 HTTP/1.1"
    assert headers["host"] == f"hooks.example.com:{port}"
    assert headers["connection"] == "close"
    assert headers["content-type"] == "application/json"
    assert verify_signature(SECRET, int(headers["x-timestamp"]), body, headers["x-signature"])


async def test_wire_falls_back_to_the_next_address_when_the_first_refuses_the_connection(
    make_sender: Callable[..., HttpWebhookSender],
) -> None:
    async with serve(NO_CONTENT) as (port, received):
        # Nothing listens on [::1]:port (the server is bound to IPv4 loopback only): connect fails first.
        wire = make_sender(StaticResolver("::1", "127.0.0.1"), allow_private_networks=True)

        await wire.send(make_payment(webhook_url=f"http://hooks.example.com:{port}/p"))

    assert len(received) == 1


async def test_wire_non_2xx_answer_with_a_large_unread_body_is_a_delivery_error(
    make_sender: Callable[..., HttpWebhookSender],
) -> None:
    big_body = b"x" * 100_000
    head = b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: %d\r\nConnection: close\r\n\r\n"
    async with serve(head % len(big_body) + big_body) as (port, _):
        wire = make_sender(StaticResolver("127.0.0.1"), allow_private_networks=True)

        with pytest.raises(WebhookDeliveryError, match="HTTP 500"):
            await wire.send(make_payment(webhook_url=f"http://hooks.example.com:{port}/p"))


# --------------------------------------------------------------------------- #
# TLS: the certificate must be verified against the real host name, not the IP
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def tls_material(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """A throw-away self-signed certificate for ``hooks.example.com`` (needs the ``openssl`` binary)."""
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("the openssl binary is needed to generate a test certificate")
    directory = tmp_path_factory.mktemp("tls")
    certificate, key = directory / "cert.pem", directory / "key.pem"
    subprocess.run(  # noqa: S603 - fixed arguments, no user input
        [
            openssl,
            *("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2"),
            *("-subj", "/CN=hooks.example.com", "-addext", "subjectAltName=DNS:hooks.example.com"),
            *("-keyout", str(key), "-out", str(certificate)),
        ],
        check=True,
        capture_output=True,
    )
    return certificate, key


async def test_tls_certificate_is_verified_against_the_host_name_while_connecting_to_the_ip(
    tls_material: tuple[Path, Path],
) -> None:
    certificate, key = tls_material
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(certificate, key)
    requested_names: list[str | None] = []
    server_context.sni_callback = lambda _sock, name, _ctx: requested_names.append(name)
    client_context = ssl.create_default_context(cafile=str(certificate))

    async with (
        serve(NO_CONTENT, ssl_context=server_context) as (port, received),
        httpx.AsyncClient(verify=client_context, trust_env=False) as client,
    ):
        wire = HttpWebhookSender(
            make_settings(allow_private_networks=True),
            client=client,
            resolver=StaticResolver("127.0.0.1"),
        )

        await wire.send(make_payment(webhook_url=f"https://hooks.example.com:{port}/p"))

    assert len(received) == 1
    assert requested_names == ["hooks.example.com"]


async def test_tls_certificate_for_another_host_name_is_rejected(tls_material: tuple[Path, Path]) -> None:
    certificate, key = tls_material
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(certificate, key)
    client_context = ssl.create_default_context(cafile=str(certificate))

    async with (
        serve(NO_CONTENT, ssl_context=server_context) as (port, received),
        httpx.AsyncClient(verify=client_context, trust_env=False) as client,
    ):
        wire = HttpWebhookSender(
            make_settings(allow_private_networks=True),
            client=client,
            resolver=StaticResolver("127.0.0.1"),
        )

        # The certificate is valid for hooks.example.com only; the URL names another host.
        with pytest.raises(WebhookDeliveryError, match=r"other\.example\.com"):
            await wire.send(make_payment(webhook_url=f"https://other.example.com:{port}/p"))

    assert received == []

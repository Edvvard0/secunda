"""Outgoing webhooks: HMAC signing and the SSRF-hardened HTTP sender.

Wire format (documented for receivers, see ``docs/ARCHITECTURE.md`` section 8):

* ``X-Timestamp`` - Unix time in seconds when the delivery attempt was signed;
* ``X-Signature`` - ``sha256=<hex>``, HMAC-SHA256 over ``"<timestamp>.<raw body>"`` keyed with
  ``WEBHOOK_SIGNING_SECRET``; receivers should compare in constant time and reject stale timestamps
  (:func:`verify_signature` does both);
* ``X-Payment-Id`` - the payment id, handy for de-duplication (delivery is at-least-once).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from typing import TYPE_CHECKING

import httpx

from payment_service.common.netguard import UnsafeTargetError, parse_webhook_url, resolve_public_addresses
from payment_service.common.time import utcnow
from payment_service.domain.exceptions import WebhookDeliveryError
from payment_service.services.interfaces import WebhookSender

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from payment_service.common.netguard import Resolver, WebhookTarget
    from payment_service.config import WebhookSettings
    from payment_service.services.dto import PaymentDTO

logger = logging.getLogger(__name__)

EVENT_TYPE = "payment.processed"
SIGNATURE_PREFIX = "sha256="
DEFAULT_TOLERANCE_SECONDS = 300

_DEADLINE_FACTOR = 3.0
"""Wall-clock budget of one delivery in units of the per-operation timeout (DNS + all connect attempts +
request), so a server that dribbles its answer byte by byte cannot hold a worker for long."""
_MAX_CONNECTIONS = 50
_UNKNOWN_HOST = "unknown"


def sign(secret: str, timestamp: int, body: bytes) -> str:
    """Return the ``X-Signature`` value for ``body`` signed at ``timestamp``."""
    message = f"{timestamp}.".encode("ascii") + body
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return f"{SIGNATURE_PREFIX}{digest}"


def verify_signature(
    secret: str,
    timestamp: int,
    body: bytes,
    signature: str,
    *,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
    now: int | None = None,
) -> bool:
    """Check a received signature in constant time and reject timestamps outside the tolerance window.

    The window defends against replaying a captured request later; it applies in both directions so a
    far-future timestamp is refused as well.

    Args:
        secret: The shared signing secret.
        timestamp: Value of the ``X-Timestamp`` header.
        body: The raw, unparsed request body.
        signature: Value of the ``X-Signature`` header.
        tolerance_seconds: Maximum accepted difference between ``timestamp`` and ``now``.
        now: Current Unix time in seconds (injectable for tests); defaults to the real clock.
    """
    current = int(utcnow().timestamp()) if now is None else now
    if abs(current - timestamp) > tolerance_seconds:
        return False
    expected = sign(secret, timestamp, body)
    return hmac.compare_digest(expected.encode("utf-8"), signature.encode("utf-8"))


def serialize_payment_event(payment: PaymentDTO) -> bytes:
    """Build the webhook body: compact, key-sorted JSON so the signed bytes are deterministic.

    The amount is a string (never a float) with exactly two decimals.
    """
    event = {
        "event": EVENT_TYPE,
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": f"{payment.amount:.2f}",
        "currency": payment.currency.value,
        "description": payment.description,
        "metadata": payment.metadata,
        "created_at": payment.created_at.isoformat(),
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }
    return json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


class HttpWebhookSender(WebhookSender):
    """Delivers webhooks over HTTP(S) with SSRF protection and HMAC signing.

    The client-supplied URL is untrusted, so every delivery re-validates it and resolves the host name
    itself: all resolved addresses must be public, and the connection goes to one of those *vetted*
    addresses (never to the name), with the original name kept in the ``Host`` header and, for HTTPS, in
    the TLS server name so the certificate is still verified against the real host. Redirects are never
    followed, proxies from the environment are ignored, and connections are not reused (a pooled
    connection is keyed by IP and could otherwise carry a request meant for another host name).

    The signing secret is only read when signing and never logged.
    """

    def __init__(
        self,
        settings: WebhookSettings,
        *,
        client: httpx.AsyncClient | None = None,
        resolver: Resolver | None = None,
    ) -> None:
        """Create the sender.

        Args:
            settings: Webhook settings (secret, timeout, SSRF switch, user agent).
            client: Replacement HTTP client for tests. It must not follow redirects or use proxies and the
                caller keeps ownership: :meth:`aclose` closes only a client created here.
            resolver: Replacement for the operating system DNS resolver.
        """
        self._settings = settings
        self._resolver = resolver
        self._owns_client = client is None
        self._client = client if client is not None else self._build_client(settings)
        self._deadline_seconds = settings.timeout_seconds * _DEADLINE_FACTOR

    @staticmethod
    def _build_client(settings: WebhookSettings) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=httpx.Timeout(settings.timeout_seconds),
            follow_redirects=False,
            # A proxy configured in the environment would resolve and connect on our behalf and bypass the
            # address vetting below.
            trust_env=False,
            limits=httpx.Limits(max_connections=_MAX_CONNECTIONS, max_keepalive_connections=0),
        )

    async def aclose(self) -> None:
        """Release the HTTP client created by this sender (an injected client is left to its owner)."""
        if self._owns_client:
            await self._client.aclose()

    async def send(self, payment: PaymentDTO) -> None:
        """Deliver the webhook for ``payment``.

        Raises:
            WebhookDeliveryError: on a rejected target, network error, timeout, TLS error or any
                non-2xx answer (redirects included). The message names the host and the reason only.
        """
        host = _UNKNOWN_HOST
        try:
            target = parse_webhook_url(
                payment.webhook_url, allow_private_networks=self._settings.allow_private_networks
            )
            host = target.host
            status_code = await self._deliver(payment, target)
        except UnsafeTargetError as exc:
            raise self._failure(payment, host, f"target rejected by the SSRF guard: {exc}") from exc
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            raise self._failure(payment, host, f"request failed ({type(exc).__name__})") from exc
        except TimeoutError as exc:
            raise self._failure(payment, host, "timed out") from exc

        if not httpx.codes.is_success(status_code):
            raise self._failure(payment, host, f"receiver answered HTTP {status_code}")
        logger.info(
            "webhook delivered: host=%s status=%s",
            host,
            status_code,
            extra={"payment_id": str(payment.id), "webhook_host": host},
        )

    def _failure(self, payment: PaymentDTO, host: str, reason: str) -> WebhookDeliveryError:
        logger.warning(
            "webhook delivery failed: host=%s reason=%s",
            host,
            reason,
            extra={"payment_id": str(payment.id), "webhook_host": host},
        )
        return WebhookDeliveryError(f"webhook delivery to {host} failed: {reason}")

    async def _deliver(self, payment: PaymentDTO, target: WebhookTarget) -> int:
        body = serialize_payment_event(payment)
        timestamp = int(utcnow().timestamp())
        headers = {
            "Content-Type": "application/json",
            "User-Agent": self._settings.user_agent,
            "X-Payment-Id": str(payment.id),
            "X-Timestamp": str(timestamp),
            "X-Signature": sign(self._settings.signing_secret.get_secret_value(), timestamp, body),
            # Send the logical host, not the IP literal we connect to.
            "Host": target.host_header,
            "Connection": "close",
        }
        async with asyncio.timeout(self._deadline_seconds):
            addresses = await resolve_public_addresses(
                target.host,
                target.port,
                allow_private_networks=self._settings.allow_private_networks,
                resolver=self._resolver,
            )
            return await self._post_to_first_reachable(target, addresses, body, headers)

    async def _post_to_first_reachable(
        self,
        target: WebhookTarget,
        addresses: Sequence[str],
        body: bytes,
        headers: Mapping[str, str],
    ) -> int:
        """POST to the vetted addresses in order; only a failed *connect* moves on to the next one.

        Once a connection exists the request may have reached the receiver, so any later failure is
        reported as is instead of being repeated against another address.
        """
        for address in addresses[:-1]:
            try:
                return await self._post(target, address, body, headers)
            except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
                logger.debug(
                    "webhook connect failed, trying the next address: host=%s error=%s",
                    target.host,
                    type(exc).__name__,
                )
        return await self._post(target, addresses[-1], body, headers)

    async def _post(
        self,
        target: WebhookTarget,
        address: str,
        body: bytes,
        headers: Mapping[str, str],
    ) -> int:
        literal = f"[{address}]" if ":" in address else address
        url = f"{target.scheme}://{literal}:{target.port}{target.request_target}"
        # The TLS handshake (SNI and certificate verification) must use the real host name, not the IP.
        extensions = {"sni_hostname": target.host} if target.scheme == "https" else {}
        # Streaming: only the status line and headers are read, the body is discarded on close.
        async with self._client.stream(
            "POST",
            url,
            content=body,
            headers=headers,
            extensions=extensions,
            follow_redirects=False,
        ) as response:
            return response.status_code

"""Demo webhook receiver, used by the README to show signing, retries and the dead-letter queue.

NOT FOR PRODUCTION. State lives in process memory and is lost on restart, nothing is authenticated
unless ``SINK_SIGNING_SECRET`` is set, and it listens on all interfaces. It exists only so that a
reviewer can watch the payment service deliver webhooks.

Run it with ``python -m payment_service.tools.webhook_sink`` (port 9000). Environment:

* ``SINK_SIGNING_SECRET`` - optional. When set, ``X-Signature`` / ``X-Timestamp`` are verified with the
  same algorithm the service signs with, and a mismatch is answered with ``401``. Use the value of the
  service's ``WEBHOOK_SIGNING_SECRET``.
* ``SINK_FLAKY_FAILURES`` - how many deliveries per payment ``/flaky`` rejects before accepting (default 2).

Routes (all ``POST``): ``/ok`` always answers 200, ``/fail`` always 500, ``/flaky`` answers 500 for the
first ``SINK_FLAKY_FAILURES`` deliveries of each ``X-Payment-Id`` and 200 afterwards. ``GET /received``
lists the last 100 deliveries for manual checking.
"""

from __future__ import annotations

import json
import logging
import os
from collections import OrderedDict, deque
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from payment_service.services.webhook import verify_signature

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

MAX_BODY_BYTES = 1024 * 1024
"""Larger request bodies are refused with 413 without being read in full."""
HISTORY_SIZE = 100
_MAX_TRACKED_ATTEMPT_KEYS = 10_000
"""Bound on the in-memory attempt counters, so a flood of distinct payment ids cannot exhaust memory."""
_MAX_PAYMENT_ID_LENGTH = 128
_PORT = 9000
_DEFAULT_FLAKY_FAILURES = 2


class Delivery(BaseModel):
    """One webhook delivery as seen by the sink."""

    payment_id: str
    path: str
    status_code: int
    """The status the sink answered with."""
    signature_valid: bool | None
    """``None`` when no signing secret is configured, so the signature was not checked."""
    attempt: int
    """How many times this payment has been delivered to this path, this delivery included."""
    body: Any
    """Parsed JSON when the body is valid JSON, decoded text otherwise, ``None`` when it was too large."""


class SinkState:
    """In-memory delivery history and per-payment attempt counters."""

    def __init__(self, signing_secret: str | None, flaky_failures: int) -> None:
        self.signing_secret = signing_secret
        self.flaky_failures = flaky_failures
        self.deliveries: deque[Delivery] = deque(maxlen=HISTORY_SIZE)
        self._attempts: OrderedDict[tuple[str, str], int] = OrderedDict()

    def next_attempt(self, path: str, payment_id: str) -> int:
        """Count one more delivery of ``payment_id`` to ``path`` and return the new count."""
        key = (path, payment_id)
        attempt = self._attempts.pop(key, 0) + 1
        self._attempts[key] = attempt
        if len(self._attempts) > _MAX_TRACKED_ATTEMPT_KEYS:
            self._attempts.popitem(last=False)
        return attempt


def create_app(
    *, signing_secret: str | None = None, flaky_failures: int = _DEFAULT_FLAKY_FAILURES
) -> FastAPI:
    """Build the receiver application with its own state (one instance per call).

    Raises:
        ValueError: if ``flaky_failures`` is negative.
    """
    if flaky_failures < 0:
        msg = "flaky_failures must not be negative"
        raise ValueError(msg)
    state = SinkState(signing_secret, flaky_failures)
    sink = FastAPI(title="Webhook sink (demo)", docs_url=None, redoc_url=None, openapi_url=None)

    @sink.post("/ok")
    async def ok(request: Request) -> JSONResponse:
        """Always accept."""
        return await _receive(request, state, lambda _attempt: HTTPStatus.OK)

    @sink.post("/fail")
    async def fail(request: Request) -> JSONResponse:
        """Always answer 500, to drive a payment into the dead-letter queue."""
        return await _receive(request, state, lambda _attempt: HTTPStatus.INTERNAL_SERVER_ERROR)

    @sink.post("/flaky")
    async def flaky(request: Request) -> JSONResponse:
        """Answer 500 for the first deliveries of each payment, then 200, to show a successful retry."""
        return await _receive(
            request,
            state,
            lambda attempt: (
                HTTPStatus.INTERNAL_SERVER_ERROR if attempt <= state.flaky_failures else HTTPStatus.OK
            ),
        )

    @sink.get("/received")
    async def received() -> list[Delivery]:
        """The last deliveries, oldest first."""
        return list(state.deliveries)

    return sink


def create_app_from_env() -> FastAPI:
    """Build the receiver configured through ``SINK_*`` environment variables."""
    raw_failures = os.environ.get("SINK_FLAKY_FAILURES", str(_DEFAULT_FLAKY_FAILURES))
    try:
        flaky_failures = int(raw_failures)
    except ValueError:
        msg = "SINK_FLAKY_FAILURES must be an integer"
        raise ValueError(msg) from None
    return create_app(
        signing_secret=os.environ.get("SINK_SIGNING_SECRET") or None,
        flaky_failures=flaky_failures,
    )


async def _receive(
    request: Request,
    state: SinkState,
    status_for_attempt: Callable[[int], HTTPStatus],
) -> JSONResponse:
    path = request.url.path
    payment_id = _printable(request.headers.get("x-payment-id", "-"))[:_MAX_PAYMENT_ID_LENGTH]
    attempt = state.next_attempt(path, payment_id)

    raw_body = await _read_body(request)
    signature_valid: bool | None = None
    parsed_body: Any = None
    if raw_body is None:
        status = HTTPStatus.REQUEST_ENTITY_TOO_LARGE
    else:
        parsed_body = _decode(raw_body)
        if state.signing_secret is not None:
            signature_valid = _signature_is_valid(state.signing_secret, request, raw_body)
        status = HTTPStatus.UNAUTHORIZED if signature_valid is False else status_for_attempt(attempt)

    delivery = Delivery(
        payment_id=payment_id,
        path=path,
        status_code=status,
        signature_valid=signature_valid,
        attempt=attempt,
        body=parsed_body,
    )
    state.deliveries.append(delivery)
    logger.info(
        "delivery: path=%s payment_id=%s attempt=%d status=%d signature_valid=%s",
        path,
        payment_id,
        attempt,
        status,
        signature_valid,
    )
    outcome = "accepted" if status < HTTPStatus.BAD_REQUEST else "rejected"
    return JSONResponse({"status": outcome}, status_code=status)


async def _read_body(request: Request) -> bytes | None:
    """Read the request body, or ``None`` if it exceeds :data:`MAX_BODY_BYTES`."""
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > MAX_BODY_BYTES:
        return None
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_BODY_BYTES:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


def _signature_is_valid(secret: str, request: Request, body: bytes) -> bool:
    signature = request.headers.get("x-signature")
    raw_timestamp = request.headers.get("x-timestamp")
    if signature is None or raw_timestamp is None:
        return False
    try:
        timestamp = int(raw_timestamp)
    except ValueError:
        return False
    return verify_signature(secret, timestamp, body, signature)


def _decode(body: bytes) -> Any:
    try:
        return json.loads(body)
    except ValueError:
        return body.decode("utf-8", errors="replace")


def _printable(value: str) -> str:
    """Replace control characters so an attacker-supplied header cannot forge log lines."""
    return "".join(char if char.isprintable() else "?" for char in value)


app = create_app_from_env()
"""ASGI application used by ``uvicorn payment_service.tools.webhook_sink:app``."""


def main() -> None:
    """Run the receiver on port 9000."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    uvicorn.run(app, host="0.0.0.0", port=_PORT)  # noqa: S104 - a demo container must be reachable from others


if __name__ == "__main__":
    main()

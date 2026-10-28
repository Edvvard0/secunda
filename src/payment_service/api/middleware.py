"""Pure ASGI middleware: request id propagation, the access log and the request size limit.

Written as plain ASGI (not ``BaseHTTPMiddleware``) so that it does not break context variables and adds no
extra task per request.
"""

from __future__ import annotations

import logging
import re
import sys
import time
import uuid
from collections import deque
from typing import TYPE_CHECKING, Final

from starlette.datastructures import Headers, MutableHeaders

from payment_service.api.problem import REQUEST_ID_HEADER, app_error_response
from payment_service.domain.exceptions import PayloadTooLargeError
from payment_service.logging_config import set_correlation_id

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

REQUEST_ID_STATE_KEY: Final = "request_id"

# Whatever a client sends ends up in logs and response headers, so only a boring alphabet is accepted.
_VALID_REQUEST_ID: Final = re.compile(r"[A-Za-z0-9._-]{1,64}")
_MAX_LOGGED_PATH_LENGTH: Final = 512
_MAX_LENGTH_DIGITS: Final = 18

access_logger = logging.getLogger("payment_service.access")


def request_id_from_scope(scope: Scope) -> str | None:
    """Return the request id stored by ``RequestContextMiddleware``, or ``None`` outside of it."""
    state = scope.get("state")
    if not isinstance(state, dict):
        return None
    value = state.get(REQUEST_ID_STATE_KEY)
    return value if isinstance(value, str) else None


def _printable(path: str) -> str:
    """Make a request path safe to write to a log (no control characters, bounded length)."""
    return "".join(char if char.isprintable() else "?" for char in path[:_MAX_LOGGED_PATH_LENGTH])


class RequestContextMiddleware:
    """Assign every HTTP request an id, expose it everywhere and log one access line per request.

    * A client supplied ``X-Request-ID`` is honoured only if it matches ``[A-Za-z0-9._-]{1,64}``;
      otherwise a fresh ``uuid4().hex`` is used.
    * The id is stored in ``scope["state"]`` (``request.state.request_id``), bound to the logging context
      variable and returned in the ``X-Request-ID`` response header, error responses included.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle one ASGI connection; non-HTTP scopes (lifespan, websocket) pass straight through."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = self._resolve_request_id(scope)
        scope.setdefault("state", {})[REQUEST_ID_STATE_KEY] = request_id
        set_correlation_id(request_id)
        status_code = 500  # what the client sees if the app crashes before sending a response
        started = time.perf_counter()

        async def send_with_request_id(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                message.setdefault("headers", [])
                MutableHeaders(scope=message)[REQUEST_ID_HEADER] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        finally:
            duration_ms = (time.perf_counter() - started) * 1000
            method = scope.get("method", "-")
            path = _printable(scope.get("path", ""))  # ``path`` never contains the query string
            access_logger.info(
                "%s %s -> %d in %.1f ms",
                method,
                path,
                status_code,
                duration_ms,
                extra={
                    "method": method,
                    "path": path,
                    "status": status_code,
                    "duration_ms": round(duration_ms, 2),
                },
            )
            set_correlation_id(None)

    @staticmethod
    def _resolve_request_id(scope: Scope) -> str:
        supplied = Headers(scope=scope).get(REQUEST_ID_HEADER)
        if supplied is not None and _VALID_REQUEST_ID.fullmatch(supplied):
            return supplied
        return uuid.uuid4().hex


class MaxBodySizeMiddleware:
    """Refuse request bodies larger than ``max_body_bytes`` with ``413`` before the application sees them.

    * A declared ``Content-Length`` above the limit is rejected without reading a byte.
    * Without one (chunked transfer) the body is read here, counting bytes as they arrive; the moment the
      count passes the limit the rest is left unread and ``413`` is sent, once. At most ``max_body_bytes``
      are ever held in memory, and a body that fits is replayed to the application unchanged.

    Install it *inside* ``RequestContextMiddleware`` so the 413 carries the request id.
    """

    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle one ASGI connection; non-HTTP scopes pass straight through."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        declared = self._declared_length(scope)
        if declared is not None and declared > self.max_body_bytes:
            await self._reject(scope, receive, send)
            return

        buffered = await self._read_body(receive)
        if buffered is None:
            await self._reject(scope, receive, send)
            return

        pending = deque(buffered)

        async def replay() -> Message:
            # The buffered messages first; afterwards the real channel (e.g. for ``http.disconnect``).
            return pending.popleft() if pending else await receive()

        await self.app(scope, replay, send)

    @staticmethod
    def _declared_length(scope: Scope) -> int | None:
        raw = Headers(scope=scope).get("content-length")
        if raw is None or not raw.isascii() or not raw.isdigit():
            return None  # absent or malformed: the byte counter below still protects us
        digits = raw.lstrip("0") or "0"
        # int() refuses absurdly long digit strings; anything this long is over every sane limit anyway.
        return int(digits) if len(digits) <= _MAX_LENGTH_DIGITS else sys.maxsize

    async def _read_body(self, receive: Receive) -> list[Message] | None:
        """Read the request body; ``None`` when it exceeds the limit (the rest is left unread)."""
        chunks: list[bytes] = []
        size = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                # The client went away mid-body: let the application see the disconnect itself.
                return [message]
            chunk: bytes = message.get("body", b"")
            size += len(chunk)
            if size > self.max_body_bytes:
                return None
            chunks.append(chunk)
            if not message.get("more_body", False):
                return [{"type": "http.request", "body": b"".join(chunks), "more_body": False}]

    async def _reject(self, scope: Scope, receive: Receive, send: Send) -> None:
        error = PayloadTooLargeError(f"The request body must not exceed {self.max_body_bytes} bytes.")
        response = app_error_response(error, request_id=request_id_from_scope(scope))
        await response(scope, receive, send)

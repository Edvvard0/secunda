"""Logging setup: one JSON object per line, enriched with the id of the request being served.

The correlation id lives in a ``ContextVar`` so that every log record emitted while a request (or a
message) is processed carries it without passing it around explicitly. The formatters add nothing that
was not passed to the logger: no environment, no headers, no settings.
"""

from __future__ import annotations

import json
import logging
import math
import sys
import time
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from payment_service.config import LoggingSettings

_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)

# Attributes every LogRecord has (plus the ones Formatter adds); anything else came from ``extra={...}``.
# ``color_message`` is uvicorn's ANSI duplicate of the message and would only add noise.
_STANDARD_ATTRIBUTES: Final = frozenset(
    logging.LogRecord(name="", level=0, pathname="", lineno=0, msg="", args=None, exc_info=None).__dict__
) | {"message", "asctime", "color_message"}

# Keys the JSON formatter writes itself; an ``extra`` with the same name is renamed, never overrides them.
_RESERVED_JSON_KEYS: Final = frozenset({"timestamp", "level", "logger", "message", "exception", "stack"})

# Third-party loggers that configure their own handlers; they are re-pointed at the root logger.
_PROPAGATING_LOGGERS: Final = ("uvicorn", "uvicorn.error", "faststream", "aio_pika", "aiormq")
# The API middleware writes an equivalent access line that includes the request id.
_SILENCED_LOGGERS: Final = ("uvicorn.access",)
# httpx logs every request as ``HTTP Request: POST <full url>`` at INFO, and httpcore traces connections at
# DEBUG. Webhook URLs can carry receiver tokens in the path or query, so these never go below WARNING,
# whatever LOG_LEVEL says.
_QUIET_LOGGERS: Final = ("httpx", "httpcore")

_HANDLER_MARK: Final = "_payment_service_handler"


def set_correlation_id(value: str | None) -> None:
    """Bind the correlation (request) id to the current context; ``None`` clears it."""
    _correlation_id.set(value)


def get_correlation_id() -> str | None:
    """Return the correlation (request) id bound to the current context, if any."""
    return _correlation_id.get()


def _safe_str(value: object) -> str:
    """``str(value)`` that can not raise: a broken ``__str__`` must not break logging."""
    try:
        return str(value)
    except Exception:
        # Logging must never take the application down, whatever a user-supplied object does.
        return f"<unprintable {type(value).__name__}>"


def _json_default(value: object) -> str:
    """Fallback for values ``json`` does not know (UUID, Decimal, datetime, sets, arbitrary objects)."""
    if isinstance(value, datetime):
        return value.isoformat()
    return _safe_str(value)


def _plain(value: object) -> object:
    """Reduce a value to something ``json.dumps(allow_nan=False)`` always accepts."""
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    return _json_default(value)


def _extras(record: logging.LogRecord) -> dict[str, Any]:
    """Fields passed through ``extra={...}``."""
    return {
        key: value
        for key, value in record.__dict__.items()
        if key not in _STANDARD_ATTRIBUTES and not key.startswith("_")
    }


class JsonFormatter(logging.Formatter):
    """Render a record as a single-line JSON object. ``format`` never raises."""

    def format(self, record: logging.LogRecord) -> str:
        """Return the JSON line for ``record``.

        Values that are not JSON serialisable are stringified; if the record still can not be rendered
        (e.g. a broken ``__str__`` in the message arguments) a minimal line describing the failure is
        returned instead of an exception.
        """
        for plain_extras in (False, True):
            try:
                return self._render(record, plain_extras=plain_extras)
            except Exception:  # noqa: S112 - fall through to the safer attempt, then to the fallback
                continue
        return self._fallback(record)

    def _render(self, record: logging.LogRecord, *, plain_extras: bool) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        correlation_id = get_correlation_id()
        if correlation_id:
            # Same value under both names: HTTP calls it request id, the messaging layer correlation id.
            payload["request_id"] = correlation_id
            payload["correlation_id"] = correlation_id
        for key, value in _extras(record).items():
            target = f"extra_{key}" if key in _RESERVED_JSON_KEYS else key
            payload[target] = _plain(value) if plain_extras else value
        if record.exc_info and record.exc_info[0] is not None:
            payload["exception"] = self.formatException(record.exc_info)
        elif record.exc_text:
            payload["exception"] = record.exc_text
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)
        return json.dumps(payload, default=_json_default, ensure_ascii=True, allow_nan=False)

    @staticmethod
    def _fallback(record: logging.LogRecord) -> str:
        try:
            try:
                message = record.getMessage()
            except Exception:
                message = _safe_str(record.msg)
            return json.dumps(
                {
                    "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(
                        timespec="milliseconds"
                    ),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": message,
                    "logging_error": "the record could not be fully rendered",
                },
                default=_json_default,
            )
        except Exception:
            return '{"logging_error": "the record could not be rendered"}'


class ConsoleFormatter(logging.Formatter):
    """Human readable single line: ``time LEVEL logger [request-id] message key=value ...``."""

    converter = staticmethod(time.gmtime)

    def formatMessage(self, record: logging.LogRecord) -> str:  # noqa: N802 - overrides the stdlib hook
        """Build the first line of the output (the exception text, if any, is appended by the base class)."""
        stamp = f"{self.formatTime(record, '%Y-%m-%dT%H:%M:%S')}.{int(record.msecs):03d}Z"
        line = f"{stamp} {record.levelname:<8} {record.name} [{get_correlation_id() or '-'}] {record.message}"
        extras = _extras(record)
        if extras:
            line += " " + " ".join(f"{key}={_safe_str(value)}" for key, value in extras.items())
        return line


def configure_logging(settings: LoggingSettings) -> None:
    """Install the process-wide log handler. Safe to call repeatedly.

    Only the handler installed by a previous call is replaced, so handlers added by other code (a test
    harness, an APM agent) survive. Records of uvicorn/FastStream/aio-pika propagate to the root logger and
    therefore share the format and the level. ``httpx`` and ``httpcore`` are held at WARNING or above so that
    URLs with secrets (webhook tokens) are never logged.
    """
    root = logging.getLogger()
    for existing in list(root.handlers):
        if getattr(existing, _HANDLER_MARK, False):
            root.removeHandler(existing)
            existing.close()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if settings.json_format else ConsoleFormatter())
    setattr(handler, _HANDLER_MARK, True)
    root.addHandler(handler)
    root.setLevel(settings.level)

    for name in _PROPAGATING_LOGGERS:
        library_logger = logging.getLogger(name)
        library_logger.handlers.clear()
        library_logger.propagate = True
    for name in _SILENCED_LOGGERS:
        silenced = logging.getLogger(name)
        silenced.handlers.clear()
        silenced.propagate = False
    for name in _QUIET_LOGGERS:
        logging.getLogger(name).setLevel(max(root.level, logging.WARNING))

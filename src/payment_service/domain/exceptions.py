"""Exception hierarchy.

``AppError`` subclasses are *expected* failures that map onto an HTTP problem response; the API layer
renders them generically (RFC 9457), so adding a new error never requires touching the handlers.
Infrastructure failures (webhook delivery) have their own base, they never reach HTTP clients.
"""

from __future__ import annotations

from typing import ClassVar


class AppError(Exception):
    """Base class of expected, client-visible errors.

    Subclasses only declare class attributes; the generic handler turns them into
    ``application/problem+json`` responses.
    """

    status_code: ClassVar[int] = 500
    code: ClassVar[str] = "internal-error"
    """Stable machine-readable identifier (kebab-case), part of the public contract."""
    title: ClassVar[str] = "Internal error"

    def __init__(self, detail: str | None = None) -> None:
        self.detail = detail or self.title
        super().__init__(self.detail)


class UnauthorizedError(AppError):
    """Missing or wrong ``X-API-Key``."""

    status_code = 401
    code = "unauthorized"
    title = "Invalid or missing API key"


class InvalidIdempotencyKeyError(AppError):
    """``Idempotency-Key`` header is absent, blank or too long."""

    status_code = 400
    code = "invalid-idempotency-key"
    title = "Invalid Idempotency-Key header"


class IdempotencyKeyReuseError(AppError):
    """The same ``Idempotency-Key`` was sent with a different request body."""

    status_code = 422
    code = "idempotency-key-reused"
    title = "Idempotency-Key was already used with a different request body"


class InvalidWebhookUrlError(AppError):
    """``webhook_url`` points at a forbidden target (non-http(s), credentials, private network)."""

    status_code = 422
    code = "invalid-webhook-url"
    title = "webhook_url is not allowed"


class PaymentNotFoundError(AppError):
    """No payment with the requested id."""

    status_code = 404
    code = "payment-not-found"
    title = "Payment not found"


class WebhookDeliveryError(Exception):
    """A webhook could not be delivered (network error, timeout, non-2xx answer, blocked target).

    Always treated as retryable by the consumer: the message goes to a wait queue and, after the last
    attempt, to the dead-letter queue.
    """


class PayloadTooLargeError(AppError):
    """The request body exceeds ``API_MAX_BODY_BYTES``."""

    status_code = 413
    code = "payload-too-large"
    title = "Request body is too large"


class ServiceUnavailableError(AppError):
    """A dependency (database) is unreachable; the caller may retry later."""

    status_code = 503
    code = "service-unavailable"
    title = "Service temporarily unavailable"

"""HTTP request/response models (Pydantic v2).

Amounts travel as JSON *strings* in responses (``"100.50"``) so no client ever parses money as a binary
float. Requests accept a numeric string or a JSON number, but a number is accepted only when it survives
the float round trip untouched.
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime  # noqa: TC003 - pydantic resolves annotations at runtime
from decimal import Decimal
from typing import TYPE_CHECKING, Annotated, Any, Final, Literal, Self
from urllib.parse import urlsplit
from uuid import UUID  # noqa: TC003

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_serializer, field_validator

from payment_service.domain.enums import Currency, PaymentStatus  # noqa: TC001
from payment_service.services.dto import CreatePaymentCommand

if TYPE_CHECKING:
    from payment_service.services.dto import PaymentDTO

MAX_METADATA_BYTES: Final = 4096
MAX_METADATA_DEPTH: Final = 5
MAX_WEBHOOK_URL_LENGTH: Final = 2048
MAX_DESCRIPTION_LENGTH: Final = 1000

_CENT: Final = Decimal("0.01")
# Largest value NUMERIC(18,2) can hold is 10**16 - 0.01; pydantic enforces the digit counts, this is a guard.
_AMOUNT_LIMIT: Final = Decimal(10) ** 16
_PLAIN_DECIMAL: Final = re.compile(r"-?[0-9]+(?:\.[0-9]+)?", re.ASCII)
# Binary doubles carry 15 significant decimal digits faithfully; beyond that a JSON number is ambiguous.
_FLOAT_SAFE_DIGITS: Final = 15


def _metadata_depth(value: object) -> int:
    """Nesting depth of JSON containers (``{}`` alone is 1, ``{"a": {}}`` is 2), computed iteratively."""
    deepest = 0
    stack: list[tuple[object, int]] = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        if isinstance(item, dict):
            children: list[object] = list(item.values())
        elif isinstance(item, list):
            children = list(item)
        else:
            continue
        deepest = max(deepest, depth)
        stack.extend((child, depth + 1) for child in children)
    return deepest


def _check_webhook_url_syntax(url: str) -> str:
    """Syntax-level checks only; the SSRF/private-network policy is enforced by ``common.netguard``."""
    if any(char.isspace() or not char.isprintable() for char in url):
        msg = "webhook_url must not contain whitespace or control characters"
        raise ValueError(msg)
    try:
        parts = urlsplit(url)
        _ = parts.port  # raises ValueError for an out-of-range or non-numeric port
    except ValueError as exc:
        msg = "webhook_url is not a valid URL"
        raise ValueError(msg) from exc
    if parts.scheme not in {"http", "https"}:
        msg = "webhook_url must use the http or https scheme"
        raise ValueError(msg)
    if not parts.hostname:
        msg = "webhook_url must contain a host"
        raise ValueError(msg)
    if "@" in parts.netloc:
        msg = "webhook_url must not contain credentials"
        raise ValueError(msg)
    return url


class CreatePaymentRequest(BaseModel):
    """Body of ``POST /api/v1/payments``."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "amount": "100.50",
                    "currency": "RUB",
                    "description": "Order #42",
                    "metadata": {"order_id": 42},
                    "webhook_url": "https://client.example.com/hooks/payments",
                }
            ]
        },
    )

    amount: Annotated[
        Decimal,
        Field(
            gt=0,
            max_digits=18,
            decimal_places=2,
            description=(
                "Positive amount with at most two decimal places and at most 18 digits in total. "
                "Send it as a string; a JSON number is accepted only up to 15 significant digits."
            ),
            examples=["100.50"],
            # pydantic can not express digit counts in JSON Schema; spell the string form out.
            json_schema_extra={
                "anyOf": [
                    {"type": "number", "exclusiveMinimum": 0},
                    {"type": "string", "pattern": r"^[0-9]{1,16}(\.[0-9]{1,2})?$"},
                ]
            },
        ),
    ]
    currency: Annotated[Currency, Field(description="ISO 4217 code of the currency.", examples=["RUB"])]
    description: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=MAX_DESCRIPTION_LENGTH),
        Field(description="Human readable purpose of the payment (1-1000 characters, not blank)."),
    ]
    metadata: Annotated[
        dict[str, Any],
        Field(
            default_factory=dict,
            description=(
                f"Free-form JSON object kept with the payment. At most {MAX_METADATA_BYTES} bytes as "
                f"compact UTF-8 JSON and at most {MAX_METADATA_DEPTH} levels of nesting."
            ),
        ),
    ]
    webhook_url: Annotated[
        str,
        Field(
            max_length=MAX_WEBHOOK_URL_LENGTH,
            description=(
                "Where the result is delivered with a signed POST. Must be an http(s) URL with a host and "
                "without credentials; private and loopback targets are rejected unless explicitly allowed."
            ),
            examples=["https://client.example.com/hooks/payments"],
        ),
    ]

    @field_validator("amount", mode="before")
    @classmethod
    def _reject_ambiguous_amount(cls, value: object) -> object:
        if isinstance(value, float):
            if not math.isfinite(value):
                msg = "amount must be a finite number"
                raise ValueError(msg)
            if len(Decimal(repr(value)).as_tuple().digits) > _FLOAT_SAFE_DIGITS:
                msg = "numbers with more than 15 significant digits must be sent as a string"
                raise ValueError(msg)
        elif isinstance(value, str) and not _PLAIN_DECIMAL.fullmatch(value):
            msg = 'amount must be a plain decimal number such as "100.50"'
            raise ValueError(msg)
        return value

    @field_validator("amount", mode="after")
    @classmethod
    def _fit_numeric_column(cls, value: Decimal) -> Decimal:
        if value >= _AMOUNT_LIMIT:
            msg = "amount is too large"
            raise ValueError(msg)
        return value.quantize(_CENT)

    @field_validator("metadata", mode="after")
    @classmethod
    def _limit_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        # Depth first: it bounds the recursion json.dumps needs below.
        if _metadata_depth(value) > MAX_METADATA_DEPTH:
            msg = f"metadata must not be nested deeper than {MAX_METADATA_DEPTH} levels"
            raise ValueError(msg)
        try:
            encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError) as exc:
            msg = "metadata must be plain JSON"
            raise ValueError(msg) from exc
        if len(encoded.encode("utf-8")) > MAX_METADATA_BYTES:
            msg = f"metadata must not exceed {MAX_METADATA_BYTES} bytes when serialised as JSON"
            raise ValueError(msg)
        return value

    @field_validator("webhook_url", mode="after")
    @classmethod
    def _check_webhook_url(cls, value: str) -> str:
        return _check_webhook_url_syntax(value)

    def to_command(self, *, webhook_url: str | None = None) -> CreatePaymentCommand:
        """Build the use-case input; ``webhook_url`` replaces the raw URL with its normalised form."""
        return CreatePaymentCommand(
            amount=self.amount,
            currency=self.currency,
            description=self.description,
            metadata=self.metadata,
            webhook_url=self.webhook_url if webhook_url is None else webhook_url,
        )


class PaymentAcceptedResponse(BaseModel):
    """Body of the ``202 Accepted`` answer: the payment exists and will be processed asynchronously."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "payment_id": "0b5f3d0e-6a77-4c41-9a43-2f9e1c8f7a10",
                    "status": "pending",
                    "created_at": "2026-01-15T10:30:00Z",
                }
            ]
        }
    )

    payment_id: Annotated[UUID, Field(description="Identifier of the payment.")]
    status: Annotated[PaymentStatus, Field(description="Current status; ``pending`` right after creation.")]
    created_at: Annotated[datetime, Field(description="Creation time (UTC).")]

    @classmethod
    def from_dto(cls, payment: PaymentDTO) -> Self:
        """Project the read model onto the 202 body."""
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentResponse(BaseModel):
    """Body of ``GET /api/v1/payments/{payment_id}``."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "payment_id": "0b5f3d0e-6a77-4c41-9a43-2f9e1c8f7a10",
                    "amount": "100.50",
                    "currency": "RUB",
                    "description": "Order #42",
                    "metadata": {"order_id": 42},
                    "status": "succeeded",
                    "webhook_url": "https://client.example.com/hooks/payments",
                    "created_at": "2026-01-15T10:30:00Z",
                    "processed_at": "2026-01-15T10:30:04Z",
                }
            ]
        }
    )

    payment_id: Annotated[UUID, Field(description="Identifier of the payment.")]
    amount: Annotated[Decimal, Field(description="Amount with exactly two decimals, serialised as a string.")]
    currency: Annotated[Currency, Field(description="ISO 4217 code of the currency.")]
    description: str
    metadata: dict[str, Any]
    status: Annotated[PaymentStatus, Field(description="``pending``, ``succeeded`` or ``failed``.")]
    webhook_url: str
    created_at: Annotated[datetime, Field(description="Creation time (UTC).")]
    processed_at: Annotated[
        datetime | None, Field(description="When the final status was set; null while pending.")
    ]

    @field_serializer("amount", when_used="json")
    def _serialise_amount(self, value: Decimal) -> str:
        return f"{value:.2f}"

    @classmethod
    def from_dto(cls, payment: PaymentDTO) -> Self:
        """Project the read model onto the public representation."""
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata,
            status=payment.status,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class ValidationIssue(BaseModel):
    """One problem found in the request."""

    loc: Annotated[list[str | int], Field(description="Path to the offending part of the request.")]
    msg: Annotated[str, Field(description="Human readable explanation; never contains the submitted value.")]
    type: Annotated[str, Field(description="Machine readable error type.")]


class ProblemDetails(BaseModel):
    """RFC 9457 problem document returned for every error (``application/problem+json``)."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "type": "https://payment-service/errors/payment-not-found",
                    "title": "Payment not found",
                    "status": 404,
                    "detail": "Payment not found",
                    "code": "payment-not-found",
                    "request_id": "3f2b8c1d4e5f4a6b9c0d1e2f3a4b5c6d",
                }
            ]
        }
    )

    type: Annotated[str, Field(description="URI identifying the problem type.")]
    title: Annotated[str, Field(description="Short summary, the same for every occurrence of the type.")]
    status: Annotated[int, Field(description="HTTP status code.")]
    detail: Annotated[str, Field(description="Explanation specific to this occurrence.")]
    code: Annotated[str, Field(description="Stable machine readable error code (kebab-case).")]
    request_id: Annotated[str | None, Field(description="Value of the X-Request-ID response header.")] = None
    errors: Annotated[
        list[ValidationIssue] | None,
        Field(description="Per-field problems; present only for ``validation-error``."),
    ] = None


class HealthResponse(BaseModel):
    """Body of the health probes."""

    status: Literal["ok"] = "ok"

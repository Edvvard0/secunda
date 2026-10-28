"""Payment endpoints (mounted under ``/api/v1`` and protected by ``X-API-Key``)."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID  # noqa: TC003 - FastAPI resolves the annotation at runtime

from fastapi import APIRouter, Header, Path, Request, Response

from payment_service.api.dependencies import (  # noqa: TC001
    PaymentServiceDep,
    RequestIdDep,
    SettingsDep,
)
from payment_service.api.schemas import (
    CreatePaymentRequest,
    PaymentAcceptedResponse,
    PaymentResponse,
    ProblemDetails,
)
from payment_service.common.netguard import UnsafeTargetError, validate_webhook_url
from payment_service.domain.exceptions import InvalidWebhookUrlError

router = APIRouter(prefix="/payments", tags=["payments"])

_UNAUTHORIZED = {"model": ProblemDetails, "description": "The X-API-Key header is missing or wrong."}
_VALIDATION_FAILED = {"model": ProblemDetails, "description": "The request was rejected (see `code`)."}


@router.post(
    "",
    name="create_payment",
    operation_id="createPayment",
    summary="Create a payment",
    description=(
        "Accepts a payment for asynchronous processing and answers immediately with `202 Accepted`; the "
        "final status is delivered to `webhook_url` and can be polled with `GET /payments/{payment_id}`.\n\n"
        "The `Idempotency-Key` header makes the call safe to retry: repeating the request with the same key "
        "and the same body returns the original payment with `Idempotent-Replayed: true`, while the same key "
        "with a different body is rejected with `422` (`idempotency-key-reused`)."
    ),
    status_code=202,
    response_model=PaymentAcceptedResponse,
    responses={
        202: {
            "description": "The payment was accepted (or an earlier identical request is being replayed).",
            "headers": {
                "Location": {
                    "description": "URL of the created payment.",
                    "schema": {
                        "type": "string",
                        "examples": ["/api/v1/payments/0b5f3d0e-6a77-4c41-9a43-2f9e1c8f7a10"],
                    },
                },
                "Idempotent-Replayed": {
                    "description": "`true` when the response replays an earlier request with the same key.",
                    "schema": {"type": "string", "enum": ["true"]},
                },
            },
        },
        400: {
            "model": ProblemDetails,
            "description": "The Idempotency-Key header is missing, blank or invalid.",
        },
        401: _UNAUTHORIZED,
        413: {
            "model": ProblemDetails,
            "description": "The request body is larger than the configured limit (`API_MAX_BODY_BYTES`).",
        },
        422: _VALIDATION_FAILED,
    },
)
async def create_payment(
    payload: CreatePaymentRequest,
    *,
    request: Request,
    response: Response,
    service: PaymentServiceDep,
    settings: SettingsDep,
    request_id: RequestIdDep,
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            description=(
                "Required. Client generated unique key of this operation: 1-255 printable ASCII characters "
                "without spaces. A missing or invalid key is rejected with 400."
            ),
            examples=["8d1f3c52-6b0e-4d54-9f55-0a1b2c3d4e5f"],
        ),
    ] = None,
) -> PaymentAcceptedResponse:
    """Validate the webhook target, then create the payment through the use case."""
    try:
        webhook_url = validate_webhook_url(
            payload.webhook_url, allow_private_networks=settings.webhook.allow_private_networks
        )
    except UnsafeTargetError as exc:
        raise InvalidWebhookUrlError(str(exc)) from exc

    # A missing header is passed on as "" so that the use case rejects it with the one canonical error.
    result = await service.create(
        payload.to_command(webhook_url=webhook_url),
        idempotency_key=idempotency_key or "",
        correlation_id=request_id,
    )

    response.headers["Location"] = str(request.app.url_path_for("get_payment", payment_id=result.payment.id))
    if not result.created:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAcceptedResponse.from_dto(result.payment)


@router.get(
    "/{payment_id}",
    name="get_payment",
    operation_id="getPayment",
    summary="Get a payment",
    description="Returns the payment with its current status. `processed_at` is null while it is pending.",
    response_model=PaymentResponse,
    responses={
        200: {"description": "The payment."},
        401: _UNAUTHORIZED,
        404: {"model": ProblemDetails, "description": "There is no payment with this id."},
        422: {"model": ProblemDetails, "description": "`payment_id` is not a valid UUID."},
    },
)
async def get_payment(
    payment_id: Annotated[UUID, Path(description="Identifier returned when the payment was created.")],
    service: PaymentServiceDep,
) -> PaymentResponse:
    """Return one payment."""
    return PaymentResponse.from_dto(await service.get(payment_id))

"""Rendering of RFC 9457 problem documents, shared by the error handlers and the middleware.

Kept apart from ``errors`` so that middleware can answer with a problem document (e.g. ``413``) without
importing the exception handlers, which themselves depend on the middleware.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

from fastapi.responses import JSONResponse

if TYPE_CHECKING:
    from collections.abc import Mapping

    from payment_service.domain.exceptions import AppError

PROBLEM_JSON_MEDIA_TYPE: Final = "application/problem+json"
ERROR_TYPE_BASE_URL: Final = "https://payment-service/errors/"
REQUEST_ID_HEADER: Final = "X-Request-ID"


def problem_response(
    *,
    status_code: int,
    code: str,
    title: str,
    detail: str,
    request_id: str | None,
    headers: Mapping[str, str] | None = None,
    errors: list[dict[str, Any]] | None = None,
) -> JSONResponse:
    """Build an RFC 9457 problem document response."""
    body: dict[str, Any] = {
        "type": f"{ERROR_TYPE_BASE_URL}{code}",
        "title": title,
        "status": status_code,
        "detail": detail,
        "code": code,
    }
    response_headers = dict(headers or {})
    if request_id is not None:
        body["request_id"] = request_id
        # The request-id middleware sets it too; repeating it keeps responses built outside of it
        # (the 500 produced by ServerErrorMiddleware) correlated.
        response_headers[REQUEST_ID_HEADER] = request_id
    if errors is not None:
        body["errors"] = errors
    return JSONResponse(
        body, status_code=status_code, headers=response_headers, media_type=PROBLEM_JSON_MEDIA_TYPE
    )


def app_error_response(exc: AppError, *, request_id: str | None) -> JSONResponse:
    """Render an ``AppError`` from its class attributes (status, code, title) and its detail."""
    return problem_response(
        status_code=exc.status_code,
        code=exc.code,
        title=exc.title,
        detail=exc.detail,
        request_id=request_id,
    )

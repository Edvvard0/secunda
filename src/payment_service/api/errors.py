"""Error rendering: every failure leaves the API as RFC 9457 ``application/problem+json``.

``AppError`` subclasses are rendered generically from their class attributes, so introducing a new
domain error never requires touching this module. Nothing the client sent is echoed back in an error
and internal failures are reported as a bare ``500``; details only go to the server log.
"""

from __future__ import annotations

import logging
import re
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Final, cast

from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from payment_service.api.middleware import request_id_from_scope
from payment_service.api.problem import app_error_response, problem_response
from payment_service.domain.exceptions import AppError
from payment_service.logging_config import set_correlation_id

if TYPE_CHECKING:
    from collections.abc import Callable

    from fastapi import FastAPI, Request
    from starlette.responses import Response
    from starlette.types import HTTPExceptionHandler

logger = logging.getLogger(__name__)

_NON_SLUG_CHARS: Final = re.compile(r"[^a-z0-9]+")
_INTERNAL_ERROR_DETAIL: Final = "An unexpected error occurred. Quote the request_id when reporting it."
# pydantic puts the offending character of a malformed UUID into the message, so it is replaced.
_GENERIC_MESSAGES: Final = {"uuid_parsing": "Input should be a valid UUID"}
_VALUE_ERROR_PREFIX: Final = "Value error, "  # pydantic prepends it to messages of our own validators


async def _handle_app_error(request: Request, exc: AppError) -> Response:
    return app_error_response(exc, request_id=request_id_from_scope(request.scope))


async def _handle_validation_error(request: Request, exc: RequestValidationError) -> Response:
    # Only location, message and error type: the "input" key of pydantic errors would echo the payload.
    errors = [
        {
            "loc": list(error["loc"]),
            "msg": _GENERIC_MESSAGES.get(error["type"], error["msg"].removeprefix(_VALUE_ERROR_PREFIX)),
            "type": error["type"],
        }
        for error in exc.errors()
    ]
    return problem_response(
        status_code=422,
        code="validation-error",
        title="Request validation failed",
        detail="The request is syntactically valid but its content is not acceptable.",
        request_id=request_id_from_scope(request.scope),
        errors=errors,
    )


async def _handle_http_exception(request: Request, exc: StarletteHTTPException) -> Response:
    try:
        phrase = HTTPStatus(exc.status_code).phrase
    except ValueError:
        phrase = "Error"
    return problem_response(
        status_code=exc.status_code,
        code=_NON_SLUG_CHARS.sub("-", phrase.lower()).strip("-"),
        title=phrase,
        detail=str(exc.detail),
        request_id=request_id_from_scope(request.scope),
        headers=exc.headers,
    )


async def _handle_unhandled_exception(request: Request, exc: Exception) -> Response:
    request_id = request_id_from_scope(request.scope)
    # ServerErrorMiddleware sits outside RequestContextMiddleware, which has already cleared the context.
    set_correlation_id(request_id)
    try:
        logger.error("Unhandled exception while processing the request", exc_info=exc)
    finally:
        set_correlation_id(None)
    return problem_response(
        status_code=500,
        code="internal-error",
        title="Internal server error",
        detail=_INTERNAL_ERROR_DETAIL,
        request_id=request_id,
    )


def _register[E: Exception](
    app: FastAPI, exception_class: type[E], handler: Callable[[Request, E], Any]
) -> None:
    # Starlette types handlers as taking ``Exception``; it dispatches by class, so the narrower
    # parameter type is safe.
    app.add_exception_handler(exception_class, cast("HTTPExceptionHandler", handler))


def register_error_handlers(app: FastAPI) -> None:
    """Install the problem+json handlers on ``app``."""
    _register(app, AppError, _handle_app_error)
    _register(app, RequestValidationError, _handle_validation_error)
    _register(app, StarletteHTTPException, _handle_http_exception)
    _register(app, Exception, _handle_unhandled_exception)

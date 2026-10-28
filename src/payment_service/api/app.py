"""Application factory of the HTTP API.

Run with ``uvicorn payment_service.api.app:create_app --factory``. Tests and tooling inject fakes for the
service and the database, so the factory never opens a connection by itself in that case.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Final

from fastapi import FastAPI

from payment_service import __version__
from payment_service.api.dependencies import DatabaseLike, PaymentServiceLike, api_key_dependency
from payment_service.api.errors import register_error_handlers
from payment_service.api.middleware import MaxBodySizeMiddleware, RequestContextMiddleware
from payment_service.api.routes import health, payments
from payment_service.config import Settings
from payment_service.logging_config import configure_logging
from payment_service.persistence import Database
from payment_service.services.payments import PaymentService

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

API_PREFIX: Final = "/api/v1"

_FIRST_ERROR_STATUS: Final = 400

_DESCRIPTION: Final = """\
Asynchronous payment processing service.

* `POST /api/v1/payments` accepts a payment and answers `202` right away; the result is delivered to the
  client's `webhook_url` as a signed POST, at least once.
* Every request may carry an `X-Request-ID` (`[A-Za-z0-9._-]`, up to 64 characters); the id actually used is
  returned in the `X-Request-ID` response header and in the `request_id` field of every error.
* Errors are `application/problem+json` documents (RFC 9457).
"""

_TAGS: Final = [
    {"name": "payments", "description": "Create payments and read their status. Requires `X-API-Key`."},
    {"name": "health", "description": "Liveness and readiness probes. No authentication."},
]


def _use_problem_media_type(schema: dict[str, Any]) -> None:
    """Document error bodies as ``application/problem+json`` (FastAPI labels model responses as JSON)."""
    for path_item in schema.get("paths", {}).values():
        for operation in path_item.values():
            for status, response in operation.get("responses", {}).items():
                content = response.get("content")
                if (
                    status.isdigit()
                    and int(status) >= _FIRST_ERROR_STATUS
                    and content
                    and "application/json" in content
                ):
                    content["application/problem+json"] = content.pop("application/json")


def create_app(
    settings: Settings | None = None,
    *,
    payment_service: PaymentServiceLike | None = None,
    database: DatabaseLike | None = None,
) -> FastAPI:
    """Build the FastAPI application.

    Args:
        settings: Configuration; loaded from the environment when omitted (``uvicorn --factory``).
        payment_service: Injected use cases. When omitted they are built on the application's own
            ``Database`` during startup.
        database: Injected database handle (readiness probe). It is *not* disposed by the application:
            whoever created it owns it. Requires ``payment_service`` to be injected as well.

    Raises:
        ValueError: if ``database`` is injected without ``payment_service``.
    """
    if database is not None and payment_service is None:
        msg = "payment_service must be injected together with a custom database"
        raise ValueError(msg)

    settings = settings or Settings.load()
    configure_logging(settings.logging)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned_database: Database | None = None
        if database is None:
            owned_database = Database(settings.db)
            app.state.database = owned_database
            if payment_service is None:
                app.state.payment_service = PaymentService(owned_database.unit_of_work)
        try:
            yield
        finally:
            if owned_database is not None:
                await owned_database.dispose()

    docs_enabled = settings.api.docs_enabled
    app = FastAPI(
        title="Payment Service API",
        description=_DESCRIPTION,
        version=__version__,
        openapi_tags=_TAGS,
        docs_url="/docs" if docs_enabled else None,
        redoc_url="/redoc" if docs_enabled else None,
        openapi_url="/openapi.json" if docs_enabled else None,
        lifespan=lifespan,
    )
    # ASGI test clients do not run the lifespan, so injected collaborators are attached right away.
    app.state.settings = settings
    if database is not None:
        app.state.database = database
    if payment_service is not None:
        app.state.payment_service = payment_service

    # Starlette wraps in reverse order of registration: the request-id middleware must be outermost so
    # that even the 413 produced by the size limit carries X-Request-ID and shows up in the access log.
    app.add_middleware(MaxBodySizeMiddleware, max_body_bytes=settings.api.max_body_bytes)
    app.add_middleware(RequestContextMiddleware)
    register_error_handlers(app)
    app.include_router(
        payments.router,
        prefix=API_PREFIX,
        dependencies=[api_key_dependency(settings.api.key)],
    )
    app.include_router(health.router)

    original_openapi = app.openapi

    def openapi() -> dict[str, Any]:
        schema = original_openapi()
        _use_problem_media_type(schema)
        return schema

    app.openapi = openapi  # type: ignore[method-assign]  # the documented way to post-process the schema
    return app

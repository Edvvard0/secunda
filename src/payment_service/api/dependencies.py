"""``Depends`` providers and the narrow interfaces the HTTP layer needs from the rest of the application.

The routes depend on the two small protocols below rather than on the concrete ``PaymentService`` and
``Database``, so tests (and the OpenAPI exporter) can plug in fakes without any infrastructure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, Protocol

from fastapi import Depends, Request

from payment_service.api.middleware import request_id_from_scope
from payment_service.api.security import ApiKeyAuth
from payment_service.config import Settings
from payment_service.services.dto import CreatePaymentCommand, CreatePaymentResult, PaymentDTO  # noqa: TC001

if TYPE_CHECKING:
    from uuid import UUID

    from pydantic import SecretStr


class PaymentServiceLike(Protocol):
    """What the routes use of ``services.payments.PaymentService``."""

    async def create(
        self,
        command: CreatePaymentCommand,
        *,
        idempotency_key: str,
        correlation_id: str | None = None,
    ) -> CreatePaymentResult:
        """Create a payment (or replay an earlier one with the same idempotency key)."""
        ...

    async def get(self, payment_id: UUID) -> PaymentDTO:
        """Return a payment or raise ``PaymentNotFoundError``."""
        ...


class DatabaseLike(Protocol):
    """What the API needs from ``persistence.Database``: a readiness probe and cleanup."""

    async def ping(self) -> None:
        """Raise if the database is unreachable."""
        ...

    async def dispose(self) -> None:
        """Release all connections."""
        ...


def get_settings(request: Request) -> Settings:
    """Settings the application was created with."""
    settings: Settings = request.app.state.settings
    return settings


def get_payment_service(request: Request) -> PaymentServiceLike:
    """The payment use cases, owned by the application (``app.state``)."""
    service: PaymentServiceLike = request.app.state.payment_service
    return service


def get_database(request: Request) -> DatabaseLike:
    """The database handle, used by the readiness probe."""
    database: DatabaseLike = request.app.state.database
    return database


def get_request_id(request: Request) -> str | None:
    """Id assigned to this request by ``RequestContextMiddleware``."""
    return request_id_from_scope(request.scope)


def api_key_dependency(api_key: SecretStr) -> Any:
    """Router level dependency that enforces ``X-API-Key`` against ``api_key``."""
    return Depends(ApiKeyAuth(api_key))


SettingsDep = Annotated[Settings, Depends(get_settings)]
PaymentServiceDep = Annotated[PaymentServiceLike, Depends(get_payment_service)]
DatabaseDep = Annotated[DatabaseLike, Depends(get_database)]
RequestIdDep = Annotated[str | None, Depends(get_request_id)]

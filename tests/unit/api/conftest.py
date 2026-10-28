"""Fixtures for the HTTP API tests: the real app wired to in-memory fakes, driven through ASGI."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import pytest_asyncio

from payment_service.api.app import create_app
from payment_service.common.time import utcnow
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.exceptions import IdempotencyKeyReuseError, PaymentNotFoundError
from payment_service.logging_config import set_correlation_id
from payment_service.services.dto import CreatePaymentCommand, CreatePaymentResult, PaymentDTO
from payment_service.services.payments import validate_idempotency_key

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

    from fastapi import FastAPI

    from payment_service.config import Settings

BASE_URL = "http://testserver"


@dataclass(frozen=True)
class RecordedCall:
    """One call of ``FakePaymentService.create``."""

    command: CreatePaymentCommand
    idempotency_key: str
    correlation_id: str | None


class FakePaymentService:
    """In-memory stand-in for ``PaymentService`` with the same observable behaviour at the HTTP boundary.

    Key validation is the real ``validate_idempotency_key``; replay and key-reuse detection use the real
    ``CreatePaymentCommand.fingerprint``. Only the persistence is replaced by dictionaries.
    """

    def __init__(self) -> None:
        self.calls: list[RecordedCall] = []
        self.failure: Exception | None = None
        self._by_key: dict[str, tuple[str, PaymentDTO]] = {}
        self._by_id: dict[uuid.UUID, PaymentDTO] = {}

    def add_payment(self, **overrides: Any) -> PaymentDTO:
        """Store a ready-made payment (for the GET tests)."""
        values: dict[str, Any] = {
            "id": uuid.uuid4(),
            "amount": Decimal("100.50"),
            "currency": Currency.RUB,
            "description": "Order #42",
            "metadata": {"order_id": 42},
            "status": PaymentStatus.PENDING,
            "idempotency_key": uuid.uuid4().hex,
            "webhook_url": "https://client.example.com/hooks/payments",
            "created_at": utcnow(),
            "processed_at": None,
            "webhook_delivered_at": None,
        }
        values.update(overrides)
        payment = PaymentDTO(**values)
        self._by_id[payment.id] = payment
        return payment

    async def create(
        self,
        command: CreatePaymentCommand,
        *,
        idempotency_key: str,
        correlation_id: str | None = None,
    ) -> CreatePaymentResult:
        self.calls.append(RecordedCall(command, idempotency_key, correlation_id))
        if self.failure is not None:
            raise self.failure
        validate_idempotency_key(idempotency_key)
        fingerprint = command.fingerprint()
        known = self._by_key.get(idempotency_key)
        if known is not None:
            known_fingerprint, payment = known
            if known_fingerprint != fingerprint:
                raise IdempotencyKeyReuseError
            return CreatePaymentResult(payment, created=False)
        payment = self.add_payment(
            amount=command.amount,
            currency=command.currency,
            description=command.description,
            metadata=command.metadata,
            idempotency_key=idempotency_key,
            webhook_url=command.webhook_url,
        )
        self._by_key[idempotency_key] = (fingerprint, payment)
        return CreatePaymentResult(payment, created=True)

    async def get(self, payment_id: uuid.UUID) -> PaymentDTO:
        if self.failure is not None:
            raise self.failure
        try:
            return self._by_id[payment_id]
        except KeyError:
            msg = f"Payment {payment_id} not found"
            raise PaymentNotFoundError(msg) from None


class FakeDatabase:
    """Records readiness probes and disposal; ``ping_error`` makes the probe fail."""

    def __init__(self) -> None:
        self.ping_error: Exception | None = None
        self.pings = 0
        self.disposed = 0

    async def ping(self) -> None:
        self.pings += 1
        if self.ping_error is not None:
            raise self.ping_error

    async def dispose(self) -> None:
        self.disposed += 1


@pytest.fixture(autouse=True)
def _restore_logging_state() -> Iterator[None]:
    """``create_app`` configures logging process-wide; undo it so tests stay independent."""
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    watched = [logging.getLogger(name) for name in ("uvicorn", "uvicorn.error", "uvicorn.access")]
    saved = [(lg, list(lg.handlers), lg.propagate) for lg in watched]
    quiet = [(lg, lg.level) for lg in (logging.getLogger("httpx"), logging.getLogger("httpcore"))]
    yield
    root.handlers[:] = handlers
    root.setLevel(level)
    for library, library_level in quiet:
        library.setLevel(library_level)
    for named, named_handlers, propagate in saved:
        named.handlers[:] = named_handlers
        named.propagate = propagate
    set_correlation_id(None)


@pytest.fixture
def settings(make_settings: Callable[..., Settings]) -> Settings:
    return make_settings()


@pytest.fixture
def api_key(settings: Settings) -> str:
    return settings.api.key.get_secret_value()


@pytest.fixture
def fake_service() -> FakePaymentService:
    return FakePaymentService()


@pytest.fixture
def fake_database() -> FakeDatabase:
    return FakeDatabase()


@pytest.fixture
def app(settings: Settings, fake_service: FakePaymentService, fake_database: FakeDatabase) -> FastAPI:
    return create_app(settings, payment_service=fake_service, database=fake_database)


@pytest.fixture
def make_client() -> Callable[[FastAPI], httpx.AsyncClient]:
    """Client factory. ``raise_app_exceptions=False`` makes crashes visible as 500 responses, like in prod."""

    def factory(app: FastAPI, **kwargs: Any) -> httpx.AsyncClient:
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        return httpx.AsyncClient(transport=transport, base_url=BASE_URL, **kwargs)

    return factory


@pytest_asyncio.fixture
async def client(
    app: FastAPI, api_key: str, make_client: Callable[..., httpx.AsyncClient]
) -> AsyncIterator[httpx.AsyncClient]:
    """Authenticated client."""
    async with make_client(app, headers={"X-API-Key": api_key}) as http_client:
        yield http_client


@pytest_asyncio.fixture
async def strict_client(
    make_settings: Callable[..., Settings],
    fake_service: FakePaymentService,
    fake_database: FakeDatabase,
    make_client: Callable[..., httpx.AsyncClient],
) -> AsyncIterator[httpx.AsyncClient]:
    """Authenticated client of an app that refuses private webhook targets (the production default)."""
    strict = make_settings(webhook={"allow_private_networks": False})
    app = create_app(strict, payment_service=fake_service, database=fake_database)
    async with make_client(app, headers={"X-API-Key": strict.api.key.get_secret_value()}) as http_client:
        yield http_client


@pytest_asyncio.fixture
async def anonymous_client(
    app: FastAPI, make_client: Callable[..., httpx.AsyncClient]
) -> AsyncIterator[httpx.AsyncClient]:
    """Client that sends no ``X-API-Key``."""
    async with make_client(app) as http_client:
        yield http_client


@pytest.fixture
def payment_body() -> dict[str, Any]:
    """A valid request body (a fresh dict per test: tests mutate it)."""
    return {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://client.example.com/hooks/payments",
    }


@pytest.fixture
def idempotency_headers() -> dict[str, str]:
    return {"Idempotency-Key": "key-0001"}

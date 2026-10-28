from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pytest

from payment_service import __version__
from payment_service.api import app as app_module
from payment_service.api.app import create_app
from payment_service.logging_config import JsonFormatter

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from payment_service.config import Settings


class RecordingDatabase:
    """Stands in for ``Database`` when the application builds its own."""

    instances: list[RecordingDatabase]

    def __init__(self, settings: object) -> None:
        self.settings = settings
        self.disposed = 0
        self.instances.append(self)

    def unit_of_work(self) -> None:
        raise NotImplementedError

    async def ping(self) -> None:
        return None

    async def dispose(self) -> None:
        self.disposed += 1


class RecordingService:
    """Stands in for ``PaymentService``; remembers the factory it was built with."""

    def __init__(self, uow_factory: Callable[[], Any]) -> None:
        self.uow_factory = uow_factory


@pytest.fixture
def built(monkeypatch: pytest.MonkeyPatch) -> list[RecordingDatabase]:
    instances: list[RecordingDatabase] = []
    RecordingDatabase.instances = instances
    monkeypatch.setattr(app_module, "Database", RecordingDatabase)
    monkeypatch.setattr(app_module, "PaymentService", RecordingService)
    return instances


def test_app_metadata_comes_from_the_package(
    settings: Settings, fake_service: Any, fake_database: Any
) -> None:
    app = create_app(settings, payment_service=fake_service, database=fake_database)

    assert app.title == "Payment Service API"
    assert app.version == __version__
    assert app.docs_url == "/docs"
    assert app.redoc_url == "/redoc"
    assert app.openapi_url == "/openapi.json"


def test_documentation_urls_are_switched_off_by_the_setting(
    make_settings: Callable[..., Settings], fake_service: Any, fake_database: Any
) -> None:
    app = create_app(
        make_settings(api={"docs_enabled": False}), payment_service=fake_service, database=fake_database
    )

    assert (app.docs_url, app.redoc_url, app.openapi_url) == (None, None, None)


def test_injected_collaborators_are_available_without_running_the_lifespan(
    settings: Settings, fake_service: Any, fake_database: Any
) -> None:
    app = create_app(settings, payment_service=fake_service, database=fake_database)

    assert app.state.payment_service is fake_service
    assert app.state.database is fake_database
    assert app.state.settings is settings


def test_a_database_without_a_service_is_a_programming_error(settings: Settings, fake_database: Any) -> None:
    with pytest.raises(ValueError, match="payment_service must be injected"):
        create_app(settings, database=fake_database)


async def test_lifespan_builds_the_database_and_the_service_and_disposes_the_database(
    settings: Settings, built: list[RecordingDatabase]
) -> None:
    app = create_app(settings)

    async with app.router.lifespan_context(app):
        (database,) = built
        assert app.state.database is database
        assert database.settings is settings.db
        assert isinstance(app.state.payment_service, RecordingService)
        assert app.state.payment_service.uow_factory == database.unit_of_work
        assert database.disposed == 0

    assert database.disposed == 1


async def test_lifespan_disposes_the_database_even_when_the_server_fails(
    settings: Settings, built: list[RecordingDatabase]
) -> None:
    app = create_app(settings)

    with pytest.raises(RuntimeError, match="server crashed"):
        async with app.router.lifespan_context(app):
            raise RuntimeError("server crashed")

    assert built[0].disposed == 1


async def test_lifespan_keeps_an_injected_service_and_builds_only_the_database(
    settings: Settings, fake_service: Any, built: list[RecordingDatabase]
) -> None:
    app = create_app(settings, payment_service=fake_service)

    async with app.router.lifespan_context(app):
        assert app.state.payment_service is fake_service
        assert app.state.database is built[0]

    assert built[0].disposed == 1


async def test_lifespan_leaves_injected_collaborators_to_their_owner(
    settings: Settings, fake_service: Any, fake_database: Any, built: list[RecordingDatabase]
) -> None:
    app = create_app(settings, payment_service=fake_service, database=fake_database)

    async with app.router.lifespan_context(app):
        pass

    assert built == []
    assert fake_database.disposed == 0


def test_settings_are_loaded_from_the_environment_when_not_given(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_service: Any, fake_database: Any
) -> None:
    monkeypatch.chdir(tmp_path)  # no stray .env
    monkeypatch.setenv("DB_URL", "postgresql+asyncpg://u:p@db:5432/x")
    monkeypatch.setenv("RABBIT_URL", "amqp://u:p@mq:5672/")
    monkeypatch.setenv("API_KEY", "env-api-key-0123456789")
    monkeypatch.setenv("WEBHOOK_SIGNING_SECRET", "env-signing-secret-0123456789")
    monkeypatch.setenv("API_DOCS_ENABLED", "false")

    app = create_app(payment_service=fake_service, database=fake_database)

    assert app.state.settings.api.key.get_secret_value() == "env-api-key-0123456789"
    assert app.openapi_url is None


def test_missing_required_settings_fail_fast(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("DB_URL", "RABBIT_URL", "API_KEY", "WEBHOOK_SIGNING_SECRET"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError, match="Field required"):
        create_app()


def test_creating_the_app_configures_logging_from_the_settings(
    make_settings: Callable[..., Settings], fake_service: Any, fake_database: Any
) -> None:
    create_app(
        make_settings(logging={"level": "DEBUG", "json_format": True}),
        payment_service=fake_service,
        database=fake_database,
    )

    root = logging.getLogger()
    assert root.level == logging.DEBUG
    assert any(isinstance(handler.formatter, JsonFormatter) for handler in root.handlers)

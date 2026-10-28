"""Export the OpenAPI document to ``docs/openapi.json``.

Usage: ``python -m payment_service.tools.export_openapi [path]``.

The application is built with throw-away settings and inert collaborators, so the export needs no
environment, database or broker and is byte-for-byte reproducible. A unit test compares the committed
file with a fresh export and fails when they drift apart.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from pydantic import SecretStr

from payment_service.api.app import create_app
from payment_service.config import (
    ApiSettings,
    DatabaseSettings,
    GatewaySettings,
    LoggingSettings,
    OutboxSettings,
    RabbitSettings,
    RetrySettings,
    Settings,
    WebhookSettings,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from uuid import UUID

    from payment_service.services.dto import CreatePaymentCommand, CreatePaymentResult, PaymentDTO

DEFAULT_OUTPUT: Final = Path("docs") / "openapi.json"

_DUMMY_SECRET: Final = SecretStr("openapi-export-placeholder-secret")


class _InertPaymentService:
    """Stands in for ``PaymentService``; the OpenAPI export never calls an endpoint."""

    async def create(
        self,
        command: CreatePaymentCommand,
        *,
        idempotency_key: str,
        correlation_id: str | None = None,
    ) -> CreatePaymentResult:
        raise NotImplementedError

    async def get(self, payment_id: UUID) -> PaymentDTO:
        raise NotImplementedError


class _InertDatabase:
    """Stands in for ``Database``; nothing is ever connected."""

    async def ping(self) -> None:
        raise NotImplementedError

    async def dispose(self) -> None:
        raise NotImplementedError


def _throwaway_settings() -> Settings:
    # ``_env_file=None``: a developer's local .env must not influence a committed artefact.
    return Settings(
        db=DatabaseSettings(
            _env_file=None, url=SecretStr("postgresql+asyncpg://user:password@localhost:5432/db")
        ),
        rabbit=RabbitSettings(_env_file=None, url=SecretStr("amqp://guest:guest@localhost:5672/")),
        api=ApiSettings(_env_file=None, key=_DUMMY_SECRET, docs_enabled=True),
        webhook=WebhookSettings(_env_file=None, signing_secret=_DUMMY_SECRET),
        retry=RetrySettings(_env_file=None),
        outbox=OutboxSettings(_env_file=None),
        gateway=GatewaySettings(_env_file=None),
        logging=LoggingSettings(_env_file=None),
    )


def build_openapi() -> dict[str, Any]:
    """Return the OpenAPI document of the application."""
    app = create_app(_throwaway_settings(), payment_service=_InertPaymentService(), database=_InertDatabase())
    return app.openapi()


def render_openapi() -> str:
    """Return the OpenAPI document as deterministic, human diffable JSON (sorted keys, 2 spaces, LF)."""
    return json.dumps(build_openapi(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    """Write the document to ``argv[0]`` (default ``docs/openapi.json``) and return the exit code."""
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) > 1:
        sys.stderr.write("usage: python -m payment_service.tools.export_openapi [path]\n")
        return 2
    target = Path(args[0]) if args else DEFAULT_OUTPUT
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_openapi(), encoding="utf-8", newline="\n")
    sys.stdout.write(f"OpenAPI document written to {target}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

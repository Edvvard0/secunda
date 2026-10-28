from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from payment_service.api.routes import health

if TYPE_CHECKING:
    import httpx
    import pytest


async def test_live_answers_ok_without_touching_the_database(
    client: httpx.AsyncClient, fake_database: Any
) -> None:
    response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["content-type"] == "application/json"
    assert fake_database.pings == 0


async def test_ready_answers_ok_when_the_database_responds(
    client: httpx.AsyncClient, fake_database: Any
) -> None:
    response = await client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert fake_database.pings == 1


async def test_ready_answers_503_problem_when_the_ping_fails(
    client: httpx.AsyncClient, fake_database: Any, caplog: pytest.LogCaptureFixture
) -> None:
    fake_database.ping_error = ConnectionError("password=DB-SENTINEL refused")

    response = await client.get("/health/ready", headers={"X-Request-ID": "probe-1"})

    assert response.status_code == 503
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json() == {
        "type": "https://payment-service/errors/service-unavailable",
        "title": "Service temporarily unavailable",
        "status": 503,
        "detail": "The database is not reachable.",
        "code": "service-unavailable",
        "request_id": "probe-1",
    }
    assert "DB-SENTINEL" not in response.text
    assert any(r.name == health.__name__ and r.levelno == logging.WARNING for r in caplog.records)


async def test_ready_recovers_when_the_database_comes_back(
    client: httpx.AsyncClient, fake_database: Any
) -> None:
    fake_database.ping_error = ConnectionError("down")
    assert (await client.get("/health/ready")).status_code == 503

    fake_database.ping_error = None

    assert (await client.get("/health/ready")).status_code == 200


async def test_ready_answers_503_when_the_ping_hangs(
    client: httpx.AsyncClient, fake_database: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def never_returns() -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(health, "_PING_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(fake_database, "ping", never_returns)

    response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["code"] == "service-unavailable"

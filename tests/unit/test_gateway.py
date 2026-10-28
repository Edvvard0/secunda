from __future__ import annotations

import random
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from payment_service.config import GatewaySettings
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.services.dto import PaymentDTO
from payment_service.services.gateway import EmulatedPaymentGateway
from payment_service.services.interfaces import PaymentGateway


class FakeSleep:
    """Records requested delays instead of waiting."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


def make_settings(**overrides: Any) -> GatewaySettings:
    values: dict[str, Any] = {"min_delay_seconds": 2.0, "max_delay_seconds": 5.0, "success_rate": 0.9}
    values.update(overrides)
    return GatewaySettings(**values)


def make_payment() -> PaymentDTO:
    return PaymentDTO(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="test",
        metadata={},
        status=PaymentStatus.PENDING,
        idempotency_key="key",
        webhook_url="https://hooks.example.com/",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        processed_at=None,
        webhook_delivered_at=None,
    )


def test_gateway_implements_the_payment_gateway_port() -> None:
    assert isinstance(EmulatedPaymentGateway(make_settings()), PaymentGateway)


async def test_charge_is_deterministic_with_a_seeded_rng() -> None:
    settings = make_settings()
    expected = random.Random(1234)  # noqa: S311 - reference stream for the assertion
    expected_delay = expected.uniform(2.0, 5.0)
    expected_status = PaymentStatus.SUCCEEDED if expected.random() < 0.9 else PaymentStatus.FAILED
    sleep = FakeSleep()
    gateway = EmulatedPaymentGateway(settings, rng=random.Random(1234), sleep=sleep)  # noqa: S311

    status = await gateway.charge(make_payment())

    assert status is expected_status
    assert sleep.delays == [expected_delay]


async def test_charge_sequence_is_reproducible_across_gateways_with_the_same_seed() -> None:
    async def run(seed: int) -> tuple[list[PaymentStatus], list[float]]:
        sleep = FakeSleep()
        gateway = EmulatedPaymentGateway(
            make_settings(success_rate=0.5),
            rng=random.Random(seed),  # noqa: S311
            sleep=sleep,
        )
        return [await gateway.charge(make_payment()) for _ in range(30)], sleep.delays

    assert await run(7) == await run(7)
    assert await run(7) != await run(8)


async def test_charge_sleeps_within_the_configured_bounds() -> None:
    sleep = FakeSleep()
    gateway = EmulatedPaymentGateway(
        make_settings(min_delay_seconds=0.5, max_delay_seconds=0.75),
        rng=random.Random(42),  # noqa: S311
        sleep=sleep,
    )

    for _ in range(200):
        await gateway.charge(make_payment())

    assert len(sleep.delays) == 200
    assert all(0.5 <= delay <= 0.75 for delay in sleep.delays)
    assert len(set(sleep.delays)) > 1


async def test_charge_with_equal_bounds_sleeps_exactly_that_long() -> None:
    sleep = FakeSleep()
    gateway = EmulatedPaymentGateway(
        make_settings(min_delay_seconds=1.25, max_delay_seconds=1.25), sleep=sleep
    )

    await gateway.charge(make_payment())

    assert sleep.delays == [1.25]


async def test_charge_always_succeeds_when_success_rate_is_one() -> None:
    gateway = EmulatedPaymentGateway(
        make_settings(success_rate=1.0, min_delay_seconds=0, max_delay_seconds=0),
        rng=random.Random(0),  # noqa: S311
        sleep=FakeSleep(),
    )

    statuses = {await gateway.charge(make_payment()) for _ in range(300)}

    assert statuses == {PaymentStatus.SUCCEEDED}


async def test_charge_always_fails_when_success_rate_is_zero() -> None:
    gateway = EmulatedPaymentGateway(
        make_settings(success_rate=0.0, min_delay_seconds=0, max_delay_seconds=0),
        rng=random.Random(0),  # noqa: S311
        sleep=FakeSleep(),
    )

    statuses = {await gateway.charge(make_payment()) for _ in range(300)}

    assert statuses == {PaymentStatus.FAILED}


async def test_charge_success_share_follows_the_success_rate() -> None:
    gateway = EmulatedPaymentGateway(
        make_settings(success_rate=0.9, min_delay_seconds=0, max_delay_seconds=0),
        rng=random.Random(2024),  # noqa: S311
        sleep=FakeSleep(),
    )

    outcomes = [await gateway.charge(make_payment()) for _ in range(2000)]

    share = outcomes.count(PaymentStatus.SUCCEEDED) / len(outcomes)
    assert 0.87 <= share <= 0.93


async def test_charge_never_returns_pending() -> None:
    gateway = EmulatedPaymentGateway(
        make_settings(success_rate=0.5, min_delay_seconds=0, max_delay_seconds=0),
        rng=random.Random(5),  # noqa: S311
        sleep=FakeSleep(),
    )

    outcomes = {await gateway.charge(make_payment()) for _ in range(100)}

    assert outcomes == {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}


async def test_charge_waits_before_deciding() -> None:
    events: list[str] = []

    class Rng(random.Random):
        def uniform(self, a: float, b: float) -> float:
            events.append("pick-delay")
            return 0.0

        def random(self) -> float:
            events.append("decide")
            return 0.0

    async def sleep(seconds: float) -> None:
        events.append("sleep")

    gateway = EmulatedPaymentGateway(make_settings(), rng=Rng(), sleep=sleep)

    await gateway.charge(make_payment())

    assert events == ["pick-delay", "sleep", "decide"]


async def test_charge_uses_the_real_asyncio_sleep_by_default() -> None:
    gateway = EmulatedPaymentGateway(make_settings(min_delay_seconds=0, max_delay_seconds=0))

    assert await gateway.charge(make_payment()) in {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}


def test_init_rejects_inverted_delay_bounds() -> None:
    with pytest.raises(ValueError, match="min_delay_seconds"):
        EmulatedPaymentGateway(make_settings(min_delay_seconds=5.0, max_delay_seconds=2.0))


def test_init_accepts_zero_delays() -> None:
    EmulatedPaymentGateway(make_settings(min_delay_seconds=0, max_delay_seconds=0))


@pytest.mark.parametrize("rate", [-0.1, 1.1])
def test_init_rejects_success_rate_outside_unit_interval(rate: float) -> None:
    settings = GatewaySettings.model_construct(
        min_delay_seconds=0.0, max_delay_seconds=0.0, success_rate=rate
    )

    with pytest.raises(ValueError, match="success_rate"):
        EmulatedPaymentGateway(settings)

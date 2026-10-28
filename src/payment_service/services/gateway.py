"""Payment gateway emulation."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import TYPE_CHECKING

from payment_service.domain.enums import PaymentStatus
from payment_service.services.interfaces import PaymentGateway

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from payment_service.config import GatewaySettings
    from payment_service.services.dto import PaymentDTO

logger = logging.getLogger(__name__)


class EmulatedPaymentGateway(PaymentGateway):
    """Stands in for a real payment service provider (PSP).

    It waits a random time (a PSP round trip) and then declines a configurable share of payments, which
    is enough to exercise both business outcomes and the webhook paths for each of them.

    A real adapter would be a drop-in replacement behind the ``PaymentGateway`` port. Because the
    processor may call ``charge()`` more than once for the same payment (at-least-once delivery, crash
    between the gateway call and the database update), such an adapter must pass ``payment.id`` to the
    provider as its idempotency key, so a repeated call can never charge the customer twice.
    """

    def __init__(
        self,
        settings: GatewaySettings,
        *,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Create the gateway.

        Args:
            settings: Delay bounds and success probability.
            rng: Source of randomness; inject a seeded ``random.Random`` for deterministic tests.
            sleep: Awaitable sleep function; inject a fake to avoid real waiting in tests.

        Raises:
            ValueError: if the delay bounds are inverted or the success rate is not within [0, 1].
        """
        if settings.min_delay_seconds > settings.max_delay_seconds:
            msg = (
                f"min_delay_seconds ({settings.min_delay_seconds}) must not exceed "
                f"max_delay_seconds ({settings.max_delay_seconds})"
            )
            raise ValueError(msg)
        if not 0.0 <= settings.success_rate <= 1.0:
            msg = f"success_rate must be within [0, 1], got {settings.success_rate}"
            raise ValueError(msg)
        self._settings = settings
        # Emulation, not cryptography: the outcome of a fake payment needs no unpredictable randomness.
        self._rng = rng if rng is not None else random.Random()  # noqa: S311
        self._sleep = sleep

    async def charge(self, payment: PaymentDTO) -> PaymentStatus:
        """Wait for the emulated provider latency, then succeed or decline at random."""
        delay = self._rng.uniform(self._settings.min_delay_seconds, self._settings.max_delay_seconds)
        await self._sleep(delay)
        succeeded = self._rng.random() < self._settings.success_rate
        status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
        logger.info(
            "emulated gateway decision: status=%s delay=%.2fs",
            status.value,
            delay,
            extra={"payment_id": str(payment.id)},
        )
        return status

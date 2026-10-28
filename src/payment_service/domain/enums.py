"""Domain enumerations."""

from __future__ import annotations

from enum import StrEnum


class Currency(StrEnum):
    """Supported payment currencies (ISO 4217 codes)."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    """Lifecycle of a payment: ``pending`` -> ``succeeded`` | ``failed`` (both final)."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def is_final(self) -> bool:
        """Whether the status can no longer change."""
        return self is not PaymentStatus.PENDING

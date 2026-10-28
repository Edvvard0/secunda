"""Time helpers. A single source of "now" keeps timestamps timezone-aware and easy to patch in tests."""

from __future__ import annotations

from datetime import UTC, datetime


def utcnow() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)

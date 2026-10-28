"""Retry policy and failure routing: wait queues first, the dead-letter queue last.

A failed message is *not* retried in place (no ``sleep`` in the consumer, no requeue storm). It is
republished to a wait queue whose TTL equals the backoff delay; when the TTL expires the broker
dead-letters it back into the work queue. After the last attempt the message is parked in the DLQ together
with diagnostics. Our own ``x-attempt`` header carries the attempt counter (see ``docs/ARCHITECTURE.md``,
sections 6 and 7).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Literal, Self

from payment_service.common.time import utcnow

if TYPE_CHECKING:
    from collections.abc import Mapping

    from payment_service.config import RetrySettings
    from payment_service.messaging.publisher import ConfirmedPublisher
    from payment_service.messaging.topology import Topology

X_ATTEMPT: Final = "x-attempt"
"""Header with the 1-based number of the processing attempt that is about to run (absent means 1)."""
X_ERROR_TYPE: Final = "x-error-type"
X_ERROR_MESSAGE: Final = "x-error-message"
X_FAILED_AT: Final = "x-failed-at"

MAX_ERROR_MESSAGE_LENGTH: Final = 500
_MAX_PARSED_ATTEMPT: Final = 2**31 - 1
"""Upper bound for a header value: keeps ``attempt + 1`` encodable as an AMQP integer."""

RetryOutcome = Literal["retry", "dead-letter"]


@dataclass(frozen=True)
class RetryPolicy:
    """How many times a message is processed and how long to wait between attempts."""

    max_attempts: int
    """Total processing attempts, the first one included."""
    base_delay_seconds: float
    backoff_factor: float

    @classmethod
    def from_settings(cls, settings: RetrySettings) -> Self:
        """Build the policy from the ``RETRY_*`` settings group."""
        return cls(
            max_attempts=settings.max_attempts,
            base_delay_seconds=settings.base_delay_seconds,
            backoff_factor=settings.backoff_factor,
        )

    def delay_seconds(self, attempt: int) -> float:
        """Return the wait before attempt ``attempt + 1``, i.e. after attempt ``attempt`` failed.

        Raises:
            ValueError: if ``attempt`` is below 1.
        """
        if attempt < 1:
            msg = f"attempt must be >= 1, got {attempt}"
            raise ValueError(msg)
        return self.base_delay_seconds * self.backoff_factor ** (attempt - 1)

    def has_attempts_left(self, attempt: int) -> bool:
        """Return whether another attempt is allowed after attempt ``attempt`` failed."""
        return attempt < self.max_attempts


def parse_attempt(headers: Mapping[str, Any] | None) -> int:
    """Read the attempt counter from message headers; a missing, malformed or non-positive value means 1."""
    raw: Any = (headers or {}).get(X_ATTEMPT)
    try:
        attempt = int(raw)  # headers come from the network: anything may be in there
    except (TypeError, ValueError, OverflowError):
        return 1
    return min(max(attempt, 1), _MAX_PARSED_ATTEMPT)


def _portable_headers(headers: Mapping[str, Any]) -> dict[str, Any]:
    """Keep application headers only.

    Every ``x-*`` header is dropped: the broker adds ``x-death`` and ``x-first-death-*`` /
    ``x-last-death-*`` while dead-lettering (they grow with every cycle) and our own ``x-*`` headers are
    rebuilt by the caller.
    """
    return {key: value for key, value in headers.items() if not key.lower().startswith("x-")}


class RetryRouter:
    """Decides what happens to a message whose processing failed and republishes it accordingly."""

    def __init__(self, publisher: ConfirmedPublisher, topology: Topology, policy: RetryPolicy) -> None:
        self._publisher = publisher
        self._topology = topology
        self._policy = policy

    async def handle_failure(
        self,
        *,
        body: bytes,
        headers: Mapping[str, Any],
        message_id: str | None,
        correlation_id: str | None,
        attempt: int,
        error: BaseException,
    ) -> RetryOutcome:
        """Send a failed message to its next wait queue, or to the DLQ once the attempts are used up.

        The caller acknowledges the original message only after this method returned, so a message is
        always either still in the work queue or already republished.

        Args:
            body: Raw body of the failed message, republished unchanged.
            headers: Headers of the failed message (broker-managed ``x-*`` headers are not copied).
            message_id: Id of the failed message, kept on the republished one.
            correlation_id: Correlation id of the failed message, kept on the republished one.
            attempt: Number of the attempt that has just failed (1-based).
            error: The failure; only its type and a truncated text are stored, and only in the DLQ.

        Returns:
            ``"retry"`` if the message went to a wait queue, ``"dead-letter"`` if it went to the DLQ.

        Raises:
            Exception: if the republish was not confirmed by the broker. The message must then stay
                unacknowledged.
        """
        topology = self._topology
        carried = _portable_headers(headers)

        if self._policy.has_attempts_left(attempt):
            await self._publisher.send(
                exchange=topology.retry_exchange,
                routing_key=topology.retry_routing_key(attempt),
                body=body,
                message_id=message_id,
                correlation_id=correlation_id,
                message_type=topology.work_routing_key,
                headers={**carried, X_ATTEMPT: attempt + 1},
            )
            return "retry"

        await self._publisher.send(
            exchange=topology.dead_letter_exchange,
            routing_key=topology.dead_letter_routing_key,
            body=body,
            message_id=message_id,
            correlation_id=correlation_id,
            message_type=topology.work_routing_key,
            headers={
                **carried,
                X_ATTEMPT: attempt,
                X_ERROR_TYPE: type(error).__name__,
                X_ERROR_MESSAGE: str(error)[:MAX_ERROR_MESSAGE_LENGTH],
                X_FAILED_AT: utcnow().isoformat(),
            },
        )
        return "dead-letter"

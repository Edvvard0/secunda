"""Base class of long-running background loops (outbox relay, ...)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


async def _sleep_unless_stopped(stop_event: asyncio.Event, delay_seconds: float) -> None:
    """Sleep for ``delay_seconds`` but wake up immediately once ``stop_event`` is set."""
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)


class BackgroundWorker(ABC):
    """A polling loop with graceful shutdown and error backoff.

    Subclasses implement :meth:`run_once` (one unit of work). The loop owns everything around it:

    * ``run_once() -> True``: there may be more work, loop again immediately;
    * ``run_once() -> False``: idle, wait ``poll_interval_seconds`` before the next poll;
    * ``run_once()`` raises: log the traceback, wait ``error_backoff_seconds`` and carry on, so a
      transient failure of the database or the broker never kills the process;
    * ``stop_event`` set: the iteration in flight is allowed to finish, every wait is interrupted and
      :meth:`run_forever` returns;
    * task cancellation (``asyncio.CancelledError``) always propagates.
    """

    def __init__(self, *, name: str, poll_interval_seconds: float, error_backoff_seconds: float) -> None:
        self.name = name
        self._poll_interval_seconds = poll_interval_seconds
        self._error_backoff_seconds = error_backoff_seconds

    @abstractmethod
    async def run_once(self) -> bool:
        """Do one unit of work.

        Returns:
            ``True`` if work was done (poll again at once), ``False`` if there was nothing to do.
        """

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        """Run :meth:`run_once` in a loop until ``stop_event`` is set."""
        logger.info("Worker %s started", self.name)
        try:
            while not stop_event.is_set():
                try:
                    did_work = await self.run_once()
                except Exception:
                    # Process boundary: nothing may escape this loop except cancellation (which is a
                    # BaseException and is therefore not caught here). The failure is logged in full.
                    backoff = self._error_backoff_seconds
                    logger.exception("Worker %s iteration failed; retrying in %.2f s", self.name, backoff)
                    await _sleep_unless_stopped(stop_event, backoff)
                    continue
                if not did_work:
                    await _sleep_unless_stopped(stop_event, self._poll_interval_seconds)
        finally:
            logger.info("Worker %s stopped", self.name)

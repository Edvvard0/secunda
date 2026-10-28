"""Graceful-shutdown wiring for worker processes."""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import FrameType

logger = logging.getLogger(__name__)

_SHUTDOWN_SIGNALS = (signal.SIGINT, signal.SIGTERM)


def _request_stop(stop_event: asyncio.Event, signum: int) -> None:
    logger.info("Received signal %s, shutting down", signal.Signals(signum).name)
    stop_event.set()


def install_shutdown_handlers(stop_event: asyncio.Event) -> None:
    """Set ``stop_event`` when the process receives SIGINT or SIGTERM.

    Must be called from inside a running event loop. POSIX loops support ``add_signal_handler``; the
    Windows loops do not (``NotImplementedError``), so there the handler is registered with
    :func:`signal.signal` and hands over to the loop thread via ``call_soon_threadsafe`` (the only safe
    way to touch an ``asyncio.Event`` from a signal handler).
    """
    loop = asyncio.get_running_loop()

    def hand_over_to_loop(received: int, _frame: FrameType | None) -> None:
        loop.call_soon_threadsafe(_request_stop, stop_event, received)

    for signum in _SHUTDOWN_SIGNALS:
        try:
            loop.add_signal_handler(signum, _request_stop, stop_event, signum)
        except NotImplementedError:
            signal.signal(signum, hand_over_to_loop)

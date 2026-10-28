from __future__ import annotations

import asyncio
import logging
import signal
from typing import TYPE_CHECKING

import pytest

from payment_service.common.signals import install_shutdown_handlers
from payment_service.common.worker import BackgroundWorker

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import FrameType

WORKER_LOGGER = "payment_service.common.worker"
LONG_INTERVAL = 30.0  # a wait that must never run to completion in a passing test
TIMEOUT = 2.0  # generous upper bound for things that must happen "immediately"

type Step = bool | BaseException | Callable[[], Awaitable[bool]]


class ScriptedWorker(BackgroundWorker):
    """Plays a script: each ``run_once`` consumes one step (a result, an exception or a coroutine)."""

    def __init__(self, steps: list[Step], *, poll: float = LONG_INTERVAL, backoff: float = LONG_INTERVAL):
        super().__init__(name="test-worker", poll_interval_seconds=poll, error_backoff_seconds=backoff)
        self._steps = steps
        self.call_times: list[float] = []
        self.first_call = asyncio.Event()

    @property
    def calls(self) -> int:
        return len(self.call_times)

    async def run_once(self) -> bool:
        self.call_times.append(asyncio.get_running_loop().time())
        self.first_call.set()
        step = self._steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        if isinstance(step, bool):
            return step
        return await step()


async def _run_to_completion(worker: BackgroundWorker, stop_event: asyncio.Event) -> None:
    await asyncio.wait_for(worker.run_forever(stop_event), timeout=TIMEOUT)


async def test_run_forever_returns_without_working_when_already_stopped():
    stop_event = asyncio.Event()
    stop_event.set()
    worker = ScriptedWorker([])

    await _run_to_completion(worker, stop_event)

    assert worker.calls == 0


async def test_run_forever_stops_promptly_when_stop_is_set_during_idle_wait():
    stop_event = asyncio.Event()
    worker = ScriptedWorker([False], poll=LONG_INTERVAL)
    task = asyncio.create_task(worker.run_forever(stop_event))

    await asyncio.wait_for(worker.first_call.wait(), timeout=TIMEOUT)
    stop_event.set()
    await asyncio.wait_for(task, timeout=TIMEOUT)

    assert worker.calls == 1


async def test_run_forever_polls_again_immediately_when_work_was_done():
    stop_event = asyncio.Event()

    async def last_step() -> bool:
        stop_event.set()
        return True

    worker = ScriptedWorker([True, True, True, last_step], poll=LONG_INTERVAL)

    await _run_to_completion(worker, stop_event)

    assert worker.calls == 4


async def test_run_forever_waits_poll_interval_when_idle():
    poll = 0.1
    stop_event = asyncio.Event()

    async def last_step() -> bool:
        stop_event.set()
        return False

    worker = ScriptedWorker([False, False, last_step], poll=poll)

    await _run_to_completion(worker, stop_event)

    gaps = [later - earlier for earlier, later in zip(worker.call_times, worker.call_times[1:], strict=False)]
    assert len(gaps) == 2
    # Half the interval is a safe lower bound for coarse Windows timers, yet far above "no wait" (~0).
    assert all(gap >= poll / 2 for gap in gaps)


async def test_run_forever_logs_exception_and_continues_after_backoff(caplog: pytest.LogCaptureFixture):
    backoff = 0.1
    stop_event = asyncio.Event()

    async def last_step() -> bool:
        stop_event.set()
        return False

    worker = ScriptedWorker([RuntimeError("boom"), last_step], backoff=backoff)

    with caplog.at_level(logging.ERROR, logger=WORKER_LOGGER):
        await _run_to_completion(worker, stop_event)

    assert worker.calls == 2
    assert worker.call_times[1] - worker.call_times[0] >= backoff / 2
    [record] = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert "test-worker" in record.getMessage()
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], RuntimeError)


async def test_run_forever_keeps_going_after_repeated_failures():
    stop_event = asyncio.Event()

    async def last_step() -> bool:
        stop_event.set()
        return True

    worker = ScriptedWorker([RuntimeError("1"), ValueError("2"), last_step], backoff=0.01)

    await _run_to_completion(worker, stop_event)

    assert worker.calls == 3


async def test_run_forever_returns_quickly_when_stopped_during_backoff():
    stop_event = asyncio.Event()
    worker = ScriptedWorker([RuntimeError("boom")], backoff=LONG_INTERVAL)
    task = asyncio.create_task(worker.run_forever(stop_event))

    await asyncio.wait_for(worker.first_call.wait(), timeout=TIMEOUT)
    stop_event.set()
    await asyncio.wait_for(task, timeout=TIMEOUT)

    assert worker.calls == 1


async def test_run_forever_propagates_cancellation_without_logging_an_error(
    caplog: pytest.LogCaptureFixture,
):
    stop_event = asyncio.Event()
    never = asyncio.Event()

    async def hang() -> bool:
        await never.wait()
        return True

    worker = ScriptedWorker([hang])
    task = asyncio.create_task(worker.run_forever(stop_event))
    await asyncio.wait_for(worker.first_call.wait(), timeout=TIMEOUT)

    with caplog.at_level(logging.ERROR, logger=WORKER_LOGGER):
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=TIMEOUT)

    assert worker.calls == 1
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


async def test_run_forever_lets_the_in_flight_iteration_finish_before_returning():
    stop_event = asyncio.Event()
    release = asyncio.Event()
    finished: list[bool] = []

    async def slow_step() -> bool:
        await release.wait()
        finished.append(True)
        return True

    worker = ScriptedWorker([slow_step])
    task = asyncio.create_task(worker.run_forever(stop_event))
    await asyncio.wait_for(worker.first_call.wait(), timeout=TIMEOUT)

    stop_event.set()
    done, _ = await asyncio.wait({task}, timeout=0.1)
    assert not done, "run_forever must wait for the iteration in flight"

    release.set()
    await asyncio.wait_for(task, timeout=TIMEOUT)
    assert finished == [True]
    assert worker.calls == 1


async def test_run_forever_logs_start_and_stop(caplog: pytest.LogCaptureFixture):
    stop_event = asyncio.Event()
    stop_event.set()

    with caplog.at_level(logging.INFO, logger=WORKER_LOGGER):
        await _run_to_completion(ScriptedWorker([]), stop_event)

    messages = [r.getMessage() for r in caplog.records if r.name == WORKER_LOGGER]
    assert messages == ["Worker test-worker started", "Worker test-worker stopped"]


async def test_install_shutdown_handlers_registers_loop_handlers_for_sigint_and_sigterm(
    monkeypatch: pytest.MonkeyPatch,
):
    registered: dict[int, tuple[Callable[..., object], tuple[object, ...]]] = {}

    def fake_add_signal_handler(signum: int, callback: Callable[..., object], *args: object) -> None:
        registered[signum] = (callback, args)

    monkeypatch.setattr(asyncio.get_running_loop(), "add_signal_handler", fake_add_signal_handler)
    stop_event = asyncio.Event()

    install_shutdown_handlers(stop_event)

    assert set(registered) == {signal.SIGINT, signal.SIGTERM}
    assert not stop_event.is_set()
    callback, args = registered[signal.SIGTERM]
    callback(*args)
    assert stop_event.is_set()


async def test_install_shutdown_handlers_falls_back_to_signal_module_when_loop_lacks_support(
    monkeypatch: pytest.MonkeyPatch,
):
    handlers: dict[int, Callable[[int, FrameType | None], object]] = {}

    def unsupported(*_args: object) -> None:
        raise NotImplementedError

    def fake_signal(signum: int, handler: Callable[[int, FrameType | None], object]) -> None:
        handlers[signum] = handler

    monkeypatch.setattr(asyncio.get_running_loop(), "add_signal_handler", unsupported)
    monkeypatch.setattr(signal, "signal", fake_signal)
    stop_event = asyncio.Event()

    install_shutdown_handlers(stop_event)

    assert set(handlers) == {signal.SIGINT, signal.SIGTERM}
    handlers[signal.SIGINT](signal.SIGINT, None)
    await asyncio.wait_for(stop_event.wait(), timeout=TIMEOUT)

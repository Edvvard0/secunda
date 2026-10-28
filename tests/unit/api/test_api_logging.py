from __future__ import annotations

import asyncio
import json
import logging
import sys
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest

from payment_service.config import LoggingSettings
from payment_service.logging_config import (
    ConsoleFormatter,
    JsonFormatter,
    configure_logging,
    get_correlation_id,
    set_correlation_id,
)

MakeRecord = Callable[..., logging.LogRecord]


@pytest.fixture
def make_record() -> MakeRecord:
    def factory(
        msg: str = "hello", *args: object, level: int = logging.INFO, **kwargs: Any
    ) -> logging.LogRecord:
        extra = kwargs.pop("extra", None)
        exc_info = kwargs.pop("exc_info", None)
        stack_info = kwargs.pop("stack_info", None)
        return logging.getLogger("tests.sample").makeRecord(
            "tests.sample", level, __file__, 10, msg, args, exc_info, func=None, extra=extra, sinfo=stack_info
        )

    return factory


@pytest.fixture(autouse=True)
def _no_correlation_id() -> Iterator[None]:
    set_correlation_id(None)
    yield
    set_correlation_id(None)


def parse(line: str) -> dict[str, Any]:
    assert "\n" not in line
    parsed = json.loads(line)
    assert isinstance(parsed, dict)
    return parsed


def test_json_line_has_the_standard_fields(make_record: MakeRecord) -> None:
    line = JsonFormatter().format(make_record("order %s accepted", "42", level=logging.WARNING))

    payload = parse(line)
    assert payload["level"] == "WARNING"
    assert payload["logger"] == "tests.sample"
    assert payload["message"] == "order 42 accepted"
    assert datetime.fromisoformat(payload["timestamp"]).utcoffset() == UTC.utcoffset(None)
    assert payload["timestamp"].endswith("+00:00")


def test_json_line_adds_nothing_that_was_not_passed_in(make_record: MakeRecord) -> None:
    payload = parse(JsonFormatter().format(make_record()))

    assert set(payload) == {"timestamp", "level", "logger", "message"}


def test_json_line_includes_the_request_id_from_the_context(make_record: MakeRecord) -> None:
    set_correlation_id("req-77")

    payload = parse(JsonFormatter().format(make_record()))

    assert payload["request_id"] == "req-77"
    assert payload["correlation_id"] == "req-77"


def test_json_line_has_no_request_id_outside_of_a_request(make_record: MakeRecord) -> None:
    payload = parse(JsonFormatter().format(make_record()))

    assert "request_id" not in payload
    assert "correlation_id" not in payload


def test_json_line_includes_every_extra_field(make_record: MakeRecord) -> None:
    payment_id = uuid.uuid4()
    record = make_record(
        extra={
            "payment_id": payment_id,
            "amount": Decimal("100.50"),
            "attempt": 2,
            "ok": True,
            "tags": ["a", "b"],
            "nested": {"k": {"deep": 1}},
            "at": datetime(2026, 1, 15, 10, 30, tzinfo=UTC),
            "nothing": None,
        }
    )

    payload = parse(JsonFormatter().format(record))

    assert payload["payment_id"] == str(payment_id)
    assert payload["amount"] == "100.50"
    assert payload["attempt"] == 2
    assert payload["ok"] is True
    assert payload["tags"] == ["a", "b"]
    assert payload["nested"] == {"k": {"deep": 1}}
    assert payload["at"] == "2026-01-15T10:30:00+00:00"
    assert payload["nothing"] is None


def test_json_line_drops_the_ansi_duplicate_that_uvicorn_attaches(make_record: MakeRecord) -> None:
    record = make_record(
        "Started server process [%d]", 7, extra={"color_message": "Started \x1b[36m%d\x1b[0m"}
    )

    payload = parse(JsonFormatter().format(record))

    assert payload["message"] == "Started server process [7]"
    assert "color_message" not in payload


def test_json_line_does_not_let_an_extra_override_its_own_fields(make_record: MakeRecord) -> None:
    payload = parse(
        JsonFormatter().format(make_record("real", extra={"level": "FAKE", "timestamp": "never"}))
    )

    assert payload["level"] == "INFO"
    assert payload["message"] == "real"
    assert payload["extra_level"] == "FAKE"
    assert payload["extra_timestamp"] == "never"


def test_json_line_carries_the_exception_text(make_record: MakeRecord) -> None:
    try:
        raise ValueError("bad value")
    except ValueError:
        record = make_record("failed", exc_info=sys.exc_info())

    payload = parse(JsonFormatter().format(record))

    assert "Traceback (most recent call last)" in payload["exception"]
    assert "ValueError: bad value" in payload["exception"]
    assert payload["message"] == "failed"


def test_json_line_ignores_an_empty_exc_info_tuple(make_record: MakeRecord) -> None:
    payload = parse(JsonFormatter().format(make_record(exc_info=(None, None, None))))

    assert "exception" not in payload


def test_json_line_carries_the_stack(make_record: MakeRecord) -> None:
    payload = parse(JsonFormatter().format(make_record(stack_info="Stack (most recent call last):\n  here")))

    assert "here" in payload["stack"]


def test_json_line_is_ascii_so_any_terminal_encoding_survives(make_record: MakeRecord) -> None:
    line = JsonFormatter().format(make_record("платёж принят", extra={"клиент": "Иван"}))

    assert line.isascii()
    assert parse(line)["message"] == "платёж принят"


class Unprintable:
    def __str__(self) -> str:
        raise RuntimeError("no string for you")

    __repr__ = __str__


def _circular() -> dict[str, Any]:
    loop: dict[str, Any] = {}
    loop["self"] = loop
    return loop


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(object(), id="plain-object"),
        pytest.param({1, 2, 3}, id="set"),
        pytest.param(b"\xff\xfe", id="bytes"),
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="infinity"),
        pytest.param(_circular(), id="circular-reference"),
        pytest.param({(1, 2): "tuple key"}, id="tuple-key"),
        pytest.param(Unprintable(), id="broken-str"),
        pytest.param([Unprintable()], id="broken-str-inside-a-list"),
        pytest.param(lambda: None, id="function"),
    ],
)
def test_json_formatter_never_raises_on_unserialisable_extras(make_record: MakeRecord, value: object) -> None:
    line = JsonFormatter().format(make_record("still logged", extra={"value": value}))

    payload = parse(line)
    assert payload["message"] == "still logged"
    assert "value" in payload


def test_json_formatter_never_raises_when_the_message_cannot_be_built(make_record: MakeRecord) -> None:
    record = make_record("%d items", "not a number")

    payload = parse(JsonFormatter().format(record))

    assert payload["level"] == "INFO"
    assert payload["logger"] == "tests.sample"
    assert "logging_error" in payload


def test_json_formatter_survives_a_message_object_with_a_broken_str(make_record: MakeRecord) -> None:
    record = make_record("x")
    record.msg = Unprintable()

    payload = parse(JsonFormatter().format(record))

    assert "logging_error" in payload


def test_console_formatter_prints_one_readable_line(make_record: MakeRecord) -> None:
    set_correlation_id("req-9")

    line = ConsoleFormatter().format(
        make_record("payment accepted", extra={"payment_id": "p-1", "attempt": 2})
    )

    assert "INFO" in line
    assert "tests.sample" in line
    assert "[req-9]" in line
    assert "payment accepted" in line
    assert "payment_id=p-1" in line
    assert "attempt=2" in line
    assert "\n" not in line


def test_console_formatter_shows_a_dash_without_request_id_and_appends_the_exception(
    make_record: MakeRecord,
) -> None:
    try:
        raise ValueError("bad value")
    except ValueError:
        record = make_record("failed", exc_info=sys.exc_info())

    text = ConsoleFormatter().format(record)

    assert "[-]" in text.splitlines()[0]
    assert "ValueError: bad value" in text


def test_console_formatter_survives_broken_extras(make_record: MakeRecord) -> None:
    assert "value=<unprintable Unprintable>" in ConsoleFormatter().format(
        make_record("x", extra={"value": Unprintable()})
    )


def test_correlation_id_round_trip() -> None:
    assert get_correlation_id() is None

    set_correlation_id("abc")
    assert get_correlation_id() == "abc"

    set_correlation_id(None)
    assert get_correlation_id() is None


async def test_correlation_id_does_not_leak_between_concurrent_tasks() -> None:
    seen: dict[str, str | None] = {}

    async def work(name: str, ready: asyncio.Event, go: asyncio.Event) -> None:
        set_correlation_id(name)
        ready.set()
        await go.wait()
        seen[name] = get_correlation_id()

    go = asyncio.Event()
    readies = [asyncio.Event(), asyncio.Event()]
    tasks = [asyncio.create_task(work(n, e, go)) for n, e in zip(("one", "two"), readies, strict=True)]
    for ready in readies:
        await ready.wait()
    go.set()
    await asyncio.gather(*tasks)

    assert seen == {"one": "one", "two": "two"}
    assert get_correlation_id() is None


def _ours(root: logging.Logger) -> list[logging.Handler]:
    return [h for h in root.handlers if getattr(h, "_payment_service_handler", False)]


def test_configure_logging_installs_a_json_handler_and_the_level() -> None:
    configure_logging(LoggingSettings(level="DEBUG", json_format=True))

    root = logging.getLogger()
    (handler,) = _ours(root)
    assert isinstance(handler.formatter, JsonFormatter)
    assert root.level == logging.DEBUG


def test_configure_logging_can_install_the_human_readable_format() -> None:
    configure_logging(LoggingSettings(level="WARNING", json_format=False))

    (handler,) = _ours(logging.getLogger())
    assert isinstance(handler.formatter, ConsoleFormatter)
    assert logging.getLogger().level == logging.WARNING


def test_configure_logging_is_idempotent() -> None:
    for _ in range(3):
        configure_logging(LoggingSettings(level="INFO", json_format=True))

    assert len(_ours(logging.getLogger())) == 1


def test_configure_logging_replaces_its_own_handler_when_the_format_changes() -> None:
    configure_logging(LoggingSettings(json_format=True))
    configure_logging(LoggingSettings(json_format=False))

    (handler,) = _ours(logging.getLogger())
    assert isinstance(handler.formatter, ConsoleFormatter)


def test_configure_logging_leaves_foreign_handlers_alone() -> None:
    foreign = logging.NullHandler()
    root = logging.getLogger()
    root.addHandler(foreign)
    try:
        configure_logging(LoggingSettings())
        configure_logging(LoggingSettings())

        assert foreign in root.handlers
    finally:
        root.removeHandler(foreign)


def test_configure_logging_routes_library_loggers_through_the_root_handler() -> None:
    for name in ("uvicorn", "uvicorn.error", "faststream", "aio_pika", "aiormq"):
        library = logging.getLogger(name)
        library.addHandler(logging.NullHandler())
        library.propagate = False

    configure_logging(LoggingSettings())

    for name in ("uvicorn", "uvicorn.error", "faststream", "aio_pika", "aiormq"):
        library = logging.getLogger(name)
        assert library.handlers == []
        assert library.propagate is True


def test_configure_logging_silences_the_duplicate_uvicorn_access_log() -> None:
    access = logging.getLogger("uvicorn.access")
    access.addHandler(logging.NullHandler())
    access.propagate = True

    configure_logging(LoggingSettings())

    assert access.handlers == []
    assert access.propagate is False


def test_logging_end_to_end_writes_json_lines_to_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(LoggingSettings(level="INFO", json_format=True))
    set_correlation_id("e2e-1")

    logging.getLogger("uvicorn.error").info("server says hi")
    logging.getLogger("payment_service.demo").info("order accepted", extra={"order": 42})
    logging.getLogger("payment_service.demo").debug("below the configured level")

    lines = capsys.readouterr().out.splitlines()
    first, second = (parse(line) for line in lines)
    assert first["logger"] == "uvicorn.error"
    assert first["request_id"] == "e2e-1"
    assert second["order"] == 42
    assert len(lines) == 2


class Recorder(logging.Handler):
    """Collects everything that reaches the root logger, whatever its level."""

    def __init__(self) -> None:
        super().__init__(level=logging.NOTSET)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def everything(self) -> str:
        return "\n".join(f"{r.getMessage()} {r.__dict__}" for r in self.records)


async def _call_receiver() -> None:
    """A real httpx round trip (in-memory transport) to a URL that carries a receiver token."""
    transport = httpx.MockTransport(lambda _request: httpx.Response(200))
    async with httpx.AsyncClient(transport=transport) as client:
        await client.post("https://203.0.113.7/hooks/abc?token=secret-token-SENTINEL", json={})


@pytest.fixture
def recorder() -> Iterator[Recorder]:
    handler = Recorder()
    logging.getLogger().addHandler(handler)
    yield handler
    logging.getLogger().removeHandler(handler)


async def test_httpx_would_log_the_full_url_without_the_safeguard(recorder: Recorder) -> None:
    # Control experiment: proves the leak is real, so the test below can not pass by accident.
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger("httpx").setLevel(logging.NOTSET)

    await _call_receiver()

    assert "token=secret-token-SENTINEL" in recorder.everything()


@pytest.mark.parametrize("level", ["DEBUG", "INFO"])
async def test_urls_with_secrets_never_reach_the_logs(
    level: str, recorder: Recorder, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging(LoggingSettings(level=level, json_format=True))

    await _call_receiver()

    assert "SENTINEL" not in recorder.everything()
    assert "SENTINEL" not in capsys.readouterr().out
    assert recorder.records == []


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        ("DEBUG", logging.WARNING),
        ("INFO", logging.WARNING),
        ("WARNING", logging.WARNING),
        ("ERROR", logging.ERROR),
    ],
)
def test_httpx_and_httpcore_are_never_more_verbose_than_warning(configured: str, expected: int) -> None:
    configure_logging(LoggingSettings(level=configured, json_format=True))

    for name in ("httpx", "httpcore"):
        assert logging.getLogger(name).getEffectiveLevel() == expected


def test_a_later_call_re_applies_the_floor_after_someone_lowered_it() -> None:
    configure_logging(LoggingSettings(level="DEBUG"))
    logging.getLogger("httpx").setLevel(logging.DEBUG)

    configure_logging(LoggingSettings(level="DEBUG"))

    assert logging.getLogger("httpx").level == logging.WARNING

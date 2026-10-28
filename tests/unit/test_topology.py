from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest
from faststream.rabbit import ExchangeType

from payment_service.messaging.topology import Topology

if TYPE_CHECKING:
    from collections.abc import Callable

    from faststream.rabbit import RabbitBroker

    from payment_service.config import Settings


@pytest.fixture
def make_topology(make_settings: Callable[..., Settings]) -> Callable[..., Topology]:
    def factory(
        *, max_attempts: int = 3, queue_type: str = "quorum", base: float = 5.0, factor: float = 2.0
    ) -> Topology:
        settings = make_settings(
            rabbit={"queue_type": queue_type},
            retry={"max_attempts": max_attempts, "base_delay_seconds": base, "backoff_factor": factor},
        )
        return Topology(settings.rabbit, settings.retry)

    return factory


def test_exchanges_are_durable_direct_and_named_after_the_architecture(
    make_topology: Callable[..., Topology],
) -> None:
    topology = make_topology()

    exchanges = [topology.payments_exchange, topology.retry_exchange, topology.dead_letter_exchange]
    assert [exchange.name for exchange in exchanges] == ["payments", "payments.retry", "payments.dlx"]
    assert {exchange.type for exchange in exchanges} == {ExchangeType.DIRECT}
    assert all(exchange.durable for exchange in exchanges)


def test_work_queue_dead_letters_rejected_messages_into_the_dlq(
    make_topology: Callable[..., Topology],
) -> None:
    queue = make_topology().work_queue

    assert queue.name == "payments.new"
    assert queue.routing() == "payments.new"
    assert queue.durable
    assert queue.arguments == {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.new.dlq",
    }


def test_dead_letter_queue_has_no_ttl_and_no_dead_lettering(make_topology: Callable[..., Topology]) -> None:
    queue = make_topology().dead_letter_queue

    assert queue.name == "payments.new.dlq"
    assert queue.routing() == "payments.new.dlq"
    assert queue.durable
    assert queue.arguments == {"x-queue-type": "quorum"}


@pytest.mark.parametrize(("max_attempts", "wait_queues"), [(1, 0), (2, 1), (3, 2), (5, 4)])
def test_wait_queue_count_is_max_attempts_minus_one(
    make_topology: Callable[..., Topology], max_attempts: int, wait_queues: int
) -> None:
    topology = make_topology(max_attempts=max_attempts)

    assert [queue.name for queue in topology.retry_queues] == [
        f"payments.new.retry.{n}" for n in range(1, wait_queues + 1)
    ]


def test_wait_queues_return_expired_messages_to_the_work_queue(
    make_topology: Callable[..., Topology],
) -> None:
    topology = make_topology(max_attempts=3)

    for attempt, queue in enumerate(topology.retry_queues, start=1):
        assert queue.routing() == topology.retry_routing_key(attempt) == f"payments.new.retry.{attempt}"
        assert queue.arguments["x-dead-letter-exchange"] == "payments"
        assert queue.arguments["x-dead-letter-routing-key"] == "payments.new"
        assert queue.arguments["x-message-ttl"] == topology.retry_ttl_ms(attempt)
        assert queue.arguments["x-queue-type"] == "quorum"


def test_wait_queue_ttl_defaults_to_5_then_10_seconds(make_topology: Callable[..., Topology]) -> None:
    topology = make_topology(max_attempts=3)

    assert [topology.retry_ttl_ms(1), topology.retry_ttl_ms(2)] == [5_000, 10_000]


def test_wait_queue_ttl_grows_geometrically_with_a_custom_factor(
    make_topology: Callable[..., Topology],
) -> None:
    topology = make_topology(max_attempts=5, base=0.5, factor=3.0)

    assert [topology.retry_ttl_ms(n) for n in range(1, 5)] == [500, 1_500, 4_500, 13_500]


def test_wait_queue_ttl_is_not_distorted_by_float_noise(make_topology: Callable[..., Topology]) -> None:
    topology = make_topology(max_attempts=3, base=0.2, factor=2.0)

    assert [topology.retry_ttl_ms(1), topology.retry_ttl_ms(2)] == [200, 400]


@pytest.mark.parametrize("attempt", [0, -1, 3, 4])
def test_retry_lookups_reject_attempts_without_a_wait_queue(
    make_topology: Callable[..., Topology], attempt: int
) -> None:
    topology = make_topology(max_attempts=3)

    with pytest.raises(ValueError, match="no wait queue"):
        topology.retry_routing_key(attempt)
    with pytest.raises(ValueError, match="no wait queue"):
        topology.retry_ttl_ms(attempt)


def test_single_attempt_topology_has_no_retry_routing(make_topology: Callable[..., Topology]) -> None:
    topology = make_topology(max_attempts=1)

    with pytest.raises(ValueError, match="no wait queue"):
        topology.retry_routing_key(1)


def test_classic_queue_type_is_applied_to_every_queue(make_topology: Callable[..., Topology]) -> None:
    topology = make_topology(max_attempts=3, queue_type="classic")

    queues = [topology.work_queue, topology.dead_letter_queue, *topology.retry_queues]
    assert {queue.arguments["x-queue-type"] for queue in queues} == {"classic"}
    assert topology.work_queue.arguments["x-dead-letter-exchange"] == "payments.dlx"
    assert topology.retry_queues[0].arguments["x-message-ttl"] == 5_000


def test_quorum_is_the_default_queue_type(make_settings: Callable[..., Settings]) -> None:
    settings = make_settings()

    assert settings.rabbit.queue_type == "quorum"
    topology = Topology(settings.rabbit, settings.retry)
    assert topology.work_queue.arguments["x-queue-type"] == "quorum"


# --------------------------------------------------------------------------- #
# declare(): which declarations and bindings are issued, in a form that needs no broker
# --------------------------------------------------------------------------- #
class FakeQueue:
    def __init__(self, name: str, bindings: list[tuple[str, str]]) -> None:
        self.name = name
        self._bindings = bindings

    async def bind(self, exchange: Any, routing_key: str) -> None:
        self._bindings.append((self.name, f"{exchange.name}:{routing_key}"))


class FakeBroker:
    """Records declarations; understands just what ``Topology.declare`` calls."""

    def __init__(self) -> None:
        self.exchanges: list[str] = []
        self.queues: list[str] = []
        self.bindings: list[tuple[str, str]] = []

    async def declare_exchange(self, exchange: Any) -> Any:
        self.exchanges.append(exchange.name)
        return exchange

    async def declare_queue(self, queue: Any) -> FakeQueue:
        self.queues.append(queue.name)
        return FakeQueue(queue.name, self.bindings)


async def test_declare_creates_every_exchange_queue_and_binding(
    make_topology: Callable[..., Topology],
) -> None:
    broker = FakeBroker()

    await make_topology(max_attempts=3).declare(cast("RabbitBroker", broker))

    assert sorted(broker.exchanges) == ["payments", "payments.dlx", "payments.retry"]
    assert sorted(broker.queues) == [
        "payments.new",
        "payments.new.dlq",
        "payments.new.retry.1",
        "payments.new.retry.2",
    ]
    assert sorted(broker.bindings) == [
        ("payments.new", "payments:payments.new"),
        ("payments.new.dlq", "payments.dlx:payments.new.dlq"),
        ("payments.new.retry.1", "payments.retry:payments.new.retry.1"),
        ("payments.new.retry.2", "payments.retry:payments.new.retry.2"),
    ]


async def test_declare_binds_the_work_queue_so_it_does_not_depend_on_the_subscriber(
    make_topology: Callable[..., Topology],
) -> None:
    broker = FakeBroker()

    await make_topology(max_attempts=1).declare(cast("RabbitBroker", broker))

    assert broker.queues.count("payments.new") == 1
    assert ("payments.new", "payments:payments.new") in broker.bindings
    assert not any(name.startswith("payments.new.retry") for name in broker.queues)

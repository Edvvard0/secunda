"""Composition root of the ``relay`` process: ``python -m payment_service.entrypoints.relay``."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack

from payment_service.common.signals import install_shutdown_handlers
from payment_service.config import Settings
from payment_service.logging_config import configure_logging
from payment_service.messaging.publisher import RabbitEventPublisher, create_broker
from payment_service.messaging.topology import Topology
from payment_service.persistence import Database
from payment_service.services.outbox import OutboxRelay


async def build_and_run(settings: Settings) -> None:
    """Wire the object graph, relay the outbox until SIGINT/SIGTERM, then release every resource."""
    stop_event = asyncio.Event()
    install_shutdown_handlers(stop_event)

    async with AsyncExitStack() as resources:
        database = Database(settings.db)
        resources.push_async_callback(database.dispose)

        broker = create_broker(settings.rabbit)
        # Registered after the database, so the broker is closed first (stack unwinds in reverse order).
        resources.push_async_callback(broker.stop)

        topology = Topology(settings.rabbit, settings.retry)
        await broker.connect()
        await topology.declare(broker)

        publisher = RabbitEventPublisher(broker, topology.payments_exchange)
        relay = OutboxRelay(database.unit_of_work, publisher, settings.outbox)
        await relay.run_forever(stop_event)


def main() -> None:
    """Entry point: load settings, configure logging and run the outbox relay."""
    settings = Settings.load()
    configure_logging(settings.logging)
    asyncio.run(build_and_run(settings))


if __name__ == "__main__":
    main()

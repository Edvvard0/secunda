"""Composition root of the ``consumer`` process: ``python -m payment_service.entrypoints.consumer``."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack

from payment_service.config import Settings
from payment_service.logging_config import configure_logging
from payment_service.messaging.consumer import PaymentConsumer, create_consumer_app
from payment_service.messaging.publisher import ConfirmedPublisher, create_broker
from payment_service.messaging.retry import RetryPolicy, RetryRouter
from payment_service.messaging.topology import Topology
from payment_service.persistence import Database
from payment_service.services.gateway import EmulatedPaymentGateway
from payment_service.services.processor import PaymentProcessor
from payment_service.services.webhook import HttpWebhookSender


async def build_and_run(settings: Settings) -> None:
    """Wire the object graph, run the consumer until SIGINT/SIGTERM, then release every resource."""
    async with AsyncExitStack() as resources:
        database = Database(settings.db)
        resources.push_async_callback(database.dispose)

        webhook_sender = HttpWebhookSender(settings.webhook)
        resources.push_async_callback(webhook_sender.aclose)

        processor = PaymentProcessor(
            database.unit_of_work, EmulatedPaymentGateway(settings.gateway), webhook_sender
        )

        broker = create_broker(settings.rabbit)
        topology = Topology(settings.rabbit, settings.retry)
        policy = RetryPolicy.from_settings(settings.retry)
        router = RetryRouter(ConfirmedPublisher(broker), topology, policy)
        app = create_consumer_app(broker, topology, PaymentConsumer(processor, router, policy))

        # FastStream installs the SIGINT/SIGTERM handlers itself and stops the broker gracefully: it
        # waits for in-flight messages before returning, so the resources above outlive every handler.
        await app.run()


def main() -> None:
    """Entry point: load settings, configure logging and run the consumer."""
    settings = Settings.load()
    configure_logging(settings.logging)
    asyncio.run(build_and_run(settings))


if __name__ == "__main__":
    main()

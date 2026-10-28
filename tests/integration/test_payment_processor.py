from __future__ import annotations

import asyncio
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text

from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.exceptions import PaymentNotFoundError, WebhookDeliveryError
from payment_service.services.dto import CreatePaymentCommand, PaymentDTO
from payment_service.services.interfaces import PaymentGateway, WebhookSender
from payment_service.services.payments import PaymentService
from payment_service.services.processor import PaymentProcessor

if TYPE_CHECKING:
    from payment_service.persistence import Database

BARRIER_TIMEOUT_SECONDS = 10


class FakeGateway(PaymentGateway):
    """Returns scripted statuses (the last one repeats) and counts calls."""

    def __init__(self, *statuses: PaymentStatus, barrier: asyncio.Barrier | None = None) -> None:
        self._statuses = statuses or (PaymentStatus.SUCCEEDED,)
        self._barrier = barrier
        self.calls = 0

    async def charge(self, payment: PaymentDTO) -> PaymentStatus:
        index = min(self.calls, len(self._statuses) - 1)
        self.calls += 1
        if self._barrier is not None:
            await asyncio.wait_for(self._barrier.wait(), BARRIER_TIMEOUT_SECONDS)
        return self._statuses[index]


class FakeWebhookSender(WebhookSender):
    """Records delivered payments; the first ``fail_first`` sends raise ``WebhookDeliveryError``."""

    def __init__(self, fail_first: int = 0) -> None:
        self._remaining_failures = fail_first
        self.attempts = 0
        self.sent: list[PaymentDTO] = []

    async def send(self, payment: PaymentDTO) -> None:
        self.attempts += 1
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            msg = "receiver is down"
            raise WebhookDeliveryError(msg)
        self.sent.append(payment)


async def count_idle_in_transaction(database: Database) -> int:
    async with database.engine.connect() as connection:
        result = await connection.execute(
            text(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE datname = current_database() AND state LIKE 'idle in transaction%'"
            )
        )
        return int(result.scalar_one())


class TransactionProbingGateway(FakeGateway):
    """Records how many database transactions are open while the gateway is being called."""

    def __init__(self, database: Database) -> None:
        super().__init__()
        self._database = database
        self.open_transactions: int | None = None

    async def charge(self, payment: PaymentDTO) -> PaymentStatus:
        self.open_transactions = await count_idle_in_transaction(self._database)
        return await super().charge(payment)


class TransactionProbingSender(FakeWebhookSender):
    """Records how many database transactions are open while the webhook is being sent."""

    def __init__(self, database: Database) -> None:
        super().__init__()
        self._database = database
        self.open_transactions: int | None = None

    async def send(self, payment: PaymentDTO) -> None:
        self.open_transactions = await count_idle_in_transaction(self._database)
        await super().send(payment)


def make_processor(database: Database, gateway: PaymentGateway, sender: WebhookSender) -> PaymentProcessor:
    return PaymentProcessor(database.unit_of_work, gateway, sender)


async def create_pending_payment(database: Database) -> uuid.UUID:
    command = CreatePaymentCommand(
        amount=Decimal("100.50"),
        currency=Currency.RUB,
        description="Order #42",
        metadata={"order_id": 42},
        webhook_url="https://client.example.com/hooks/payments",
    )
    result = await PaymentService(database.unit_of_work).create(command, idempotency_key=str(uuid.uuid4()))
    return result.payment.id


async def load_payment(database: Database, payment_id: uuid.UUID) -> PaymentDTO:
    return await PaymentService(database.unit_of_work).get(payment_id)


async def test_process_charges_pending_payment_and_delivers_webhook_once(clean_database: Database):
    payment_id = await create_pending_payment(clean_database)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    sender = FakeWebhookSender()

    await make_processor(clean_database, gateway, sender).process(payment_id)

    stored = await load_payment(clean_database, payment_id)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None
    assert gateway.calls == 1
    assert [sent.id for sent in sender.sent] == [payment_id]
    assert sender.sent[0].status is PaymentStatus.SUCCEEDED
    assert sender.sent[0].processed_at == stored.processed_at


async def test_process_stores_failed_status_and_still_notifies_the_client(clean_database: Database):
    payment_id = await create_pending_payment(clean_database)
    sender = FakeWebhookSender()

    await make_processor(clean_database, FakeGateway(PaymentStatus.FAILED), sender).process(payment_id)

    stored = await load_payment(clean_database, payment_id)
    assert stored.status is PaymentStatus.FAILED
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None
    assert [sent.status for sent in sender.sent] == [PaymentStatus.FAILED]


async def test_process_keeps_final_status_when_webhook_delivery_fails(clean_database: Database):
    payment_id = await create_pending_payment(clean_database)
    processor = make_processor(
        clean_database, FakeGateway(PaymentStatus.SUCCEEDED), FakeWebhookSender(fail_first=1)
    )

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment_id)

    stored = await load_payment(clean_database, payment_id)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is None


async def test_redelivery_after_webhook_failure_retries_only_the_webhook(clean_database: Database):
    payment_id = await create_pending_payment(clean_database)
    # A second charge would answer FAILED: the test fails loudly if the gateway is called again.
    gateway = FakeGateway(PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
    sender = FakeWebhookSender(fail_first=1)
    processor = make_processor(clean_database, gateway, sender)
    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment_id)
    after_failure = await load_payment(clean_database, payment_id)

    await processor.process(payment_id)

    stored = await load_payment(clean_database, payment_id)
    assert gateway.calls == 1
    assert sender.attempts == 2
    assert [sent.status for sent in sender.sent] == [PaymentStatus.SUCCEEDED]
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at == after_failure.processed_at
    assert stored.webhook_delivered_at is not None


async def test_redelivery_after_full_success_does_nothing(clean_database: Database):
    payment_id = await create_pending_payment(clean_database)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
    sender = FakeWebhookSender()
    processor = make_processor(clean_database, gateway, sender)
    await processor.process(payment_id)
    before = await load_payment(clean_database, payment_id)

    await processor.process(payment_id)

    assert await load_payment(clean_database, payment_id) == before
    assert gateway.calls == 1
    assert sender.attempts == 1


async def test_process_raises_not_found_for_missing_payment(clean_database: Database):
    gateway = FakeGateway()
    sender = FakeWebhookSender()
    missing = uuid.uuid4()

    with pytest.raises(PaymentNotFoundError, match=str(missing)):
        await make_processor(clean_database, gateway, sender).process(missing)

    assert gateway.calls == 0
    assert sender.attempts == 0


async def test_concurrent_processors_store_a_single_final_status(clean_database: Database):
    payment_id = await create_pending_payment(clean_database)
    # The barrier holds both processors inside charge() until each has read the payment as pending, so
    # they deterministically race for the compare-and-set. The two gateways disagree on the outcome.
    gateway = FakeGateway(PaymentStatus.SUCCEEDED, PaymentStatus.FAILED, barrier=asyncio.Barrier(2))
    sender = FakeWebhookSender()
    processors = [make_processor(clean_database, gateway, sender) for _ in range(2)]

    await asyncio.gather(*(processor.process(payment_id) for processor in processors))

    stored = await load_payment(clean_database, payment_id)
    assert gateway.calls == 2
    assert stored.status.is_final
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None
    assert 1 <= len(sender.sent) <= 2
    # Whoever lost the race notifies the client with the persisted outcome, not with its own charge result.
    assert {sent.status for sent in sender.sent} == {stored.status}
    assert {sent.processed_at for sent in sender.sent} == {stored.processed_at}


async def test_process_rejects_non_final_gateway_status(clean_database: Database):
    payment_id = await create_pending_payment(clean_database)
    sender = FakeWebhookSender()
    processor = make_processor(clean_database, FakeGateway(PaymentStatus.PENDING), sender)

    with pytest.raises(ValueError, match="non-final"):
        await processor.process(payment_id)

    stored = await load_payment(clean_database, payment_id)
    assert stored.status is PaymentStatus.PENDING
    assert stored.processed_at is None
    assert sender.attempts == 0


async def test_gateway_is_called_without_an_open_database_transaction(clean_database: Database):
    payment_id = await create_pending_payment(clean_database)
    gateway = TransactionProbingGateway(clean_database)

    await make_processor(clean_database, gateway, FakeWebhookSender()).process(payment_id)

    assert gateway.open_transactions == 0


async def test_webhook_is_sent_without_an_open_database_transaction(clean_database: Database):
    payment_id = await create_pending_payment(clean_database)
    sender = TransactionProbingSender(clean_database)

    await make_processor(clean_database, FakeGateway(), sender).process(payment_id)

    assert sender.open_transactions == 0

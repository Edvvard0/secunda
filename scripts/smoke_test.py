"""End-to-end smoke test against a running docker-compose stack (``make smoke``).

Exercises the real system through its public interfaces only: HTTP API, the demo webhook sink and the
RabbitMQ management API. It verifies the guarantees the service promises:

1. a payment is accepted (202) and reaches a final status (processed through the broker);
2. the signed webhook arrives at the receiver exactly as signed;
3. replaying the same ``Idempotency-Key`` returns the same payment, a different body is rejected (422);
4. a receiver that always fails leads to retries and finally a message in the dead-letter queue.

Requires the demo profile: ``docker compose --profile demo up --build -d``.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from typing import Any

import httpx

POLL_INTERVAL_SECONDS = 0.5
FINAL_STATUSES = {"succeeded", "failed"}


class SmokeTestError(Exception):
    """A check of the smoke test did not hold."""


def check(condition: bool, message: str) -> None:
    """Fail the smoke test with ``message`` unless ``condition`` is true."""
    if not condition:
        raise SmokeTestError(message)
    print(f"  ok  {message}")


def wait_for(description: str, probe: Any, *, timeout: float) -> Any:
    """Call ``probe`` until it returns a truthy value or ``timeout`` seconds elapse."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(POLL_INTERVAL_SECONDS)
    msg = f"timed out after {timeout:.0f}s waiting for: {description}"
    raise SmokeTestError(msg)


def create_payment(
    client: httpx.Client, *, key: str, webhook_url: str, amount: str = "100.50"
) -> httpx.Response:
    """POST a payment with the given idempotency key."""
    return client.post(
        "/api/v1/payments",
        headers={"Idempotency-Key": key},
        json={
            "amount": amount,
            "currency": "RUB",
            "description": "smoke test",
            "metadata": {"source": "smoke"},
            "webhook_url": webhook_url,
        },
    )


def scenario_happy_path(api: httpx.Client, sink: httpx.Client, sink_internal_url: str) -> None:
    """Accepted -> processed -> signed webhook received; idempotency holds."""
    print("Scenario 1: happy path and idempotency")
    key = f"smoke-{uuid.uuid4()}"
    webhook_url = f"{sink_internal_url}/ok"

    first = create_payment(api, key=key, webhook_url=webhook_url)
    check(first.status_code == 202, "POST /payments -> 202 Accepted")
    payment_id = first.json()["payment_id"]
    check(first.json()["status"] == "pending", "new payment is pending")

    replay = create_payment(api, key=key, webhook_url=webhook_url)
    check(replay.status_code == 202, "replay with the same key -> 202")
    check(replay.json()["payment_id"] == payment_id, "replay returns the same payment_id")
    check(replay.headers.get("idempotent-replayed") == "true", "replay is marked Idempotent-Replayed")

    conflict = create_payment(api, key=key, webhook_url=webhook_url, amount="999.00")
    check(conflict.status_code == 422, "same key with another body -> 422")
    check(conflict.json()["code"] == "idempotency-key-reused", "problem code is idempotency-key-reused")

    def final_payment() -> dict[str, Any] | None:
        body: dict[str, Any] = api.get(f"/api/v1/payments/{payment_id}").json()
        return body if body["status"] in FINAL_STATUSES else None

    payment = wait_for("payment to reach a final status", final_payment, timeout=60)
    check(payment["status"] in FINAL_STATUSES, f"payment finalized as {payment['status']}")
    check(payment["processed_at"] is not None, "processed_at is set")

    def delivered() -> dict[str, Any] | None:
        for item in sink.get("/received").json():
            if item["payment_id"] == payment_id and item["status_code"] == 200:
                return item  # type: ignore[no-any-return]
        return None

    delivery = wait_for("webhook to reach the sink", delivered, timeout=30)
    check(delivery["signature_valid"] is True, "webhook signature verified by the receiver")
    check(delivery["body"]["status"] == payment["status"], "webhook reports the stored status")


def scenario_dead_letter(
    api: httpx.Client, sink_internal_url: str, rabbit: httpx.Client | None, *, timeout: float
) -> None:
    """A permanently failing receiver -> retries -> the message ends in the dead-letter queue."""
    print("Scenario 2: failing webhook -> retries -> DLQ")
    key = f"smoke-{uuid.uuid4()}"
    response = create_payment(api, key=key, webhook_url=f"{sink_internal_url}/fail")
    check(response.status_code == 202, "payment with an always-failing webhook is accepted")
    if rabbit is None:
        print("  skip  RabbitMQ management credentials not provided: DLQ check skipped")
        return

    def dead_lettered() -> bool:
        reply = rabbit.get("/api/queues/%2F/payments.new.dlq")
        return reply.status_code == 200 and reply.json().get("messages", 0) >= 1

    wait_for("message in payments.new.dlq", dead_lettered, timeout=timeout)
    check(True, "message reached payments.new.dlq after the retries")


def main() -> int:
    """Run all scenarios; exit code 0 when everything holds."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--sink-url", default="http://localhost:9000", help="sink as seen from here")
    parser.add_argument(
        "--sink-internal-url",
        default="http://webhook-sink:9000",
        help="sink as seen from inside the docker network (used as webhook_url)",
    )
    parser.add_argument("--rabbit-management-url", default="http://localhost:15672")
    parser.add_argument("--dlq-timeout", type=float, default=90.0)
    args = parser.parse_args()

    api_key = os.environ.get("API_KEY")
    if not api_key:
        print("API_KEY environment variable is required (see .env)", file=sys.stderr)
        return 2
    rabbit_user = os.environ.get("RABBITMQ_DEFAULT_USER")
    rabbit_password = os.environ.get("RABBITMQ_DEFAULT_PASS")

    rabbit = (
        httpx.Client(base_url=args.rabbit_management_url, auth=(rabbit_user, rabbit_password), timeout=10)
        if rabbit_user and rabbit_password
        else None
    )
    try:
        with (
            httpx.Client(base_url=args.api_url, headers={"X-API-Key": api_key}, timeout=10) as api,
            httpx.Client(base_url=args.sink_url, timeout=10) as sink,
        ):
            check(api.get("/health/ready").status_code == 200, "GET /health/ready -> 200")
            scenario_happy_path(api, sink, args.sink_internal_url)
            scenario_dead_letter(api, args.sink_internal_url, rabbit, timeout=args.dlq_timeout)
    except SmokeTestError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    finally:
        if rabbit is not None:
            rabbit.close()
    print("ALL SMOKE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())

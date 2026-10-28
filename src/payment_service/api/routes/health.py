"""Liveness and readiness probes. Unauthenticated: orchestrators call them without credentials."""

from __future__ import annotations

import asyncio
import logging
from typing import Final

from fastapi import APIRouter

from payment_service.api.dependencies import DatabaseDep  # noqa: TC001
from payment_service.api.schemas import HealthResponse, ProblemDetails
from payment_service.domain.exceptions import ServiceUnavailableError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/health", tags=["health"])

# A hung database must not pile up probe requests; orchestrators time out on their own anyway.
_PING_TIMEOUT_SECONDS: Final = 3.0


@router.get(
    "/live",
    operation_id="getLiveness",
    summary="Liveness probe",
    description="Returns 200 while the process is able to serve requests. Does not touch dependencies.",
    response_model=HealthResponse,
)
async def live() -> HealthResponse:
    """Report that the process is alive."""
    return HealthResponse()


@router.get(
    "/ready",
    operation_id="getReadiness",
    summary="Readiness probe",
    description="Returns 200 when the database answers; 503 otherwise, so traffic is routed elsewhere.",
    response_model=HealthResponse,
    responses={503: {"model": ProblemDetails, "description": "The database is unreachable."}},
)
async def ready(database: DatabaseDep) -> HealthResponse:
    """Check that the database is reachable."""
    try:
        async with asyncio.timeout(_PING_TIMEOUT_SECONDS):
            await database.ping()
    except Exception as exc:
        # Any driver, network or timeout error means "not ready"; details stay in the log, not the response.
        logger.warning("Readiness check failed: database is unreachable", exc_info=True)
        msg = "The database is not reachable."
        raise ServiceUnavailableError(msg) from exc
    return HealthResponse()

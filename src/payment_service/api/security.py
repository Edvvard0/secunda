"""``X-API-Key`` authentication.

The key is held as ``SecretStr`` and compared in constant time. Neither the expected nor the supplied
value is ever logged or echoed in an error.
"""

from __future__ import annotations

import secrets
from typing import TYPE_CHECKING, Annotated, Final

from fastapi import Security
from fastapi.security import APIKeyHeader

from payment_service.domain.exceptions import UnauthorizedError

if TYPE_CHECKING:
    from pydantic import SecretStr

API_KEY_HEADER_NAME: Final = "X-API-Key"
API_KEY_SCHEME_NAME: Final = "ApiKey"

# Stateless description of where the key lives. ``auto_error=False`` hands a missing header to
# ``ApiKeyAuth`` so that the failure is rendered as the service's own problem+json (not FastAPI's 403).
api_key_header = APIKeyHeader(
    name=API_KEY_HEADER_NAME,
    scheme_name=API_KEY_SCHEME_NAME,
    description="Static API key issued to the client.",
    auto_error=False,
)


class ApiKeyAuth:
    """Dependency that lets a request through only if it carries the configured API key.

    Keys are expected to be ASCII, which is what HTTP headers are meant to carry.
    """

    def __init__(self, api_key: SecretStr) -> None:
        self._expected: Final = api_key.get_secret_value().encode("utf-8")

    async def __call__(self, provided: Annotated[str | None, Security(api_key_header)] = None) -> None:
        """Raise ``UnauthorizedError`` when the header is missing or does not match."""
        if provided is None or not secrets.compare_digest(provided.encode("utf-8"), self._expected):
            raise UnauthorizedError

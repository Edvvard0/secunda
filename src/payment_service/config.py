"""Application settings.

Twelve-factor style: every value comes from the environment (or a local ``.env`` file for development).
Settings are split into small groups, each with its own environment prefix, so that a group can be built
and tested in isolation. Secrets are ``SecretStr`` and therefore never appear in ``repr``/logs.
"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class _EnvSettings(BaseSettings):
    """Common behaviour for every settings group: immutable, ``.env`` aware, unknown keys ignored."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )


class DatabaseSettings(_EnvSettings):
    """PostgreSQL connection settings (``DB_*``)."""

    model_config = SettingsConfigDict(env_prefix="DB_")

    url: SecretStr
    """SQLAlchemy URL; must use the async driver, e.g. ``postgresql+asyncpg://user:pass@host/db``."""
    pool_size: int = Field(default=10, ge=1)
    max_overflow: int = Field(default=10, ge=0)
    pool_timeout_seconds: float = Field(default=30.0, gt=0)
    echo: bool = False

    @field_validator("url")
    @classmethod
    def _require_async_driver(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().startswith("postgresql+asyncpg://"):
            msg = "DB_URL must use the async driver: postgresql+asyncpg://..."
            raise ValueError(msg)
        return value


class RabbitSettings(_EnvSettings):
    """RabbitMQ connection and consumption settings (``RABBIT_*``)."""

    model_config = SettingsConfigDict(env_prefix="RABBIT_")

    url: SecretStr
    """AMQP URL, e.g. ``amqp://user:pass@host:5672/``."""
    prefetch_count: int = Field(default=10, ge=1)
    """Max unacknowledged messages per consumer: the concurrency limit of the consumer process."""
    queue_type: Literal["quorum", "classic"] = "quorum"
    """Quorum queues are replicated and recommended by RabbitMQ for data that must not be lost."""


class ApiSettings(_EnvSettings):
    """HTTP API settings (``API_*``)."""

    model_config = SettingsConfigDict(env_prefix="API_")

    key: SecretStr = Field(min_length=16)
    """Static key expected in the ``X-API-Key`` header."""
    docs_enabled: bool = True
    """Expose Swagger UI / ReDoc / OpenAPI JSON."""
    max_body_bytes: int = Field(default=65536, ge=1024)
    """Requests with a larger body are rejected with 413 before they are read into memory."""


class WebhookSettings(_EnvSettings):
    """Outgoing webhook settings (``WEBHOOK_*``)."""

    model_config = SettingsConfigDict(env_prefix="WEBHOOK_")

    signing_secret: SecretStr = Field(min_length=16)
    """HMAC-SHA256 key used to sign webhook bodies so receivers can verify the sender."""
    timeout_seconds: float = Field(default=5.0, gt=0)
    allow_private_networks: bool = False
    """SSRF guard switch. ``False`` (production): only globally routable addresses are allowed.

    ``True`` (local demos, tests, docker networks) additionally allows loopback, RFC 1918 / ULA private
    ranges and internal host names. Link-local and cloud-metadata addresses (169.254.0.0/16, ...) stay
    forbidden in both modes.
    """
    user_agent: str = "payment-service-webhook/1.0"


class RetrySettings(_EnvSettings):
    """Message retry policy (``RETRY_*``)."""

    model_config = SettingsConfigDict(env_prefix="RETRY_")

    max_attempts: int = Field(default=3, ge=1)
    """Total processing attempts (first try included) before a message goes to the DLQ."""
    base_delay_seconds: float = Field(default=5.0, gt=0)
    backoff_factor: float = Field(default=2.0, ge=1.0)
    """Delay before attempt ``n + 1`` is ``base_delay * backoff_factor ** (n - 1)``."""


class OutboxSettings(_EnvSettings):
    """Transactional outbox relay settings (``OUTBOX_*``)."""

    model_config = SettingsConfigDict(env_prefix="OUTBOX_")

    batch_size: int = Field(default=100, ge=1)
    poll_interval_seconds: float = Field(default=0.5, gt=0)
    error_backoff_seconds: float = Field(default=5.0, gt=0)
    """Pause after a failed relay iteration (broker or database down)."""
    retention_hours: float = Field(default=72.0, gt=0)
    """Published outbox rows older than this are purged."""
    purge_interval_seconds: float = Field(default=3600.0, gt=0)


class GatewaySettings(_EnvSettings):
    """Payment gateway emulation settings (``GATEWAY_*``)."""

    model_config = SettingsConfigDict(env_prefix="GATEWAY_")

    min_delay_seconds: float = Field(default=2.0, ge=0)
    max_delay_seconds: float = Field(default=5.0, ge=0)
    success_rate: float = Field(default=0.9, ge=0, le=1)


class LoggingSettings(_EnvSettings):
    """Logging settings (``LOG_*``)."""

    model_config = SettingsConfigDict(env_prefix="LOG_")

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    json_format: bool = True
    """Emit one JSON object per line (machine friendly); ``False`` gives human readable lines."""


class Settings(BaseModel):
    """Aggregate of all settings groups, built once per process in the composition root."""

    model_config = {"frozen": True}

    db: DatabaseSettings
    rabbit: RabbitSettings
    api: ApiSettings
    webhook: WebhookSettings
    retry: RetrySettings
    outbox: OutboxSettings
    gateway: GatewaySettings
    logging: LoggingSettings

    @classmethod
    def load(cls) -> Self:
        """Read every group from the environment, failing fast if a required value is missing."""
        return cls(
            db=DatabaseSettings(),
            rabbit=RabbitSettings(),
            api=ApiSettings(),
            webhook=WebhookSettings(),
            retry=RetrySettings(),
            outbox=OutboxSettings(),
            gateway=GatewaySettings(),
            logging=LoggingSettings(),
        )

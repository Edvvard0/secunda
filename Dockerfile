# syntax=docker/dockerfile:1.7
#
# One image, several processes: the default command starts the API; relay, consumer, migrate and the
# demo webhook sink reuse the same image with a different command (see docker-compose.yml).
#
# Build:  docker build -t payment-service:local .

# --------------------------------------------------------------------------- #
# Stage 1: builder - resolves and installs dependencies into a virtualenv.
# Nothing from this stage except /opt/venv and the migration files reaches the runtime image,
# so uv, the build cache and the sources stay out of the final layers.
# --------------------------------------------------------------------------- #
FROM python:3.12-slim-bookworm AS builder

# Pinned uv: copying the static binary is faster and more reproducible than pip-installing it.
# Keep in sync with the uv_build range in pyproject.toml ([build-system]).
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /bin/uv

# UV_PYTHON_DOWNLOADS=never + only-system: always build against the interpreter of the base image,
#   the runtime stage is the same image, so the venv's python symlink stays valid after the copy.
# UV_COMPILE_BYTECODE=1: precompile .pyc now; the runtime has PYTHONDONTWRITEBYTECODE=1 and (in compose)
#   a read-only root filesystem, so it could not cache them itself.
# UV_LINK_MODE=copy: the uv cache is a BuildKit cache mount (another filesystem), hardlinks are impossible.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON_PREFERENCE=only-system \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# Layer 1 - third-party dependencies only. Re-runs solely when pyproject.toml or uv.lock change,
# so editing application code never re-downloads the world.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --frozen --no-dev --no-install-project

# Layer 2 - the project itself. README.md is required because pyproject.toml declares it as the readme.
# --no-editable installs a real wheel into the venv: the runtime image needs no source tree.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# Files used by `alembic upgrade head` (the `migrate` service). Copied after the install so that a new
# migration does not invalidate the dependency/project layers above.
COPY alembic.ini ./
COPY migrations ./migrations

# --------------------------------------------------------------------------- #
# Stage 2: runtime - slim base, virtualenv, non-root user, nothing else.
# --------------------------------------------------------------------------- #
FROM python:3.12-slim-bookworm AS runtime

ARG VERSION=0.1.0
ARG REVISION=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="payment-service" \
      org.opencontainers.image.description="Asynchronous payment processing microservice: API, outbox relay and RabbitMQ consumer in one image" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.base.name="docker.io/library/python:3.12-slim-bookworm"

# PYTHONUNBUFFERED: log lines reach `docker logs` immediately instead of sitting in a buffer.
# PYTHONDONTWRITEBYTECODE: bytecode was precompiled in the builder; do not write into the image at runtime.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:${PATH}"

# Fixed numeric uid/gid: stable across rebuilds and verifiable by orchestrators (runAsNonRoot).
# No home directory and no login shell - the service never needs either.
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin app

WORKDIR /app

# Code and dependencies stay root-owned and read-only for the app user: a compromised process cannot
# modify the application it runs.
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app/alembic.ini ./alembic.ini
COPY --from=builder /app/migrations ./migrations

# `alembic upgrade head` works from /app: script_location is resolved relative to alembic.ini (%(here)s)
# and `prepend_sys_path = src` points to a directory that does not exist in this image (harmless) - the
# `payment_service` package is imported from /opt/venv/lib/python3.12/site-packages, where the wheel is installed.

USER 10001:10001

EXPOSE 8000

# Meaningful for the default (API) command only: relay/consumer/migrate disable it in docker-compose.yml.
# urllib instead of curl: no extra packages in the image. A non-2xx answer raises HTTPError -> exit code 1.
# /health/live is a pure liveness probe (no database access), so a database outage does not restart the API.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=3)"]

# --no-access-log:         the application has its own access-log middleware (JSON, with request id).
# --proxy-headers:         honour X-Forwarded-* from a reverse proxy; by default only from 127.0.0.1,
#                          widen explicitly with FORWARDED_ALLOW_IPS when a trusted proxy sits in front.
# --timeout-graceful-shutdown: finish in-flight requests on SIGTERM, but stay below the 30s stop_grace_period
#                          of docker-compose.yml so the container exits by itself instead of being SIGKILLed.
# --no-server-header:      do not advertise the server software.
CMD ["uvicorn", "payment_service.api.app:create_app", "--factory", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--no-access-log", "--proxy-headers", "--no-server-header", \
     "--timeout-graceful-shutdown", "25"]

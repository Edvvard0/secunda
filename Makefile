# Developer shortcuts. All Python tooling runs through `uv run`, so no manual virtualenv activation is needed.
# Works with GNU make on Linux/macOS and on Windows (Git Bash). Run `make` or `make help` for the target list.

.DEFAULT_GOAL := help

COMPOSE ?= docker compose

.PHONY: help install hooks lint format typecheck test test-integration test-all check \
        env up up-demo down logs migrate smoke migrate-local openapi clean

# Self-documenting help: every target with a trailing "## description" is listed.
help: ## Show this help
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-18s %s\n", $$1, $$2}'

# --------------------------------------------------------------------------- #
# Development
# --------------------------------------------------------------------------- #

install: ## Install the project with dev dependencies exactly as pinned in uv.lock
	uv sync --locked

hooks: ## Install the git pre-commit hook
	uv run pre-commit install

lint: ## Ruff lint and format check (changes nothing)
	uv run ruff check .
	uv run ruff format --check .

format: ## Apply ruff fixes and format the code
	uv run ruff check --fix .
	uv run ruff format .

typecheck: ## mypy --strict on src/
	uv run mypy

test: ## Unit tests (fast, no Docker needed)
	uv run pytest -m "not integration"

test-integration: ## Integration tests (need Docker, Postgres and RabbitMQ start via testcontainers)
	uv run pytest -m integration

test-all: ## All tests with a coverage report
	uv run pytest --cov --cov-report=term-missing

check: lint typecheck test ## Lint, type check and unit tests (CI minus Docker)

openapi: ## Export the OpenAPI schema to docs/openapi.json
	uv run python -m payment_service.tools.export_openapi

migrate-local: ## Apply migrations from the host (needs DB_URL in the environment or .env)
	uv run alembic upgrade head

# --------------------------------------------------------------------------- #
# Docker Compose
# --------------------------------------------------------------------------- #

env: ## Create .env from .env.example unless it exists (review the secrets afterwards)
	@test -f .env || { cp .env.example .env && echo "Created .env from .env.example"; }

up: ## Build the image and start the stack in the background
	$(COMPOSE) up --build -d

up-demo: ## Like up, plus the demo webhook sink (profile "demo")
	$(COMPOSE) --profile demo up --build -d

down: ## Stop and remove the containers, data volumes are kept
	$(COMPOSE) --profile demo down

logs: ## Follow the logs of all services
	$(COMPOSE) --profile demo logs -f --tail=100

migrate: ## Apply migrations inside the compose stack
	$(COMPOSE) run --rm migrate

smoke: ## End-to-end smoke test against the running demo stack (run make up-demo first)
	@set -a && . ./.env && set +a && uv run python scripts/smoke_test.py \
		--api-url http://localhost:$${API_HOST_PORT:-8000} \
		--sink-url http://localhost:$${WEBHOOK_SINK_HOST_PORT:-9000} \
		--rabbit-management-url http://localhost:$${RABBITMQ_UI_HOST_PORT:-15672}

# --------------------------------------------------------------------------- #
# Housekeeping
# --------------------------------------------------------------------------- #

clean: ## Remove caches, coverage reports and build artefacts
	rm -rf .ruff_cache .mypy_cache .pytest_cache htmlcov .coverage coverage.xml dist build
	uv run python -c "import pathlib, shutil; [shutil.rmtree(p) for d in ('src', 'tests', 'migrations') for p in pathlib.Path(d).rglob('__pycache__')]"

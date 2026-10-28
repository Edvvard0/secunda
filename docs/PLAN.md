# План разработки: асинхронный сервис процессинга платежей

> Тестовое задание Secunda. Источник требований — `redirect.pdf` (ТЗ) и описание вакансии.
> Детали устройства — в [ARCHITECTURE.md](ARCHITECTURE.md), запуск — в [README](../README.md).

## 1. Цель

Микросервис, который принимает запрос на оплату, **гарантированно** ставит его в очередь, обрабатывает
через эмуляцию платёжного шлюза и уведомляет клиента webhook-ом. Ключевая ценность решения — не набор
эндпоинтов, а **надёжность на стыке БД и брокера** (Outbox, идемпотентность, retry, DLQ).

## 2. Что компания хочет увидеть → чем это закрывается

| Ожидание (вакансия / критерии оценки ТЗ) | Как показываем |
|---|---|
| Архитектура и чистота кода, SOLID | Слои `api → services → persistence/messaging`, зависимости через абстракции (`Protocol`/ABC), общая логика в базовых классах (`BaseRepository`, `BackgroundWorker`, `AppError`, `MessagePublisher`) |
| Корректный Outbox | Платёж и событие пишутся **в одной транзакции**; отдельный relay (polling publisher, `FOR UPDATE SKIP LOCKED`, publisher confirms); at-least-once |
| Работа с RabbitMQ (очереди, обменники, DLQ) | Явная топология: `payments` / `payments.retry` / `payments.dlx`, quorum-очереди, wait-очередь на каждый уровень backoff, DLQ, декларация в коде |
| Идемпотентность | HTTP: `Idempotency-Key` + UNIQUE + отпечаток тела (повтор → тот же платёж, другой body → 422). Consumer: CAS-обновление статуса, повторная доставка безопасна |
| Обработка ошибок и retry | 3 попытки, экспоненциальная задержка (5 с → 10 с), счётчик попыток в заголовке, после лимита — DLQ с причиной |
| Docker-окружение | `docker compose up` поднимает postgres, rabbitmq, migrate, api, relay, consumer; healthchecks, `depends_on: service_healthy`, non-root, multi-stage |
| Асинхронность (event loop, конкурентность) | Полностью async стек; тяжёлое (sleep эмуляции) не держит транзакцию; `prefetch_count` ограничивает конкурентность; graceful shutdown |
| БД: схемы, индексы, оптимизация | Миграции Alembic, CHECK-ограничения, `NUMERIC(18,2)`, partial index для outbox, `EXPLAIN` в docs |
| Безопасность | `X-API-Key` (constant-time), SSRF-защита webhook, HMAC-подпись webhook, валидация входа, секреты только из env, non-root контейнер, `ruff S` (bandit) |
| Контрактное взаимодействие | OpenAPI (автогенерация + экспорт в `docs/openapi.json`), единый формат ошибок RFC 9457 |
| CI/CD, ревью, best practices | GitHub Actions (ruff, mypy, pytest, docker build), pre-commit, ADR с обоснованием выбора |

## 3. Допущения (сознательно принятые решения по неоднозначным местам ТЗ)

1. **Бизнес-отказ ≠ ошибка доставки.** 10 % «ошибок» шлюза — это итоговый статус `failed` и webhook об
   этом. Retry/DLQ относятся к *инфраструктурным* сбоям (webhook недоступен, БД, брокер).
2. **Retry на уровне брокера, а не `sleep` в обработчике** — не блокирует consumer, переживает рестарт.
   Любой не-2xx ответ webhook считается ошибкой доставки и ретраится (как в ТЗ: 3 попытки → DLQ).
3. **Outbox relay — отдельный процесс** того же образа (`relay`). Это добавляет сервис к списку из ТЗ
   (postgres, rabbitmq, api, consumer), но убирает связность api↔брокер. Допустимо запускать и внутри consumer.
4. `webhook_url` обязателен; `description` обязателен; `metadata` необязателен (по умолчанию `{}`).
5. `Idempotency-Key` уникален глобально (один статический API-ключ = один клиент). При появлении
   нескольких клиентов ключ станет составным `(client_id, key)` — описано в ARCHITECTURE.
6. `/health/*` не требуют `X-API-Key` (нужны оркестратору); все `/api/v1/*` — требуют.
7. Сеть: SSRF-защита блокирует приватные адреса. В compose для демо включён флаг
   `WEBHOOK_ALLOW_PRIVATE_NETWORKS=true`, чтобы webhook-sink в docker-сети принимал вызовы; по умолчанию — `false`.

## 4. Этапы

Каждый этап завершается зелёными `ruff check`, `ruff format --check`, `mypy`, `pytest`.

### Этап 0 — Скаффолд и качество (готово)
- [x] `uv`, Python 3.12, зависимости (актуальные версии: FastAPI 0.162, SQLAlchemy 2.1, FastStream 0.7)
- [x] `ruff` (lint + format, включая `D`, `S`, `DTZ`, `ASYNC`), `mypy --strict`, `pytest` (+asyncio, cov)

### Этап 1 — Фундамент
- [x] `config.py` — иерархия настроек (pydantic-settings, `SecretStr`), общий базовый класс
- [x] `domain/` — `Currency`, `PaymentStatus`, события, иерархия `AppError`
- [x] `persistence/` — модели `Payment`, `Outbox`, `Database`, `UnitOfWork`, `BaseRepository`
- [x] Alembic: async `env.py`, миграция `0001_initial`

### Этап 2 — Прикладные сервисы
- [x] `PaymentService` (создание с идемпотентностью + запись outbox; чтение)
- [x] `OutboxRelay` на базе `BackgroundWorker` (SKIP LOCKED, confirms, purge)
- [x] `PaymentProcessor` (идемпотентная обработка), `PaymentGateway` (эмуляция), `WebhookSender` (SSRF, HMAC)

### Этап 3 — Брокер
- [x] `Topology` (exchanges/queues/bindings/DLX/retry-очереди) — единый источник правды
- [x] `MessagePublisher` (persist + mandatory + confirms), `RetryRouter` (retry → wait-очередь → DLQ)
- [x] Consumer на FastStream, `AckPolicy.REJECT_ON_ERROR`, `prefetch_count`

### Этап 4 — HTTP API
- [x] `create_app()` factory, lifespan, `X-API-Key`, схемы Pydantic v2, ошибки RFC 9457
- [x] `POST /api/v1/payments` (202), `GET /api/v1/payments/{id}`, `/health/live|ready`
- [x] Request-id middleware, JSON-логирование с контекстом

### Этап 5 — Контейнеризация
- [x] `Dockerfile` (multi-stage, uv, non-root), `docker-compose.yml`, `.env.example`, `webhook-sink` (profile `demo`)
- [x] Healthchecks, `migrate` как one-shot, graceful stop

### Этап 6 — Тесты
- [x] Unit: схемы/SSRF, подпись, retry-политика, gateway, сервисы на фейках
- [x] API: ASGI-клиент, auth, идемпотентность, формат ошибок
- [x] Integration (testcontainers: Postgres + RabbitMQ): сквозной сценарий, дубль доставки, retry→DLQ, параллельные одинаковые ключи, SKIP LOCKED
- [x] Цель: покрытие ≥ 85 %, интеграционные помечены `@pytest.mark.integration`

### Этап 7 — Документация и приёмка
- [x] `README` (запуск, curl-примеры, как посмотреть DLQ), `ARCHITECTURE.md`, ADR, `docs/openapi.json`
- [x] CI (GitHub Actions), pre-commit
- [x] Финальный прогон: `docker compose up --build` + реальный сценарий руками

## 5. Порядок реализации

| Шаг | Что |
|---|---|
| 1. Фундамент | архитектура и контракты модулей, настройки, домен, персистентность, безопасность (этап 1) |
| 2. Независимые модули | API-слой; webhook/gateway; брокер-слой; Docker/CI; тесты — по непересекающимся файлам |
| 3. Приёмка | прогон `ruff`, `mypy`, `pytest`, ревью диффа, финальный сквозной прогон |

Правило: у каждого модуля есть контракт (файлы, сигнатуры, критерии приёмки, способ проверки);
модуль принимается **только** после зелёных `ruff`, `mypy`, `pytest` и ревью диффа.

## 6. Стратегия качества

- `ruff check` + `ruff format` — единый стиль, плюс security-правила (`S`) и запрет naive datetime (`DTZ`).
- `mypy --strict` на `src/` — типы как документация и защита от регрессий.
- Docstrings (Google-стиль) обязательны для публичных классов/функций — проверяет `ruff D`.
- Тестовая пирамида: много быстрых unit-тестов, меньше API-тестов, ключевые сценарии надёжности — на реальных
  Postgres и RabbitMQ (моки не доказывают поведение DLX/TTL/SKIP LOCKED).
- `pre-commit` и CI запускают то же, что разработчик локально.

## 7. Риски и как их закрываем

| Риск | Митигация |
|---|---|
| Дубль события из-за падения relay между publish и commit | Допустимо by design (at-least-once); consumer идемпотентен; тест на дубль |
| Потеря сообщения при retry (republish не удался) | Ack только после успешного republish; иначе исключение → redelivery брокером |
| «Вечный» pending | Индекс `(status, created_at)` + запрос в runbook; outbox хранит `attempts/last_error` |
| Docker daemon недоступен в среде разработки | Интеграционные тесты автоматически skip; финальная проверка compose выполняется отдельно и фиксируется в отчёте |
| Новые мажорные версии библиотек | API FastStream/SQLAlchemy сверены по установленным исходникам, версии зафиксированы в `uv.lock` |

## 8. Definition of Done

1. `docker compose up --build` → создаёт платёж, получает webhook, статус `succeeded|failed` в GET.
2. Повтор POST с тем же ключом не создаёт дубль; другой body → 422.
3. Недоступный webhook → 3 попытки с нарастающей паузой → сообщение в `payments.new.dlq` с причиной.
4. `ruff check`, `ruff format --check`, `mypy`, `pytest` — зелёные; CI-конфиг присутствует.
5. README позволяет запустить проект и воспроизвести все сценарии за 5 минут.

## 9. Статус и результаты приёмки

| Проверка | Результат |
|---|---|
| `ruff check` / `ruff format --check` | чисто (правила `D`, `S`, `DTZ`, `ASYNC`, `PL` и др.) |
| `mypy --strict` (48 файлов `src/`) | чисто |
| `pytest` (unit + integration + e2e, реальные PostgreSQL/RabbitMQ) | **1262 passed**, покрытие 94,6 % (порог 90 %) |
| `docker compose --profile demo up --build` + `scripts/smoke_test.py` | пройден: 202, идемпотентность, подпись webhook, retry 5 с → 10 с → DLQ |
| Graceful shutdown в контейнерах | relay и consumer — exit 0 на SIGTERM; полная остановка стенда ≈ 3 с |
| `EXPLAIN ANALYZE` на 1 млн строк outbox | опрос relay 0,68 мс с частичным индексом против 164 мс без него ([DATABASE.md](DATABASE.md)) |

Что **не** проверялось: workflow GitHub Actions ни разу не запускался (конфиг валидирован схемой, шаги — `bash -n`);
многоузловой RabbitMQ-кластер и переподключения брокера; нагрузочные тесты; Linux-хост (разработка и прогон — Windows + Docker Desktop).

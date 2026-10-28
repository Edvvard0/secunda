# Правила разработки

## Команды

```bash
uv sync                          # окружение (Python 3.12) из uv.lock
uv run ruff check .              # линтер (включает bandit-правила S, docstrings D, DTZ, ASYNC, ...)
uv run ruff format .             # форматирование
uv run mypy                      # strict-типизация src/
uv run pytest -m "not integration"   # быстрые тесты (без Docker)
uv run pytest                    # все тесты (интеграционные поднимают Postgres и RabbitMQ в Docker)
```

Перед любым коммитом/PR все четыре проверки должны быть зелёными. Тесты не «чинят» ослаблением проверок.

## Стиль кода

- Python 3.12, `from __future__ import annotations` в каждом модуле, полные аннотации типов (mypy `--strict`).
- Длина строки — 110. Форматирует `ruff format`, руками не выравниваем.
- Docstrings (Google-стиль, **на английском**) обязательны для публичных модулей, классов, функций и методов в `src/`.
  Комментарии объясняют «почему», а не «что». Тесты документируются именами (`test_<что>_<при каком условии>`).
- Идентификаторы — английские. README и `docs/` — русские.
- Время — только timezone-aware (`payment_service.common.time.utcnow()`); `datetime.now()` без tz и `utcnow()` из stdlib запрещены.
- Деньги — `Decimal`, в JSON — строкой; `float` для сумм не используется.
- `print` запрещён; логи — через `logging.getLogger(__name__)`, без секретов и персональных данных в сообщениях.
  Используем ленивое форматирование: `logger.info("x=%s", x)` или `extra={...}`.
- Исключения: ловим конкретные типы; `except Exception` допустим только на границе процесса/воркера с логированием и обязательным
  `# noqa: BLE001` с пояснением. Не «глотаем» ошибки молча.
- SOLID: зависимости передаются через конструктор (абстракции из `services/interfaces.py`), общая логика — в базовых классах,
  а не копируется. Нет глобального изменяемого состояния; singletons создаются только в composition root.
- Блокирующие вызовы (файловый I/O, `time.sleep`, синхронные HTTP/DB-клиенты) в `async`-коде запрещены.
- Безопасность: никаких секретов в коде/логах/образах; сравнение секретов — `secrets.compare_digest`; весь SQL — через SQLAlchemy;
  внешний ввод валидируется Pydantic на границе.

## Структура и зависимости

См. `docs/ARCHITECTURE.md`, раздел 3. `services/*` не импортирует FastAPI, FastStream, httpx и aio-pika; адаптеры реализуют
порты из `services/interfaces.py`.

## Тесты

- `tests/unit` — быстрые, без Docker и сети (сеть мокается `respx`, брокер/БД — заглушками или не используются).
- `tests/integration` — на реальных Postgres/RabbitMQ через testcontainers (`tests/conftest.py`: `database`, `clean_database`,
  `rabbit_url`, `make_settings`). Автоматически пропускаются, если Docker недоступен.
- Асинхронные тесты — `async def` без декоратора (`asyncio_mode = "auto"`, один event loop на сессию).
- Один тест — одно поведение; детерминизм: без `sleep` «на глаз» (используем ожидание условия с таймаутом), без реальной случайности.

## Git

Conventional Commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`), небольшие осмысленные коммиты.

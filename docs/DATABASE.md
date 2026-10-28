# База данных: схема, индексы, планы запросов

Схема создаётся миграцией Alembic `0001` ([migrations/versions](../migrations/versions)); отсутствие расхождений между
моделями SQLAlchemy и миграциями проверяет интеграционный тест `test_migrations.py` (`compare_metadata == []`).

## Таблицы и ограничения

| Таблица | Ключевые решения |
|---|---|
| `payments` | `id UUID` PK · `amount NUMERIC(18,2) CHECK (amount > 0)` (деньги — никогда не `float`) · `currency`/`status` — `VARCHAR` + `CHECK IN (...)` вместо `ENUM` (добавить значение = обычная миграция, без `ALTER TYPE`) · `idempotency_key UNIQUE` · `metadata JSONB` · все времена `TIMESTAMPTZ` |
| `outbox` | `id UUID` PK (он же `event_id` и `message_id`) · `payload JSONB` · `published_at IS NULL` ⇒ ждёт публикации · `attempts`/`last_error` для диагностики |

Имена ограничений детерминированы (`NAMING_CONVENTION`): `uq_payments_idempotency_key`, `ck_payments_amount_positive`,
`ix_outbox_unpublished`, … — ошибки БД и миграции читаются без гаданий.

## Индексы и почему именно они

| Индекс | Запрос, который он обслуживает |
|---|---|
| `uq_payments_idempotency_key` (UNIQUE) | идемпотентность: `INSERT … ON CONFLICT DO NOTHING` и поиск по ключу; уникальность гарантирует БД, а не код |
| `ix_outbox_unpublished (created_at) WHERE published_at IS NULL` — **частичный** | опрос relay: `… WHERE published_at IS NULL ORDER BY created_at, id LIMIT n FOR UPDATE SKIP LOCKED` |
| `ix_payments_status_created_at (status, created_at)` | мониторинг «зависших» `pending` |

## EXPLAIN (ANALYZE, BUFFERS)

Измерено на PostgreSQL 17 (docker), данные: 500 000 платежей, 1 000 000 строк outbox, из них 100 не опубликованы.
Скрипт воспроизводим: схема из миграции, наполнение через `generate_series`, `ANALYZE`.

### 1. Опрос outbox (горячий путь relay) — частичный индекс

```
Limit (actual rows=100)                      Buffers: shared hit=112 read=1
  -> LockRows
       -> Incremental Sort (Sort Key: created_at, id; Presorted Key: created_at)
            -> Index Scan using ix_outbox_unpublished on outbox (actual rows=100)
                 Filter: (published_at IS NULL)               Buffers: shared hit=3
Execution Time: 0.677 ms
```

**Без** частичного индекса (тот же запрос):

```
Limit (actual rows=100)                      Buffers: shared hit=11095 read=6247
  -> LockRows -> Sort -> Seq Scan on outbox (Rows Removed by Filter: 999900)
Execution Time: 164.390 ms
```

| | Время | Буферы | Размер индекса |
|---|---|---|---|
| с частичным индексом | **0,68 мс** | 113 | **16 КБ** |
| без него (Seq Scan) | 164 мс | 17 342 | — (таблица 135 МБ) |

Вывод: стоимость опроса определяется числом **неопубликованных** строк, а не размером таблицы; индекс остаётся крошечным,
потому что опубликованные строки в него не попадают. Это ключевое свойство, делающее polling-publisher рабочим решением
при миллионах накопленных событий (плюс очистка опубликованных строк по `OUTBOX_RETENTION_HOURS`).

### 2. Идемпотентность — поиск по ключу

```
Index Scan using uq_payments_idempotency_key on payments (actual rows=1)   Buffers: shared read=4
Execution Time: 0.148 ms
```

### 3. «Зависшие» платежи

```
Index Scan using ix_payments_status_created_at on payments (actual rows=10)
  Index Cond: (status = 'pending' AND created_at < now() - '00:05:00')
Execution Time: 0.515 ms
```

Запрос из [README, «Эксплуатация»](../README.md): индекс сразу отсекает 499 990 финальных платежей по `status`.

## Конкурентность

- `INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING` — гонка двух одинаковых запросов разрешается самой БД:
  проигравший ждёт коммита победителя, ничего не вставляет и читает его строку (уровень `READ COMMITTED` даёт свежий снимок).
- `UPDATE payments SET status=… WHERE id=:id AND status='pending'` — compare-and-set: повторная доставка сообщения не может
  перезаписать итоговый статус.
- `SELECT … FOR UPDATE SKIP LOCKED` — несколько relay берут непересекающиеся пачки и не блокируют друг друга.
- Внешние вызовы (шлюз, webhook) выполняются **вне** транзакций: транзакция/соединение не удерживаются на секунды, пул не исчерпывается
  (интеграционный тест проверяет отсутствие `idle in transaction` на время вызова).

## Масштабирование (когда понадобится)

- Партиционирование `payments` по `created_at` (RANGE, помесячно) + `DETACH` старых партиций вместо `DELETE`.
- Партиционирование `outbox` по `created_at` и `DROP PARTITION` вместо построчной очистки.
- Пробуждение relay через `LISTEN/NOTIFY` вместо опроса; при очень высоком RPS — CDC (Debezium) вместо polling.

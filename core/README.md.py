# PythonEE Core

Event-driven модульный монолит: `identity` / `catalog` / `orders` / `notifications`
(в этой итерации). Интеграция — только через Apache Kafka; HTTP — только
`/health` и `/ready`. Полная архитектура — [`../docs/architecture.md`](../docs/architecture.md).

**Статус:** каркас ядра завершён; сквозной сценарий работает end-to-end.
Клиенты (`clients/`, `packages/bff-kit`) — следующая итерация.

## Что уже работает

| Модуль | Kafka-команды (in) | События (out) |
|---|---|---|
| `catalog` | `catalog.commands.create-product.v1` | — (write-only) |
| `orders` | `orders.commands.create.v1`, `orders.commands.ship.v1` | `orders.events.order-placed.v1`, `orders.events.order-shipped.v1` |
| `notifications` | подписан на `orders.events.order-placed.v1`, `orders.events.order-shipped.v1` | — (только логирует) |
| `identity` | — | — (справочник) |

**Надёжность:** Transactional Outbox + идемпотентный consumer (dedup по `messageId`
через `processed_messages`) + best-effort DLQ (лог + commit).

---

## Требования

- Docker + Docker Compose (с Linux-движком для testcontainers)
- Python 3.12
- [`uv`](https://docs.astral.sh/uv/)

---

## Быстрый старт

```powershell
# 1. Поднять инфраструктуру (Kafka + Postgres + Kafka UI + Schema Registry)
cd E:\githab\PythonEE
docker compose up -d

# 2. Установить зависимости (из корня workspace)
uv sync

# 3. Прогнать миграции
cd core
$env:DATABASE_URL = "postgresql+asyncpg://python-ee:python-ee@localhost:5432/python-ee"
uv run alembic -c alembic.ini upgrade head

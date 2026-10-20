# ADR-0001: Kafka client — aiokafka

**Status:** Accepted (2026-10-20)

## Context

Ядро — async-монолит: FastAPI + SQLAlchemy 2.0 async + async outbox relay + async consumers.
Нужен Kafka-клиент, который живёт в том же event loop, что и остальные I/O-операции.
Требования итерации: producer, consumer с ручным commit, keyed-сообщения, DLQ-публикация.

## Decision

Используем **aiokafka**.

## Alternatives considered

| Клиент | Плюсы | Минусы |
|---|---|---|
| **aiokafka** | native async, чистый Python, совпадает с async-стеком | нет встроенного Schema Registry, медленнее librdkafka |
| confluent-kafka | быстрый (librdkafka), богатый Schema Registry API | sync API → нужны потоки/executor, ломает async-модель |
| kafka-python | — | заброшен, не поддерживать |

## Consequences

- **Положительно:** единый event loop, простой graceful shutdown, тесты с testcontainers проще.
- **Отрицательно:** Schema Registry пока не подключён — сериализация JSON, поле `SCHEMA_REGISTRY_URL` зарезервировано. TODO на следующую итерацию: `fastavro` + HTTP-клиент к Schema Registry либо вынос producer в отдельный процесс на confluent-kafka.
- **Производительность:** ниже, чем у librdkafka, но для каркаса и ожидаемых нагрузок — приемлемо. Пересмотреть при появлении реальных метрик.

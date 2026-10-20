# ADR-0002: Kafka as integration backbone

## Status
Accepted — 2026-10-20

## Context
Нужна слабая связанность ядра и клиентов, возможность добавлять
новые клиенты без изменений в ядре, устойчивость к сбоям.

## Decision
Вся интеграция — через Apache Kafka. Команды — `*.commands.*`,
события — `*.events.*`. Schema Registry + Protobuf.

## Consequences
+ Слабая связанность
+ Легко добавить клиента или модуль
+ Воспроизводимость событий (retention)
- Сложнее отладка (нужны инструменты: Kafka UI, трассировка)
- Нужна инфраструктура (Schema Registry, Outbox)

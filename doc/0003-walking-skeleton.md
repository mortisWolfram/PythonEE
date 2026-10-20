# ADR-0003: Walking skeleton first

## Status
Accepted — 2026-10-20

## Context
Много архитектурных решений (Kafka, Outbox, manifest,
capability-модель). Хотим проверить их дёшево до того, как
накопится код.

## Decision
Первый этап — один тонкий сценарий (`create order`) через все слои.
Без catalog, payments, seller/courier BFF. Инфраструктура
(Outbox, idempotency, Schema Registry) вводится постепенно,
по мере необходимости.

## Consequences
+ Раннее выявление интеграционных проблем
+ Быстрый фидбек
+ Понятный scope
- Часть «полной» архитектуры откладывается

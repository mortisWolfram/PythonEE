# ADR-0002: Dependency injection — plain factory functions

**Status:** Accepted (2026-10-20)

## Context

Модульный монолит, все модули в одном процессе. Нужен DI для: репозиториев (зависят от AsyncSession),
UnitOfWork, producer/consumer, outbox relay, командных хендлеров. Хочется минимум магии и
прозрачное тестирование (подмена в тестах).

## Decision

**Ручной composition root** в точках входа (`main.py`, entrypoint консьюмера) + набор
`build_*()` фабричных функций в `infrastructure/` каждого модуля. Никаких DI-фреймворков.

Пример:
```python
# core/src/modules/orders/infrastructure/factories.py
def build_order_unit_of_work(session_factory) -> OrderUnitOfWork: ...
def build_create_order_handler(uow_factory, producer) -> CreateOrderHandler: ...

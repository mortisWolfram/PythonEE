from __future__ import annotations

from abc import abstractmethod

from modules.orders.domain.repository import OrderRepository
from shared.application.unit_of_work import UnitOfWork
from shared.outbox.repository import OutboxRepository


class OrderUnitOfWork(UnitOfWork):
    """Transaction boundary for order aggregates + outbox."""

    @property
    @abstractmethod
    def orders(self) -> OrderRepository: ...

    @property
    @abstractmethod
    def outbox(self) -> OutboxRepository: ...

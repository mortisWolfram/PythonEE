from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from shared.domain.aggregate_root import AggregateRoot

TAggregate = TypeVar("TAggregate", bound=AggregateRoot[Any])
TId = TypeVar("TId")


class Repository(Generic[TAggregate, TId], ABC):
    """Port for aggregate persistence. Concrete impls live in infrastructure/."""

    @abstractmethod
    async def add(self, aggregate: TAggregate) -> None: ...

    @abstractmethod
    async def get(self, id: TId) -> TAggregate | None: ...

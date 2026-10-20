from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from shared.application.query import Query

TQuery = TypeVar("TQuery", bound=Query)
TResult = TypeVar("TResult")


class QueryHandler(Generic[TQuery, TResult], ABC):
    @abstractmethod
    async def handle(self, query: TQuery) -> TResult: ...

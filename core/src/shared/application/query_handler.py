from __future__ import annotations

from abc import ABC, abstractmethod

from shared.application.query import Query


class QueryHandler[TQuery: Query, TResult](ABC):
    @abstractmethod
    async def handle(self, query: TQuery) -> TResult: ...

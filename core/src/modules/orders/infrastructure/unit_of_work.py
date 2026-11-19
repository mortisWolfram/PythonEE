from __future__ import annotations

from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from modules.orders.application.unit_of_work import OrderUnitOfWork
from modules.orders.domain.repository import OrderRepository
from modules.orders.infrastructure.repository import SQLAlchemyOrderRepository
from shared.outbox.repository import OutboxRepository


class SQLAlchemyOrderUnitOfWork(OrderUnitOfWork):
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None
        self._orders: OrderRepository | None = None
        self._outbox: OutboxRepository | None = None

    async def __aenter__(self) -> Self:
        self._session = self._session_factory()
        self._orders = SQLAlchemyOrderRepository(self._session)
        self._outbox = OutboxRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._session is None:
            return
        try:
            if exc_type is not None:
                await self._session.rollback()
        finally:
            await self._session.close()
            self._session = None
            self._orders = None
            self._outbox = None

    @property
    def orders(self) -> OrderRepository:
        if self._orders is None:
            raise RuntimeError("UnitOfWork used outside 'async with' block")
        return self._orders

    @property
    def outbox(self) -> OutboxRepository:
        if self._outbox is None:
            raise RuntimeError("UnitOfWork used outside 'async with' block")
        return self._outbox

    async def commit(self) -> None:
        if self._session is None:
            raise RuntimeError("UnitOfWork used outside 'async with' block")
        await self._session.commit()

    async def rollback(self) -> None:
        if self._session is None:
            raise RuntimeError("UnitOfWork used outside 'async with' block")
        await self._session.rollback()

from __future__ import annotations

from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from modules.catalog.application.unit_of_work import ProductUnitOfWork
from modules.catalog.domain.repository import ProductRepository
from modules.catalog.infrastructure.repository import SQLAlchemyProductRepository


class SQLAlchemyProductUnitOfWork(ProductUnitOfWork):
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None
        self._products: ProductRepository | None = None

    async def __aenter__(self) -> Self:
        self._session = self._session_factory()
        self._products = SQLAlchemyProductRepository(self._session)
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
            self._products = None

    @property
    def products(self) -> ProductRepository:
        if self._products is None:
            raise RuntimeError("UnitOfWork used outside 'async with' block")
        return self._products

    async def commit(self) -> None:
        if self._session is None:
            raise RuntimeError("UnitOfWork used outside 'async with' block")
        await self._session.commit()

    async def rollback(self) -> None:
        if self._session is None:
            raise RuntimeError("UnitOfWork used outside 'async with' block")
        await self._session.rollback()

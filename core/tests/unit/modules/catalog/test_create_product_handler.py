from __future__ import annotations

from types import TracebackType
from typing import Self
from uuid import UUID, uuid4

from modules.catalog.application.commands import CreateProduct
from modules.catalog.application.handlers import CreateProductHandler
from modules.catalog.application.unit_of_work import ProductUnitOfWork
from modules.catalog.domain.entities import Product
from modules.catalog.domain.repository import ProductRepository
from modules.catalog.domain.value_objects import ProductId


class _Storage:
    def __init__(self) -> None:
        self.products: dict[UUID, Product] = {}
        self.commits = 0
        self.rollbacks = 0


class _FakeProductRepository(ProductRepository):
    def __init__(self, storage: _Storage) -> None:
        self._s = storage

    async def add(self, product: Product) -> None:
        self._s.products[product.id.value] = product

    async def get(self, id: ProductId) -> Product | None:
        return self._s.products.get(id.value)

    async def get_or_raise(self, id: ProductId) -> Product:
        product = await self.get(id)
        if product is None:
            raise LookupError(str(id.value))
        return product


class _FakeUoW(ProductUnitOfWork):
    def __init__(self, storage: _Storage) -> None:
        self._storage = storage
        self._products = _FakeProductRepository(storage)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            self._storage.rollbacks += 1

    @property
    def products(self) -> ProductRepository:
        return self._products

    async def commit(self) -> None:
        self._storage.commits += 1

    async def rollback(self) -> None:
        self._storage.rollbacks += 1


async def test_handle_persists_product() -> None:
    storage = _Storage()
    handler = CreateProductHandler(lambda: _FakeUoW(storage))
    command = CreateProduct(
        message_id=uuid4(),
        correlation_id=uuid4(),
        tenant_id="tenant-1",
        seller_id=uuid4(),
        name="Widget",
        price="100.00",
        quantity=5,
    )

    await handler.handle(command)

    assert len(storage.products) == 1
    assert storage.commits == 1
    product = next(iter(storage.products.values()))
    assert product.name.value == "Widget"
    assert product.quantity == 5
    assert product.tenant_id == "tenant-1"

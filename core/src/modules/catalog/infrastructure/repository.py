from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.catalog.domain.entities import Product
from modules.catalog.domain.exceptions import ProductNotFoundError
from modules.catalog.domain.repository import ProductRepository
from modules.catalog.domain.value_objects import (
    Money,
    ProductId,
    ProductName,
    SellerId,
)
from modules.catalog.infrastructure.models import ProductModel


class SQLAlchemyProductRepository(ProductRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, product: Product) -> None:
        self._session.add(_to_model(product))

    async def get(self, id: ProductId) -> Product | None:
        stmt = select(ProductModel).where(ProductModel.id == id.value)
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return _to_domain(row) if row is not None else None

    async def get_or_raise(self, id: ProductId) -> Product:
        product = await self.get(id)
        if product is None:
            raise ProductNotFoundError(str(id.value))
        return product


def _to_model(product: Product) -> ProductModel:
    return ProductModel(
        id=product.id.value,
        seller_id=product.seller_id.value,
        tenant_id=product.tenant_id,
        name=product.name.value,
        price_amount=product.price.amount,
        price_currency=product.price.currency,
        quantity=product.quantity,
    )


def _to_domain(model: ProductModel) -> Product:
    return Product(
        id=ProductId(model.id),
        seller_id=SellerId(model.seller_id),
        name=ProductName(model.name),
        price=Money(model.price_amount, model.price_currency),
        quantity=model.quantity,
        tenant_id=model.tenant_id,
    )

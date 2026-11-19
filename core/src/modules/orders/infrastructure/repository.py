from __future__ import annotations

from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from modules.orders.domain.entities import Order, OrderItem
from modules.orders.domain.exceptions import OrderNotFoundError
from modules.orders.domain.repository import OrderRepository
from modules.orders.domain.value_objects import (
    CustomerId,
    Money,
    OrderId,
    OrderStatus,
    ProductId,
    SellerId,
)
from modules.orders.infrastructure.models import OrderItemModel, OrderModel


class SQLAlchemyOrderRepository(OrderRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, order: Order) -> None:
        self._session.add(_to_model(order))

    async def update(self, order: Order) -> None:
        stmt = (
            select(OrderModel)
            .where(OrderModel.id == order.id.value)
            .options(selectinload(OrderModel.items))
        )
        result = await self._session.execute(stmt)
        existing = result.scalar_one_or_none()
        if existing is None:
            self._session.add(_to_model(order))
            return

        existing.status = order.status.value
        existing.shipped_at = order.shipped_at

        by_product = {i.product_id.value: i for i in order.items}
        for item_model in existing.items:
            domain_item = by_product.get(item_model.product_id)
            if domain_item is not None:
                item_model.shipped = domain_item.shipped

    async def get(self, id: OrderId) -> Order | None:
        stmt = (
            select(OrderModel)
            .where(OrderModel.id == id.value)
            .options(selectinload(OrderModel.items))
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return _to_domain(row) if row is not None else None

    async def get_or_raise(self, id: OrderId) -> Order:
        order = await self.get(id)
        if order is None:
            raise OrderNotFoundError(str(id.value))
        return order


def _to_model(order: Order) -> OrderModel:
    return OrderModel(
        id=order.id.value,
        customer_id=order.customer_id.value,
        tenant_id=order.tenant_id,
        status=order.status.value,
        created_at=order.created_at,
        shipped_at=order.shipped_at,
        items=[
            OrderItemModel(
                id=uuid4(),
                product_id=i.product_id.value,
                seller_id=i.seller_id.value,
                quantity=i.quantity,
                unit_price_amount=i.unit_price.amount,
                unit_price_currency=i.unit_price.currency,
                shipped=i.shipped,
            )
            for i in order.items
        ],
    )


def _to_domain(model: OrderModel) -> Order:
    return Order(
        id=OrderId(model.id),
        customer_id=CustomerId(model.customer_id),
        items=[
            OrderItem(
                product_id=ProductId(i.product_id),
                seller_id=SellerId(i.seller_id),
                quantity=i.quantity,
                unit_price=Money(amount=i.unit_price_amount, currency=i.unit_price_currency),
                shipped=i.shipped,
            )
            for i in model.items
        ],
        status=OrderStatus(model.status),
        created_at=model.created_at,
        shipped_at=model.shipped_at,
        tenant_id=model.tenant_id,
    )

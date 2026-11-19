from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from modules.orders.domain.entities import Order, OrderItem
from modules.orders.domain.exceptions import OrderNotFoundError
from modules.orders.domain.value_objects import (
    CustomerId,
    Money,
    OrderId,
    OrderStatus,
    ProductId,
    SellerId,
)
from modules.orders.infrastructure.repository import SQLAlchemyOrderRepository

pytestmark = pytest.mark.integration


def _order(*, two_sellers: bool = False) -> Order:
    items = [
        OrderItem(
            product_id=ProductId(uuid4()),
            seller_id=SellerId(uuid4()),
            quantity=2,
            unit_price=Money.of("100.00"),
        ),
    ]
    if two_sellers:
        items.append(
            OrderItem(
                product_id=ProductId(uuid4()),
                seller_id=SellerId(uuid4()),
                quantity=1,
                unit_price=Money.of("50.00"),
            )
        )
    return Order.create(
        customer_id=CustomerId(uuid4()),
        items=items,
        tenant_id="tenant-42",
    )


async def test_add_then_get_roundtrip(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order = _order(two_sellers=True)
    order.pull_events()  # discard OrderPlaced

    async with session_factory() as session:
        repo = SQLAlchemyOrderRepository(session)
        await repo.add(order)
        await session.commit()

    async with session_factory() as session:
        repo = SQLAlchemyOrderRepository(session)
        loaded = await repo.get(order.id)

    assert loaded is not None
    assert loaded.id == order.id
    assert loaded.customer_id == order.customer_id
    assert loaded.tenant_id == "tenant-42"
    assert loaded.status is OrderStatus.CREATED
    assert loaded.shipped_at is None
    assert len(loaded.items) == 2
    assert {i.product_id for i in loaded.items} == {i.product_id for i in order.items}
    assert all(i.shipped is False for i in loaded.items)
    assert loaded.total_amount() == order.total_amount()


async def test_get_unknown_returns_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        repo = SQLAlchemyOrderRepository(session)
        assert await repo.get(OrderId(uuid4())) is None


async def test_get_or_raise_unknown_raises(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        repo = SQLAlchemyOrderRepository(session)
        with pytest.raises(OrderNotFoundError):
            await repo.get_or_raise(OrderId(uuid4()))


async def test_update_after_ship(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order = _order()
    (seller,) = order.sellers()
    order.pull_events()

    async with session_factory() as session:
        repo = SQLAlchemyOrderRepository(session)
        await repo.add(order)
        await session.commit()

    async with session_factory() as session:
        repo = SQLAlchemyOrderRepository(session)
        loaded = await repo.get_or_raise(order.id)
        loaded.ship(seller)
        await repo.update(loaded)
        await session.commit()

    async with session_factory() as session:
        repo = SQLAlchemyOrderRepository(session)
        reloaded = await repo.get_or_raise(order.id)

    assert reloaded.status is OrderStatus.SHIPPED
    assert reloaded.shipped_at is not None
    assert all(i.shipped is True for i in reloaded.items)

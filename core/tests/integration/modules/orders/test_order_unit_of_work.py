from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from modules.orders.application.outbox import to_outbox
from modules.orders.domain.entities import Order, OrderItem
from modules.orders.domain.value_objects import (
    CustomerId,
    Money,
    ProductId,
    SellerId,
)
from modules.orders.infrastructure.models import OrderItemModel, OrderModel
from modules.orders.infrastructure.unit_of_work import SQLAlchemyOrderUnitOfWork
from shared.outbox.model import OutboxMessage

pytestmark = pytest.mark.integration


def _order_with_outbox() -> tuple[Order, OutboxMessage]:
    order = Order.create(
        customer_id=CustomerId(uuid4()),
        items=[
            OrderItem(
                product_id=ProductId(uuid4()),
                seller_id=SellerId(uuid4()),
                quantity=1,
                unit_price=Money.of("100.00"),
            )
        ],
        tenant_id="tenant-1",
    )
    (event,) = order.pull_events()
    msg = to_outbox(
        event,
        aggregate_id=str(order.id.value),
        correlation_id=uuid4(),
        causation_id=uuid4(),
        tenant_id="tenant-1",
    )
    return order, msg


async def test_uow_commits_order_and_outbox_together(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order, msg = _order_with_outbox()

    async with SQLAlchemyOrderUnitOfWork(session_factory) as uow:
        await uow.orders.add(order)
        await uow.outbox.add(msg)
        await uow.commit()

    async with session_factory() as session:
        orders = (await session.execute(select(OrderModel))).scalars().all()
        items = (await session.execute(select(OrderItemModel))).scalars().all()
        outbox = (await session.execute(select(OutboxMessage))).scalars().all()

    assert len(orders) == 1
    assert len(items) == 1
    assert len(outbox) == 1
    assert outbox[0].aggregate_id == str(order.id.value)
    assert outbox[0].topic == "orders.events.order-placed.v1"


async def test_uow_rolls_back_on_exception(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order, _msg = _order_with_outbox()

    with pytest.raises(RuntimeError, match="boom"):
        async with SQLAlchemyOrderUnitOfWork(session_factory) as uow:
            await uow.orders.add(order)
            raise RuntimeError("boom")

    async with session_factory() as session:
        orders = (await session.execute(select(OrderModel))).scalars().all()

    assert orders == []

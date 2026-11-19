from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from modules.orders.application.handlers import CreateOrderHandler, ShipOrderHandler
from modules.orders.infrastructure.models import OrderModel
from modules.orders.infrastructure.unit_of_work import SQLAlchemyOrderUnitOfWork
from modules.orders.presentation.kafka_consumer import (
    OrdersCreateEnvelopeHandler,
    OrdersShipEnvelopeHandler,
)
from shared.kafka.envelope import EventEnvelope
from shared.outbox.model import OutboxMessage

pytestmark = pytest.mark.integration


def _create_envelope(*, seller_id: UUID, product_id: UUID) -> EventEnvelope:
    return EventEnvelope.new(
        type="orders.commands.create",
        version=1,
        producer="test",
        payload={
            "customerId": str(uuid4()),
            "items": [
                {
                    "productId": str(product_id),
                    "sellerId": str(seller_id),
                    "quantity": 1,
                    "unitPrice": "100.00",
                }
            ],
        },
        tenant_id="tenant-42",
    )


async def _seed_order(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    seller_id: UUID,
    product_id: UUID,
) -> UUID:
    create_handler = CreateOrderHandler(
        lambda: SQLAlchemyOrderUnitOfWork(session_factory)
    )
    envelope = _create_envelope(seller_id=seller_id, product_id=product_id)
    await OrdersCreateEnvelopeHandler(create_handler)(envelope)

    async with session_factory() as session:
        order = (
            await session.execute(select(OrderModel))
        ).scalars().one()
        return order.id


async def test_ship_envelope_emits_order_shipped_outbox(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    seller = uuid4()
    product = uuid4()
    order_id = await _seed_order(
        session_factory, seller_id=seller, product_id=product
    )

    # Clear outbox of the create event so we can distinguish.
    async with session_factory() as session:
        msgs = (await session.execute(select(OutboxMessage))).scalars().all()
        for m in msgs:
            await session.delete(m)
        await session.commit()

    ship_envelope = EventEnvelope.new(
        type="orders.commands.ship",
        version=1,
        producer="test",
        payload={
            "orderId": str(order_id),
            "sellerId": str(seller),
        },
        tenant_id="tenant-42",
    )

    ship_handler = ShipOrderHandler(
        lambda: SQLAlchemyOrderUnitOfWork(session_factory)
    )
    await OrdersShipEnvelopeHandler(ship_handler)(ship_envelope)

    async with session_factory() as session:
        order = (
            await session.execute(select(OrderModel))
        ).scalars().one()
        outbox = (await session.execute(select(OutboxMessage))).scalars().all()

    assert order.status == "shipped"
    assert order.shipped_at is not None
    assert len(outbox) == 1
    msg = outbox[0]
    assert msg.event_type == "orders.events.order-shipped"
    assert msg.topic == "orders.events.order-shipped.v1"
    assert msg.aggregate_id == str(order_id)
    assert msg.payload["causation_id"] == str(ship_envelope.message_id)
    assert msg.payload["payload"]["orderId"] == str(order_id)
    assert msg.payload["payload"]["sellerId"] == str(seller)

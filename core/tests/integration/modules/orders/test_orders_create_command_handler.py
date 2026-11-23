from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from modules.orders.application.handlers import CreateOrderHandler
from modules.orders.infrastructure.models import OrderModel
from modules.orders.infrastructure.unit_of_work import SQLAlchemyOrderUnitOfWork
from modules.orders.presentation.kafka_consumer import OrdersCreateEnvelopeHandler
from shared.kafka.envelope import EventEnvelope
from shared.outbox.model import OutboxMessage

pytestmark = pytest.mark.integration


async def test_envelope_persists_order_and_outbox(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    customer = uuid4()
    product = uuid4()
    seller = uuid4()

    envelope = EventEnvelope.new(
        type="orders.commands.create",
        version=1,
        producer="test",
        payload={
            "customerId": str(customer),
            "items": [
                {
                    "productId": str(product),
                    "sellerId": str(seller),
                    "quantity": 2,
                    "unitPrice": "100.00",
                }
            ],
        },
        tenant_id="tenant-42",
    )

    handler = CreateOrderHandler(lambda: SQLAlchemyOrderUnitOfWork(session_factory))
    envelope_handler = OrdersCreateEnvelopeHandler(handler)

    await envelope_handler(envelope)

    async with session_factory() as session:
        orders = (await session.execute(select(OrderModel))).scalars().all()
        outbox = (await session.execute(select(OutboxMessage))).scalars().all()

    assert len(orders) == 1
    assert len(outbox) == 1

    order = orders[0]
    msg = outbox[0]

    assert str(order.customer_id) == str(customer)
    assert order.tenant_id == "tenant-42"

    assert msg.event_type == "orders.events.order-placed"
    assert msg.topic == "orders.events.order-placed.v1"
    assert msg.aggregate_id == str(order.id)
    assert msg.payload["causation_id"] == str(envelope.message_id)
    assert msg.payload["correlation_id"] == str(envelope.correlation_id)
    assert msg.payload["tenant_id"] == "tenant-42"
    assert msg.payload["producer"] == "core.orders"
    assert msg.payload["payload"]["customerId"] == str(customer)
    assert msg.payload["payload"]["totalAmount"] == {
        "amount": "200.00",
        "currency": "RUB",
    }

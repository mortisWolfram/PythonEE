from __future__ import annotations

from uuid import uuid4

from modules.orders.application.commands import CreateOrder, ShipOrder
from modules.orders.presentation.kafka_consumer import (
    OrdersCreateEnvelopeHandler,
    OrdersShipEnvelopeHandler,
)
from shared.kafka.envelope import EventEnvelope


class _CapturingCreateHandler:
    def __init__(self) -> None:
        self.commands: list[CreateOrder] = []

    async def handle(self, command: CreateOrder) -> None:
        self.commands.append(command)


class _CapturingShipHandler:
    def __init__(self) -> None:
        self.commands: list[ShipOrder] = []

    async def handle(self, command: ShipOrder) -> None:
        self.commands.append(command)


async def test_create_envelope_maps_to_command() -> None:
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
        capabilities=["orders.order.create"],
    )

    handler = _CapturingCreateHandler()
    await OrdersCreateEnvelopeHandler(handler)(envelope)  # type: ignore[arg-type]

    assert len(handler.commands) == 1
    cmd = handler.commands[0]
    assert cmd.message_id == envelope.message_id
    assert cmd.correlation_id == envelope.correlation_id
    assert cmd.tenant_id == "tenant-42"
    assert cmd.customer_id == customer
    assert len(cmd.items) == 1
    assert cmd.items[0].product_id == product
    assert cmd.items[0].seller_id == seller
    assert cmd.items[0].quantity == 2
    assert cmd.items[0].unit_price == "100.00"


async def test_ship_envelope_maps_to_command() -> None:
    order = uuid4()
    seller = uuid4()
    envelope = EventEnvelope.new(
        type="orders.commands.ship",
        version=1,
        producer="test",
        payload={
            "orderId": str(order),
            "sellerId": str(seller),
        },
    )

    handler = _CapturingShipHandler()
    await OrdersShipEnvelopeHandler(handler)(envelope)  # type: ignore[arg-type]

    assert len(handler.commands) == 1
    cmd = handler.commands[0]
    assert cmd.order_id == order
    assert cmd.seller_id == seller
    assert cmd.message_id == envelope.message_id
    assert cmd.tenant_id is None

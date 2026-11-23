from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from modules.orders.application.outbox import (
    ORDER_PLACED_TOPIC,
    ORDER_SHIPPED_TOPIC,
    PRODUCER_NAME,
    to_outbox,
    topic_for_event,
)
from modules.orders.domain.events import (
    OrderPlaced,
    OrderPlacedItem,
    OrderShipped,
)
from modules.orders.domain.value_objects import (
    CustomerId,
    Money,
    OrderId,
    ProductId,
    SellerId,
)


def _order_placed_event() -> OrderPlaced:
    return OrderPlaced(
        event_type="orders.events.order-placed",
        occurred_at=datetime.now(UTC),
        order_id=OrderId(uuid4()),
        customer_id=CustomerId(uuid4()),
        items=(
            OrderPlacedItem(
                product_id=ProductId(uuid4()),
                seller_id=SellerId(uuid4()),
                quantity=2,
                unit_price=Money.of("100.00"),
            ),
        ),
        total_amount=Money.of("200.00"),
        placed_at=datetime.now(UTC),
    )


def _order_shipped_event() -> OrderShipped:
    return OrderShipped(
        event_type="orders.events.order-shipped",
        occurred_at=datetime.now(UTC),
        order_id=OrderId(uuid4()),
        seller_id=SellerId(uuid4()),
        shipped_item_ids=(ProductId(uuid4()),),
        shipped_at=datetime.now(UTC),
    )


def test_topic_for_known_events() -> None:
    assert topic_for_event("orders.events.order-placed") == ORDER_PLACED_TOPIC
    assert topic_for_event("orders.events.order-shipped") == ORDER_SHIPPED_TOPIC


def test_topic_for_unknown_event_raises() -> None:
    with pytest.raises(ValueError, match="no topic mapping"):
        topic_for_event("orders.events.unknown")


def test_to_outbox_order_placed() -> None:
    event = _order_placed_event()
    command_message_id = uuid4()
    correlation_id = uuid4()

    msg = to_outbox(
        event,
        aggregate_id=str(event.order_id.value),
        correlation_id=correlation_id,
        causation_id=command_message_id,
        tenant_id="tenant-1",
    )

    assert msg.event_type == "orders.events.order-placed"
    assert msg.topic == ORDER_PLACED_TOPIC
    assert msg.aggregate_id == str(event.order_id.value)
    assert msg.published_at is None
    assert msg.attempts == 0

    payload = msg.payload
    assert payload["type"] == "orders.events.order-placed"
    assert payload["version"] == 1
    assert payload["producer"] == PRODUCER_NAME
    assert payload["tenant_id"] == "tenant-1"
    assert UUID(payload["correlation_id"]) == correlation_id
    assert UUID(payload["causation_id"]) == command_message_id
    assert UUID(payload["message_id"])  # generated

    body = payload["payload"]
    assert body["orderId"] == str(event.order_id.value)
    assert body["customerId"] == str(event.customer_id.value)
    assert body["totalAmount"] == {"amount": "200.00", "currency": "RUB"}
    assert len(body["items"]) == 1
    item = body["items"][0]
    assert item["quantity"] == 2
    assert item["unitPrice"] == {"amount": "100.00", "currency": "RUB"}
    assert "productId" in item
    assert "sellerId" in item


def test_to_outbox_order_shipped() -> None:
    event = _order_shipped_event()

    msg = to_outbox(
        event,
        aggregate_id=str(event.order_id.value),
        correlation_id=uuid4(),
        causation_id=uuid4(),
        tenant_id=None,
    )

    assert msg.event_type == "orders.events.order-shipped"
    assert msg.topic == ORDER_SHIPPED_TOPIC
    body = msg.payload["payload"]
    assert body["orderId"] == str(event.order_id.value)
    assert body["sellerId"] == str(event.seller_id.value)
    assert body["shippedItemIds"] == [str(p.value) for p in event.shipped_item_ids]
    assert "shippedAt" in body


def test_to_outbox_unknown_event_raises() -> None:
    # Build a DomainEvent subclass that has no topic mapping.
    class _Unknown:
        event_type = "orders.events.unknown"
        occurred_at = datetime.now(UTC)

    from shared.domain.event import DomainEvent

    unknown = DomainEvent(
        event_type="orders.events.unknown",
        occurred_at=datetime.now(UTC),
    )
    with pytest.raises(ValueError, match="unsupported event type"):
        to_outbox(
            unknown,
            aggregate_id="x",
            correlation_id=uuid4(),
            causation_id=uuid4(),
            tenant_id=None,
        )

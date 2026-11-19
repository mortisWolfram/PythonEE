from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from modules.orders.domain.events import OrderPlaced, OrderShipped
from modules.orders.domain.value_objects import Money
from shared.domain.event import DomainEvent
from shared.kafka.envelope import EventEnvelope
from shared.outbox.model import OutboxMessage

ORDER_PLACED_TOPIC = "orders.events.order-placed.v1"
ORDER_SHIPPED_TOPIC = "orders.events.order-shipped.v1"

PRODUCER_NAME = "core.orders"

_EVENT_TOPIC_BY_TYPE: dict[str, str] = {
    "orders.events.order-placed": ORDER_PLACED_TOPIC,
    "orders.events.order-shipped": ORDER_SHIPPED_TOPIC,
}


def topic_for_event(event_type: str) -> str:
    try:
        return _EVENT_TOPIC_BY_TYPE[event_type]
    except KeyError as exc:
        raise ValueError(f"no topic mapping for event type {event_type!r}") from exc


# -*+9/ payload DTOs (wire format: camelCase)


class _MoneyDTO(BaseModel):
    model_config = ConfigDict(frozen=True)

    amount: str
    currency: str

    @classmethod
    def from_vo(cls, money: Money) -> _MoneyDTO:
        return cls(amount=str(money.amount), currency=money.currency)


class _OrderPlacedItemDTO(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    product_id: str = Field(alias="productId")
    seller_id: str = Field(alias="sellerId")
    quantity: int
    unit_price: _MoneyDTO = Field(alias="unitPrice")


class _OrderPlacedPayloadDTO(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    order_id: str = Field(alias="orderId")
    customer_id: str = Field(alias="customerId")
    items: list[_OrderPlacedItemDTO]
    total_amount: _MoneyDTO = Field(alias="totalAmount")
    placed_at: datetime = Field(alias="placedAt")


class _OrderShippedPayloadDTO(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    order_id: str = Field(alias="orderId")
    seller_id: str = Field(alias="sellerId")
    shipped_item_ids: list[str] = Field(alias="shippedItemIds")
    shipped_at: datetime = Field(alias="shippedAt")


def _payload_for(event: DomainEvent) -> dict[str, Any]:
    if isinstance(event, OrderPlaced):
        dto = _OrderPlacedPayloadDTO(
            order_id=str(event.order_id.value),
            customer_id=str(event.customer_id.value),
            items=[
                _OrderPlacedItemDTO(
                    product_id=str(i.product_id.value),
                    seller_id=str(i.seller_id.value),
                    quantity=i.quantity,
                    unit_price=_MoneyDTO.from_vo(i.unit_price),
                )
                for i in event.items
            ],
            total_amount=_MoneyDTO.from_vo(event.total_amount),
            placed_at=event.placed_at,
        )
        return dto.model_dump(by_alias=True, mode="json")

    if isinstance(event, OrderShipped):
        dto2 = _OrderShippedPayloadDTO(
            order_id=str(event.order_id.value),
            seller_id=str(event.seller_id.value),
            shipped_item_ids=[str(p.value) for p in event.shipped_item_ids],
            shipped_at=event.shipped_at,
        )
        return dto2.model_dump(by_alias=True, mode="json")

    raise ValueError(f"unsupported event type: {type(event).__name__}")


def to_outbox(
    event: DomainEvent,
    *,
    aggregate_id: str,
    correlation_id: UUID,
    causation_id: UUID,
    tenant_id: str | None,
) -> OutboxMessage:
    """Wrap a domain event into an ``OutboxMessage`` ready for the relay.

    ``causation_id`` must be the ``messageId`` of the inbound command,
    per docs/architecture.md §5.2 / §4.1.
    """
    envelope = EventEnvelope(
        message_id=uuid4(),
        correlation_id=correlation_id,
        causation_id=causation_id,
        type=event.event_type,
        version=1,
        timestamp=event.occurred_at,
        producer=PRODUCER_NAME,
        tenant_id=tenant_id,
        capabilities=[],
        payload=_payload_for(event),
    )
    return OutboxMessage(
        id=uuid4(),
        aggregate_id=aggregate_id,
        event_type=event.event_type,
        topic=topic_for_event(event.event_type),
        payload=envelope.model_dump(mode="json"),
        attempts=0,
    )

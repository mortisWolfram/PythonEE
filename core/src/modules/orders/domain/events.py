from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from modules.orders.domain.value_objects import (
    CustomerId,
    Money,
    OrderId,
    ProductId,
    SellerId,
)
from shared.domain.event import DomainEvent


@dataclass(frozen=True)
class OrderPlacedItem:
    """Flattened snapshot of an order line as it went into the aggregate.

    Kept separate from ``OrderItem`` so that the domain event is an immutable
    fact not coupled to the aggregate's current state.
    """

    product_id: ProductId
    seller_id: SellerId
    quantity: int
    unit_price: Money


@dataclass(frozen=True)
class OrderPlaced(DomainEvent):
    order_id: OrderId
    customer_id: CustomerId
    items: tuple[OrderPlacedItem, ...]
    total_amount: Money
    placed_at: datetime


@dataclass(frozen=True)
class OrderShipped(DomainEvent):
    order_id: OrderId
    seller_id: SellerId
    shipped_item_ids: tuple[ProductId, ...]
    shipped_at: datetime

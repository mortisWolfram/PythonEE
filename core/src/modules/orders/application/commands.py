from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from shared.application.command import Command


@dataclass(frozen=True)
class CreateOrderItemInput:
    """Wire-friendly item from the command payload.

    ``unit_price`` is a string (``"100.00"``) because that is how it arrives
    in the Kafka JSON payload; the handler converts to ``Money`` via
    ``Money.of(...)``, which is where validation happens.
    """

    product_id: UUID
    seller_id: UUID
    quantity: int
    unit_price: str


@dataclass(frozen=True)
class CreateOrder(Command):
    message_id: UUID
    correlation_id: UUID
    tenant_id: str | None
    customer_id: UUID
    items: tuple[CreateOrderItemInput, ...]


@dataclass(frozen=True)
class ShipOrder(Command):
    message_id: UUID
    correlation_id: UUID
    tenant_id: str | None
    order_id: UUID
    seller_id: UUID

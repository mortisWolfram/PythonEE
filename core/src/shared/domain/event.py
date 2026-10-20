from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class DomainEvent:
    """Immutable fact describing something that happened in the domain.

    Concrete events subclass this and add payload fields:

        @dataclass(frozen=True)
        class OrderPlaced(DomainEvent):
            order_id: OrderId
            customer_id: CustomerId
            total: Money
    """

    event_type: str
    occurred_at: datetime

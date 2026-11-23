from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from modules.orders.domain.events import (
    OrderPlaced,
    OrderPlacedItem,
    OrderShipped,
)
from modules.orders.domain.exceptions import (
    AccessDeniedError,
    CannotShipOrderError,
    EmptyOrderError,
    InvalidOrderItemError,
)
from modules.orders.domain.value_objects import (
    CustomerId,
    Money,
    OrderId,
    OrderStatus,
    ProductId,
    SellerId,
)
from shared.domain.aggregate_root import AggregateRoot


@dataclass(eq=False)
class OrderItem:
    """Line inside the Order aggregate.

    Not an entity with its own lifecycle — its identity is (order_id, product_id)
    and it lives only inside the aggregate boundary (see ADR-0002).
    """

    product_id: ProductId
    seller_id: SellerId
    quantity: int
    unit_price: Money
    shipped: bool = False

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise InvalidOrderItemError(f"quantity must be > 0, got {self.quantity}")

    def line_total(self) -> Money:
        return self.unit_price * self.quantity

    def mark_shipped(self) -> None:
        self.shipped = True


class Order(AggregateRoot[OrderId]):
    """Order aggregate root.

    Shipping is per-seller: each ``ship(seller_id)`` call marks only items
    belonging to that seller. ``status`` becomes ``SHIPPED`` only when every
    item is shipped; otherwise ``PARTIALLY_SHIPPED``.
    """

    def __init__(
        self,
        *,
        id: OrderId,
        customer_id: CustomerId,
        items: list[OrderItem],
        status: OrderStatus,
        created_at: datetime,
        shipped_at: datetime | None = None,
        tenant_id: str | None = None,
    ) -> None:
        super().__init__()
        self.id = id
        self.customer_id = customer_id
        self.items = items
        self.status = status
        self.created_at = created_at
        self.shipped_at = shipped_at
        self.tenant_id = tenant_id

    @classmethod
    def create(
        cls,
        *,
        customer_id: CustomerId,
        items: list[OrderItem],
        tenant_id: str | None = None,
    ) -> Order:
        if not items:
            raise EmptyOrderError("order must contain at least one item")

        now = datetime.now(UTC)
        order = cls(
            id=OrderId.generate(),
            customer_id=customer_id,
            items=items,
            status=OrderStatus.CREATED,
            created_at=now,
            tenant_id=tenant_id,
        )
        order._record(
            OrderPlaced(
                event_type="orders.events.order-placed",
                occurred_at=now,
                order_id=order.id,
                customer_id=customer_id,
                items=tuple(
                    OrderPlacedItem(
                        product_id=i.product_id,
                        seller_id=i.seller_id,
                        quantity=i.quantity,
                        unit_price=i.unit_price,
                    )
                    for i in items
                ),
                total_amount=order.total_amount(),
                placed_at=now,
            )
        )
        return order

    def total_amount(self) -> Money:
        if not self.items:
            raise EmptyOrderError("order has no items")
        total = self.items[0].line_total()
        for item in self.items[1:]:
            total = total + item.line_total()
        return total

    def sellers(self) -> frozenset[SellerId]:
        return frozenset(i.seller_id for i in self.items)

    def ship(self, seller_id: SellerId) -> None:
        """Ship all items belonging to ``seller_id``.

        Raises ``AccessDeniedError`` if the seller has no items in this order.
        Raises ``CannotShipOrderError`` if every item of the seller is already
        shipped.
        """
        if seller_id not in self.sellers():
            raise AccessDeniedError(
                f"seller {seller_id} has no items in order {self.id}"
            )

        pending = [i for i in self.items if i.seller_id == seller_id and not i.shipped]
        if not pending:
            raise CannotShipOrderError(
                f"seller {seller_id} has no pending items in order {self.id}"
            )

        now = datetime.now(UTC)
        for item in pending:
            item.mark_shipped()

        self._record(
            OrderShipped(
                event_type="orders.events.order-shipped",
                occurred_at=now,
                order_id=self.id,
                seller_id=seller_id,
                shipped_item_ids=tuple(i.product_id for i in pending),
                shipped_at=now,
            )
        )

        if all(i.shipped for i in self.items):
            self.status = OrderStatus.SHIPPED
            self.shipped_at = now
        else:
            self.status = OrderStatus.PARTIALLY_SHIPPED

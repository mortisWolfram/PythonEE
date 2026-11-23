from __future__ import annotations

from modules.catalog.domain.exceptions import InvalidProductError
from modules.catalog.domain.value_objects import (
    Money,
    ProductId,
    ProductName,
    SellerId,
)
from shared.domain.aggregate_root import AggregateRoot


class Product(AggregateRoot[ProductId]):
    """Product aggregate root.

    Per step-7 decisions: no domain events in this iteration. The
    ``create-product`` command is a fire-and-forget write; the e2e checks the
    DB row, not a Kafka event.
    """

    def __init__(
        self,
        *,
        id: ProductId,
        seller_id: SellerId,
        name: ProductName,
        price: Money,
        quantity: int,
        tenant_id: str | None = None,
    ) -> None:
        super().__init__()
        if quantity < 0:
            raise InvalidProductError(f"quantity must be >= 0, got {quantity}")
        self.id = id
        self.seller_id = seller_id
        self.name = name
        self.price = price
        self.quantity = quantity
        self.tenant_id = tenant_id

    @classmethod
    def create(
        cls,
        *,
        seller_id: SellerId,
        name: ProductName,
        price: Money,
        quantity: int,
        tenant_id: str | None = None,
    ) -> Product:
        return cls(
            id=ProductId.generate(),
            seller_id=seller_id,
            name=name,
            price=price,
            quantity=quantity,
            tenant_id=tenant_id,
        )

    def change_price(self, new_price: Money) -> None:
        self.price = new_price

    def change_quantity(self, new_quantity: int) -> None:
        if new_quantity < 0:
            raise InvalidProductError(f"quantity must be >= 0, got {new_quantity}")
        self.quantity = new_quantity

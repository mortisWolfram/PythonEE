from __future__ import annotations

from abc import abstractmethod

from modules.orders.domain.entities import Order
from modules.orders.domain.value_objects import OrderId
from shared.domain.repository import Repository


class OrderRepository(Repository[Order, OrderId]):
    """Port for Order persistence.

    ``add`` — persist a new aggregate.
    ``update`` — persist changes to an existing aggregate. Domain objects are
    plain Python (not SQLAlchemy-tracked), so handlers must call ``update``
    explicitly; the infrastructure implementation performs the merge.
    """

    @abstractmethod
    async def update(self, order: Order) -> None: ...

    @abstractmethod
    async def get_or_raise(self, id: OrderId) -> Order:
        """Convenience: ``get`` or raise ``OrderNotFoundError``."""
        ...

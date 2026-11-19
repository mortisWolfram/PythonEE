from __future__ import annotations

from types import TracebackType
from typing import Self
from uuid import UUID, uuid4

import pytest

from modules.orders.application.commands import ShipOrder
from modules.orders.application.handlers import ShipOrderHandler
from modules.orders.application.unit_of_work import OrderUnitOfWork
from modules.orders.domain.entities import Order, OrderItem
from modules.orders.domain.exceptions import (
    AccessDeniedError,
    CannotShipOrderError,
    OrderNotFoundError,
)
from modules.orders.domain.repository import OrderRepository
from modules.orders.domain.value_objects import (
    CustomerId,
    Money,
    OrderId,
    OrderStatus,
    ProductId,
    SellerId,
)
from shared.outbox.model import OutboxMessage
from shared.outbox.repository import OutboxRepository

#  shared storage for the fake UoW


class _Storage:
    def __init__(self) -> None:
        self.orders: dict[UUID, Order] = {}
        self.messages: list[OutboxMessage] = []
        self.commits = 0
        self.rollbacks = 0


class _FakeOrderRepository(OrderRepository):
    def __init__(self, storage: _Storage) -> None:
        self._s = storage

    async def add(self, order: Order) -> None:
        self._s.orders[order.id.value] = order

    async def update(self, order: Order) -> None:
        self._s.orders[order.id.value] = order

    async def get(self, id: OrderId) -> Order | None:
        return self._s.orders.get(id.value)

    async def get_or_raise(self, id: OrderId) -> Order:
        order = await self.get(id)
        if order is None:
            raise LookupError(str(id.value))
        return order


class _FakeOutboxRepository(OutboxRepository):
    def __init__(self, storage: _Storage) -> None:
        self._s = storage

    async def add(self, message: OutboxMessage) -> None:
        self._s.messages.append(message)

    async def fetch_unpublished(self, *, limit: int, max_attempts: int = 10):  # type: ignore[override]
        return []

    async def mark_published(self, message_id: UUID) -> None:
        return None

    async def record_failure(self, message_id: UUID, error: str) -> None:
        return None


class _FakeUoW(OrderUnitOfWork):
    def __init__(self, storage: _Storage) -> None:
        self._storage = storage
        self._orders = _FakeOrderRepository(storage)
        self._outbox = _FakeOutboxRepository(storage)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            self._storage.rollbacks += 1

    @property
    def orders(self) -> OrderRepository:
        return self._orders

    @property
    def outbox(self) -> OutboxRepository:
        return self._outbox

    async def commit(self) -> None:
        self._storage.commits += 1

    async def rollback(self) -> None:
        self._storage.rollbacks += 1


#  helpers


def _seed_order(
    storage: _Storage,
    *,
    seller_id: SellerId | None = None,
) -> Order:
    seller = seller_id or SellerId(uuid4())
    order = Order.create(
        customer_id=CustomerId(uuid4()),
        items=[
            OrderItem(
                product_id=ProductId(uuid4()),
                seller_id=seller,
                quantity=1,
                unit_price=Money.of("100.00"),
            )
        ],
    )
    order.pull_events()  # discard OrderPlaced from the seed
    storage.orders[order.id.value] = order
    return order


def _command(order: Order, *, seller_id: UUID | None = None) -> ShipOrder:
    return ShipOrder(
        message_id=uuid4(),
        correlation_id=uuid4(),
        tenant_id="tenant-1",
        order_id=order.id.value,
        seller_id=seller_id or next(iter(order.sellers())).value,
    )


def _make_handler(storage: _Storage) -> ShipOrderHandler:
    return ShipOrderHandler(lambda: _FakeUoW(storage))


#  tests


async def test_handle_ships_order_and_emits_event() -> None:
    storage = _Storage()
    order = _seed_order(storage)
    handler = _make_handler(storage)
    command = _command(order)

    await handler.handle(command)

    assert order.status is OrderStatus.SHIPPED
    assert len(storage.messages) == 1
    msg = storage.messages[0]
    assert msg.event_type == "orders.events.order-shipped"
    assert msg.topic == "orders.events.order-shipped.v1"
    assert msg.aggregate_id == str(order.id.value)
    assert msg.payload["causation_id"] == str(command.message_id)
    assert msg.payload["payload"]["orderId"] == str(order.id.value)


async def test_unknown_order_raises() -> None:
    storage = _Storage()
    handler = _make_handler(storage)
    command = ShipOrder(
        message_id=uuid4(),
        correlation_id=uuid4(),
        tenant_id=None,
        order_id=uuid4(),
        seller_id=uuid4(),
    )

    with pytest.raises(OrderNotFoundError):
        await handler.handle(command)

    assert storage.messages == []
    assert storage.commits == 0


async def test_foreign_seller_raises_access_denied() -> None:
    storage = _Storage()
    order = _seed_order(storage)
    handler = _make_handler(storage)
    command = _command(order, seller_id=uuid4())

    with pytest.raises(AccessDeniedError):
        await handler.handle(command)

    assert storage.messages == []
    assert storage.commits == 0


async def test_double_ship_raises_cannot_ship() -> None:
    storage = _Storage()
    order = _seed_order(storage)
    handler = _make_handler(storage)
    command = _command(order)

    await handler.handle(command)
    storage.messages.clear()

    with pytest.raises(CannotShipOrderError):
        await handler.handle(command)

    assert storage.messages == []

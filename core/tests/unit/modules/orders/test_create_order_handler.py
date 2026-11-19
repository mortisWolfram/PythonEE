from __future__ import annotations

from types import TracebackType
from typing import Self
from uuid import UUID, uuid4

import pytest

from modules.orders.application.commands import CreateOrder, CreateOrderItemInput
from modules.orders.application.handlers import CreateOrderHandler
from modules.orders.application.unit_of_work import OrderUnitOfWork
from modules.orders.domain.entities import Order
from modules.orders.domain.exceptions import EmptyOrderError
from modules.orders.domain.repository import OrderRepository
from modules.orders.domain.value_objects import OrderId
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


def _command(
    *,
    items: tuple[CreateOrderItemInput, ...] | None = None,
) -> CreateOrder:
    if items is None:
        items = (
            CreateOrderItemInput(
                product_id=uuid4(),
                seller_id=uuid4(),
                quantity=2,
                unit_price="100.00",
            ),
        )
    return CreateOrder(
        message_id=uuid4(),
        correlation_id=uuid4(),
        tenant_id="tenant-1",
        customer_id=uuid4(),
        items=items,
    )


def _make_handler(storage: _Storage) -> CreateOrderHandler:
    return CreateOrderHandler(lambda: _FakeUoW(storage))


# tests


async def test_handle_persists_order_and_outbox() -> None:
    storage = _Storage()
    handler = _make_handler(storage)
    command = _command()

    await handler.handle(command)

    assert len(storage.orders) == 1
    assert len(storage.messages) == 1
    assert storage.commits == 1
    assert storage.rollbacks == 0

    order = next(iter(storage.orders.values()))
    msg = storage.messages[0]
    assert msg.event_type == "orders.events.order-placed"
    assert msg.aggregate_id == str(order.id.value)

    payload = msg.payload
    assert payload["causation_id"] == str(command.message_id)
    assert payload["correlation_id"] == str(command.correlation_id)
    assert payload["tenant_id"] == "tenant-1"
    assert payload["producer"] == "core.orders"

    body = payload["payload"]
    assert body["customerId"] == str(command.customer_id)
    assert body["totalAmount"] == {"amount": "200.00", "currency": "RUB"}


async def test_handle_empty_items_raises_and_rolls_back() -> None:
    storage = _Storage()
    handler = _make_handler(storage)

    with pytest.raises(EmptyOrderError):
        await handler.handle(_command(items=()))

    assert storage.orders == {}
    assert storage.messages == []
    assert storage.commits == 0


async def test_handle_does_not_commit_on_empty_order() -> None:
    # Belt-and-braces: commit counter must stay zero if the aggregate never
    # reaches the UoW (Order.create raises before the context manager opens).
    storage = _Storage()
    handler = _make_handler(storage)

    with pytest.raises(EmptyOrderError):
        await handler.handle(_command(items=()))

    assert storage.commits == 0

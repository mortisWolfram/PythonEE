from uuid import uuid4

import pytest

from modules.orders.domain.entities import Order, OrderItem
from modules.orders.domain.events import OrderPlaced, OrderShipped
from modules.orders.domain.exceptions import (
    AccessDeniedError,
    CannotShipOrderError,
    EmptyOrderError,
)
from modules.orders.domain.value_objects import (
    CustomerId,
    Money,
    OrderStatus,
    ProductId,
    SellerId,
)


def _item(
    seller_id: SellerId,
    *,
    product_id: ProductId | None = None,
    quantity: int = 1,
    unit_price: str = "100.00",
) -> OrderItem:
    return OrderItem(
        product_id=product_id or ProductId(uuid4()),
        seller_id=seller_id,
        quantity=quantity,
        unit_price=Money.of(unit_price),
    )


def _order(
    *,
    customer_id: CustomerId | None = None,
) -> Order:
    seller = SellerId(uuid4())
    order = Order.create(
        customer_id=customer_id or CustomerId(uuid4()),
        items=[_item(seller)],
    )
    return order


def _order_with_two_sellers() -> Order:
    return Order.create(
        customer_id=CustomerId(uuid4()),
        items=[
            _item(SellerId(uuid4())),
            _item(SellerId(uuid4())),
        ],
    )


def test_create_emits_order_placed() -> None:
    order = _order()
    events = order.pull_events()
    assert len(events) == 1
    assert isinstance(events[0], OrderPlaced)
    placed = events[0]
    assert placed.order_id == order.id
    assert placed.customer_id == order.customer_id
    assert len(placed.items) == 1
    assert placed.total_amount == Money.of("100.00")
    assert placed.placed_at.tzinfo is not None


def test_create_empty_raises() -> None:
    with pytest.raises(EmptyOrderError):
        Order.create(customer_id=CustomerId(uuid4()), items=[])


def test_total_amount_sums_items() -> None:
    seller = SellerId(uuid4())
    order = Order.create(
        customer_id=CustomerId(uuid4()),
        items=[
            _item(seller, quantity=2, unit_price="50.00"),
            _item(seller, quantity=3, unit_price="10.00"),
        ],
    )
    assert order.total_amount() == Money.of("130.00")


def test_new_order_status_created() -> None:
    order = _order()
    assert order.status is OrderStatus.CREATED
    assert order.shipped_at is None


def test_ship_full_single_seller() -> None:
    order = _order()
    (seller,) = order.sellers()
    order.pull_events()

    order.ship(seller)

    assert order.status is OrderStatus.SHIPPED
    assert order.shipped_at is not None
    events = order.pull_events()
    assert len(events) == 1
    assert isinstance(events[0], OrderShipped)
    assert events[0].seller_id == seller
    assert len(events[0].shipped_item_ids) == 1


def test_ship_partial_with_two_sellers() -> None:
    order = _order_with_two_sellers()
    sellers = list(order.sellers())
    order.pull_events()

    order.ship(sellers[0])
    assert order.status is OrderStatus.PARTIALLY_SHIPPED
    assert order.shipped_at is None
    events = order.pull_events()
    assert len(events) == 1
    assert isinstance(events[0], OrderShipped)
    assert events[0].seller_id == sellers[0]

    order.ship(sellers[1])
    assert order.status is OrderStatus.SHIPPED
    assert order.shipped_at is not None


def test_ship_twice_same_seller_raises() -> None:
    order = _order()
    (seller,) = order.sellers()
    order.pull_events()
    order.ship(seller)
    order.pull_events()

    with pytest.raises(CannotShipOrderError):
        order.ship(seller)


def test_ship_unknown_seller_raises() -> None:
    order = _order()
    with pytest.raises(AccessDeniedError):
        order.ship(SellerId(uuid4()))


def test_pull_events_clears_buffer() -> None:
    order = _order()
    assert len(order.pull_events()) == 1
    assert order.pull_events() == []

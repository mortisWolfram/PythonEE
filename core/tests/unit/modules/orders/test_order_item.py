import uuid

import pytest

from modules.orders.domain.entities import OrderItem
from modules.orders.domain.exceptions import InvalidOrderItemError
from modules.orders.domain.value_objects import Money, ProductId, SellerId


def _item(*, quantity: int = 2, unit_price: str = "100.00") -> OrderItem:
    return OrderItem(
        product_id=ProductId(uuid.uuid4()),
        seller_id=SellerId(uuid.uuid4()),
        quantity=quantity,
        unit_price=Money.of(unit_price),
    )


def test_line_total() -> None:
    item = _item(quantity=3, unit_price="10.50")
    assert item.line_total() == Money.of("31.50")


def test_zero_quantity_raises() -> None:
    with pytest.raises(InvalidOrderItemError):
        _item(quantity=0)


def test_negative_quantity_raises() -> None:
    with pytest.raises(InvalidOrderItemError):
        _item(quantity=-1)


def test_mark_shipped() -> None:
    item = _item()
    assert item.shipped is False
    item.mark_shipped()
    assert item.shipped is True

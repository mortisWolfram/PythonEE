from uuid import uuid4

import pytest

from modules.catalog.domain.entities import Product
from modules.catalog.domain.exceptions import InvalidProductError
from modules.catalog.domain.value_objects import (
    Money,
    ProductName,
    SellerId,
)


def _product(*, quantity: int = 10) -> Product:
    return Product.create(
        seller_id=SellerId(uuid4()),
        name=ProductName("Widget"),
        price=Money.of("100.00"),
        quantity=quantity,
        tenant_id="tenant-1",
    )


def test_create_valid_product() -> None:
    p = _product()
    assert p.name.value == "Widget"
    assert p.price == Money.of("100.00")
    assert p.quantity == 10
    assert p.tenant_id == "tenant-1"
    assert p.pull_events() == []  # no domain events in this iteration


def test_zero_quantity_allowed() -> None:
    assert _product(quantity=0).quantity == 0


def test_negative_quantity_raises() -> None:
    with pytest.raises(InvalidProductError):
        _product(quantity=-1)


def test_change_price() -> None:
    p = _product()
    p.change_price(Money.of("200.00"))
    assert p.price == Money.of("200.00")


def test_change_quantity_valid() -> None:
    p = _product()
    p.change_quantity(5)
    assert p.quantity == 5


def test_change_quantity_negative_raises() -> None:
    p = _product()
    with pytest.raises(InvalidProductError):
        p.change_quantity(-1)


def test_product_name_strips_whitespace() -> None:
    assert ProductName("  Widget  ").value == "Widget"


def test_product_name_rejects_empty() -> None:
    with pytest.raises(InvalidProductError):
        ProductName("")
    with pytest.raises(InvalidProductError):
        ProductName("   ")


def test_product_name_rejects_too_long() -> None:
    with pytest.raises(InvalidProductError):
        ProductName("x" * 256)


def test_money_rejects_negative() -> None:
    with pytest.raises(InvalidProductError):
        Money.of("-1.00")


def test_money_rejects_bad_currency() -> None:
    with pytest.raises(InvalidProductError):
        Money(amount=Money.of("1").amount, currency="rub")

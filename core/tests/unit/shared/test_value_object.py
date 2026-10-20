from dataclasses import FrozenInstanceError, dataclass

import pytest

from shared.domain.value_object import ValueObject


@dataclass(frozen=True)
class _Money(ValueObject):
    amount: int
    currency: str


def test_equality_by_fields() -> None:
    assert _Money(100, "USD") == _Money(100, "USD")
    assert _Money(100, "USD") != _Money(200, "USD")
    assert _Money(100, "USD") != _Money(100, "EUR")


def test_immutable() -> None:
    m = _Money(100, "USD")
    with pytest.raises(FrozenInstanceError):
        m.amount = 200  # type: ignore[misc]


def test_is_value_object_instance() -> None:
    assert isinstance(_Money(1, "USD"), ValueObject)

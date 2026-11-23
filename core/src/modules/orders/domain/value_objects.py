from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid4

from modules.orders.domain.exceptions import InvalidOrderItemError
from shared.domain.value_object import ValueObject


@dataclass(frozen=True)
class OrderId(ValueObject):
    value: UUID

    @classmethod
    def generate(cls) -> OrderId:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True)
class CustomerId(ValueObject):
    value: UUID

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True)
class SellerId(ValueObject):
    value: UUID

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True)
class ProductId(ValueObject):
    value: UUID

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True)
class Money(ValueObject):
    """Monetary amount in minor units avoided for now — store as Decimal.

    Invariants: amount >= 0, currency is a 3-letter ISO-like code.
    """

    amount: Decimal
    currency: str

    def __post_init__(self) -> None:
        if self.amount < 0:
            raise InvalidOrderItemError(f"amount must be >= 0, got {self.amount}")
        if len(self.currency) != 3 or not self.currency.isupper():
            raise InvalidOrderItemError(f"invalid currency code: {self.currency!r}")

    @classmethod
    def of(cls, amount: str | int | Decimal, currency: str = "RUB") -> Money:
        return cls(amount=Decimal(str(amount)), currency=currency)

    def __add__(self, other: Money) -> Money:
        if self.currency != other.currency:
            raise InvalidOrderItemError(
                f"cannot add {self.currency} and {other.currency}"
            )
        return Money(self.amount + other.amount, self.currency)

    def __mul__(self, factor: int) -> Money:
        if factor < 0:
            raise InvalidOrderItemError(f"factor must be >= 0, got {factor}")
        return Money(self.amount * factor, self.currency)

    def __str__(self) -> str:
        return f"{self.amount:.2f} {self.currency}"


class OrderStatus(StrEnum):
    CREATED = "created"
    PARTIALLY_SHIPPED = "partially_shipped"
    SHIPPED = "shipped"

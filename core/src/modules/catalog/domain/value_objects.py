from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID, uuid4

from modules.catalog.domain.exceptions import InvalidProductError
from shared.domain.value_object import ValueObject


@dataclass(frozen=True)
class ProductId(ValueObject):
    value: UUID

    @classmethod
    def generate(cls) -> ProductId:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True)
class SellerId(ValueObject):
    """Duplicated from orders on purpose — bounded contexts don't share classes."""

    value: UUID

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True)
class ProductName(ValueObject):
    value: str

    def __post_init__(self) -> None:
        stripped = self.value.strip()
        if not stripped:
            raise InvalidProductError("name must not be empty")
        if len(self.value) > 255:
            raise InvalidProductError(f"name too long: {len(self.value)} > 255")
        object.__setattr__(self, "value", stripped)

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class Money(ValueObject):
    """Minimal Money for catalog — no arithmetic, only storage/validation."""

    amount: Decimal
    currency: str

    def __post_init__(self) -> None:
        if self.amount < 0:
            raise InvalidProductError(f"price must be >= 0, got {self.amount}")
        if len(self.currency) != 3 or not self.currency.isupper():
            raise InvalidProductError(f"invalid currency code: {self.currency!r}")

    @classmethod
    def of(cls, amount: str | int | Decimal, currency: str = "RUB") -> Money:
        return cls(amount=Decimal(str(amount)), currency=currency)

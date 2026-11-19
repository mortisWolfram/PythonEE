from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID, uuid4

from modules.identity.domain.exceptions import InvalidEmailError
from shared.domain.value_object import ValueObject


@dataclass(frozen=True)
class UserId(ValueObject):
    value: UUID

    @classmethod
    def generate(cls) -> UserId:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True)
class Email(ValueObject):
    """Email with minimal syntactic validation; normalises to lowercase."""

    value: str

    def __post_init__(self) -> None:
        if not self.value or " " in self.value:
            raise InvalidEmailError(self.value)
        local, sep, domain = self.value.partition("@")
        if not sep or not local or not domain:
            raise InvalidEmailError(self.value)
        object.__setattr__(self, "value", self.value.lower())

    def __str__(self) -> str:
        return self.value


class Role(StrEnum):
    """Coarse label. Capabilities carry the real permissions (arch §6.2)."""

    CUSTOMER = "customer"
    SELLER = "seller"
    COURIER = "courier"
    ADMIN = "admin"
    SUPPORT = "support"


@dataclass(frozen=True)
class Capabilities(ValueObject):
    """Immutable set of granular permission strings (``catalog.product.write``)."""

    values: frozenset[str]

    @classmethod
    def empty(cls) -> Capabilities:
        return cls(frozenset())

    def has(self, capability: str) -> bool:
        return capability in self.values

    def with_added(self, capability: str) -> Capabilities:
        return Capabilities(self.values | {capability})

    def with_removed(self, capability: str) -> Capabilities:
        return Capabilities(self.values - {capability})

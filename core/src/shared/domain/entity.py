from __future__ import annotations

from typing import Generic, TypeVar

TId = TypeVar("TId")


class Entity(Generic[TId]):
    """Base class for entities identified by a typed ID.

    Subclasses MUST set ``self.id`` in their ``__init__``.
    Equality and hashing are based solely on ``(type, id)``.
    """

    id: TId

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Entity):
            return NotImplemented
        if self.__class__ is not other.__class__:
            return False
        return bool(self.id == other.id)

    def __hash__(self) -> int:
        return hash((self.__class__, self.id))

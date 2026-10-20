from __future__ import annotations

from typing import Generic, TypeVar

from shared.domain.entity import Entity
from shared.domain.event import DomainEvent

TId = TypeVar("TId")


class AggregateRoot(Entity[TId], Generic[TId]):
    """Consistency boundary that records domain events.

    Subclasses call ``self._record(event)`` inside their behavior methods,
    and the application layer pulls events via ``pull_events()`` to publish
    them (usually by writing to the outbox within the same transaction).
    """

    def __init__(self) -> None:
        self._events: list[DomainEvent] = []

    def _record(self, event: DomainEvent) -> None:
        self._events.append(event)

    def pull_events(self) -> list[DomainEvent]:
        """Return recorded events and clear the buffer."""
        events = self._events
        self._events = []
        return events

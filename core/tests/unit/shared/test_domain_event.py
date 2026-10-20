from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from shared.domain.event import DomainEvent


def test_fields_are_set() -> None:
    now = datetime.now(UTC)
    e = DomainEvent(event_type="orders.events.order-placed", occurred_at=now)
    assert e.event_type == "orders.events.order-placed"
    assert e.occurred_at == now


def test_immutable() -> None:
    e = DomainEvent(event_type="a", occurred_at=datetime.now(UTC))
    with pytest.raises(FrozenInstanceError):
        e.event_type = "b"  # type: ignore[misc]


def test_hashable() -> None:
    now = datetime.now(UTC)
    assert len({DomainEvent("a", now), DomainEvent("b", now)}) == 2

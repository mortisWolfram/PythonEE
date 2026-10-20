from datetime import UTC, datetime

from shared.domain.aggregate_root import AggregateRoot
from shared.domain.event import DomainEvent


class _SomethingHappened(DomainEvent):
    pass


class _Order(AggregateRoot[str]):
    def __init__(self, id: str) -> None:
        super().__init__()
        self.id = id

    def do_something(self) -> None:
        self._record(
            _SomethingHappened(
                event_type="test.something-happened",
                occurred_at=datetime.now(UTC),
            )
        )


def test_pull_events_returns_recorded_and_clears() -> None:
    order = _Order("o-1")
    order.do_something()
    order.do_something()

    events = order.pull_events()
    assert len(events) == 2
    assert all(isinstance(e, _SomethingHappened) for e in events)
    assert order.pull_events() == []


def test_pull_events_empty_when_nothing_recorded() -> None:
    assert _Order("o-2").pull_events() == []

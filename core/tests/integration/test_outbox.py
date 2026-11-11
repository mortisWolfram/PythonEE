from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.kafka.envelope import EventEnvelope
from shared.outbox.model import OutboxMessage
from shared.outbox.relay import OutboxRelay
from shared.outbox.repository import OutboxRepository

pytestmark = pytest.mark.integration


def _message(aggregate_id: str = "order-1") -> OutboxMessage:
    envelope = EventEnvelope.new(
        type="orders.events.order-placed",
        version=1,
        producer="test",
        payload={"order_id": aggregate_id},
    )
    return OutboxMessage(
        id=uuid4(),
        aggregate_id=aggregate_id,
        event_type="orders.events.order-placed",
        topic="orders.events.order-placed.v1",
        payload=envelope.model_dump(mode="json"),
    )


async def test_add_then_fetch(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        repo = OutboxRepository(session)
        await repo.add(_message())
        await session.commit()

    async with session_factory() as session:
        repo = OutboxRepository(session)
        unpublished = await repo.fetch_unpublished(limit=10)
        assert len(unpublished) == 1
        assert unpublished[0].aggregate_id == "order-1"
        assert unpublished[0].published_at is None
        assert unpublished[0].attempts == 0


async def test_mark_published_removes_from_fetch(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        repo = OutboxRepository(session)
        msg = _message()
        await repo.add(msg)
        await session.commit()
        await repo.mark_published(msg.id)
        await session.commit()

    async with session_factory() as session:
        repo = OutboxRepository(session)
        assert await repo.fetch_unpublished(limit=10) == []


async def test_record_failure_increments_attempts(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        repo = OutboxRepository(session)
        msg = _message()
        await repo.add(msg)
        await session.commit()
        await repo.record_failure(msg.id, "boom")
        await session.commit()

    async with session_factory() as session:
        repo = OutboxRepository(session)
        unpublished = await repo.fetch_unpublished(limit=10)
        assert len(unpublished) == 1
        assert unpublished[0].attempts == 1
        assert unpublished[0].last_error == "boom"


async def test_max_attempts_excludes_from_fetch(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        repo = OutboxRepository(session)
        msg = _message()
        await repo.add(msg)
        await session.commit()
        for _ in range(3):
            await repo.record_failure(msg.id, "boom")
        await session.commit()

    async with session_factory() as session:
        repo = OutboxRepository(session)
        assert await repo.fetch_unpublished(limit=10, max_attempts=3) == []


class _FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, EventEnvelope]] = []

    async def send(self, topic: str, key: str, envelope: EventEnvelope) -> None:
        self.sent.append((topic, key, envelope))


async def test_relay_publishes_and_marks_published(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    original = _message(aggregate_id="order-42")

    async with session_factory() as session:
        repo = OutboxRepository(session)
        await repo.add(original)
        await session.commit()

    producer = _FakeProducer()
    relay = OutboxRelay(
        session_factory=session_factory,
        producer=producer,
        poll_interval_ms=10,
        batch_size=10,
    )
    count = await relay.process_batch()
    assert count == 1

    assert len(producer.sent) == 1
    topic, key, envelope = producer.sent[0]
    assert topic == "orders.events.order-placed.v1"
    assert key == "order-42"
    assert envelope.type == "orders.events.order-placed"
    assert envelope.payload == {"order_id": "order-42"}

    async with session_factory() as session:
        repo = OutboxRepository(session)
        assert await repo.fetch_unpublished(limit=10) == []


class _FlakyProducer:
    def __init__(self) -> None:
        self.failures_left = 1
        self.succeeded: list[UUID] = []

    async def send(self, topic: str, key: str, envelope: EventEnvelope) -> None:
        if self.failures_left > 0:
            self.failures_left -= 1
            raise RuntimeError("kafka down")
        self.succeeded.append(envelope.message_id)


async def test_relay_records_failure_and_retries_next_batch(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        repo = OutboxRepository(session)
        await repo.add(_message(aggregate_id="order-retry"))
        await session.commit()

    producer = _FlakyProducer()
    relay = OutboxRelay(
        session_factory=session_factory,
        producer=producer,
        poll_interval_ms=10,
        batch_size=10,
    )

    await relay.process_batch()  # first attempt fails

    async with session_factory() as session:
        repo = OutboxRepository(session)
        unpublished = await repo.fetch_unpublished(limit=10)
        assert len(unpublished) == 1
        assert unpublished[0].attempts == 1
        assert unpublished[0].last_error == "kafka down"

    await relay.process_batch()  # second attempt succeeds

    assert len(producer.succeeded) == 1

    async with session_factory() as session:
        repo = OutboxRepository(session)
        assert await repo.fetch_unpublished(limit=10) == []

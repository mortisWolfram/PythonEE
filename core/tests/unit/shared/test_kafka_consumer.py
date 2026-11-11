from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import pytest

from shared.idempotency.memory import InMemoryIdempotencyStore
from shared.idempotency.store import IdempotencyStore
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.envelope import EventEnvelope
from shared.kafka.serializer import JsonSerializer

# ---- test doubles ----------------------------------------------------------


@dataclass
class _Record:
    topic: str
    key: bytes | None
    value: bytes


class _FakeConsumer:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.commits = 0

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def getone(self) -> _Record:
        raise NotImplementedError

    async def commit(self) -> None:
        self.commits += 1


class _FakeDlqProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, bytes, dict[str, str] | None]] = []

    async def send_raw(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.sent.append((topic, key, value, headers))


class _RaisingIdempotency(IdempotencyStore):
    async def is_processed(self, message_id: UUID) -> bool:
        raise RuntimeError("db down")

    async def mark_processed(self, message_id: UUID) -> None:
        raise RuntimeError("db down")

# ---- helpers ---------------------------------------------------------------


def _envelope(msg_id_seed: str = "x") -> EventEnvelope:
    return EventEnvelope.new(
        type="orders.events.order-placed",
        version=1,
        producer="test",
        payload={"order_id": "o-1", "seed": msg_id_seed},
    )


def _record(
    topic: str = "orders.events.order-placed.v1",
    *,
    env: EventEnvelope | None = None,
) -> _Record:
    serializer = JsonSerializer()
    return _Record(
        topic=topic,
        key=b"order-1",
        value=serializer.serialize(env or _envelope()),
    )


def _build(
    *,
    handler,
    idempotency: IdempotencyStore | None = None,
    dlq_producer: _FakeDlqProducer | None = None,
) -> tuple[KafkaConsumer, _FakeConsumer]:
    fake = _FakeConsumer()
    consumer = KafkaConsumer(
        consumer=fake,
        serializer=JsonSerializer(),
        idempotency=idempotency or InMemoryIdempotencyStore(),
        handler=handler,
        dlq_producer=dlq_producer,
        getone_timeout_s=0.01,
        error_backoff_s=0.0,
    )
    return consumer, fake

# ---- tests -----------------------------------------------------------------


async def test_happy_path_marks_processed_and_commits() -> None:
    received: list[EventEnvelope] = []

    async def handler(env: EventEnvelope) -> None:
        received.append(env)

    idem = InMemoryIdempotencyStore()
    consumer, fake = _build(handler=handler, idempotency=idem)
    record = _record()
    env = JsonSerializer().deserialize(record.value)

    await consumer._process_one(record)

    assert received == [env]
    assert await idem.is_processed(env.message_id) is True
    assert fake.commits == 1


async def test_duplicate_is_skipped_without_invoking_handler() -> None:
    calls = 0

    async def handler(env: EventEnvelope) -> None:
        nonlocal calls
        calls += 1

    idem = InMemoryIdempotencyStore()
    consumer, fake = _build(handler=handler, idempotency=idem)
    record = _record()
    env = JsonSerializer().deserialize(record.value)
    await idem.mark_processed(env.message_id)

    await consumer._process_one(record)

    assert calls == 0
    assert fake.commits == 1


async def test_handler_failure_goes_to_dlq_and_commits() -> None:
    async def handler(env: EventEnvelope) -> None:
        raise RuntimeError("boom")

    idem = InMemoryIdempotencyStore()
    dlq = _FakeDlqProducer()
    consumer, fake = _build(handler=handler, idempotency=idem, dlq_producer=dlq)
    record = _record()
    env = JsonSerializer().deserialize(record.value)

    await consumer._process_one(record)

    assert fake.commits == 1
    assert len(dlq.sent) == 1
    topic, key, value, headers = dlq.sent[0]
    assert topic == "orders.events.order-placed.v1.dlq.v1"
    assert key == "order-1"
    assert value == record.value
    assert headers is not None and "x-error" in headers
    assert await idem.is_processed(env.message_id) is False


async def test_handler_failure_without_dlq_producer_still_commits() -> None:
    async def handler(env: EventEnvelope) -> None:
        raise RuntimeError("boom")

    consumer, fake = _build(handler=handler)
    await consumer._process_one(_record())
    assert fake.commits == 1


async def test_deserialize_failure_goes_to_dlq_and_commits() -> None:
    async def handler(env: EventEnvelope) -> None:  # pragma: no cover
        raise AssertionError("must not be called")

    dlq = _FakeDlqProducer()
    consumer, fake = _build(handler=handler, dlq_producer=dlq)
    record = _Record(topic="orders.events.order-placed.v1", key=b"k", value=b"{not-json")

    await consumer._process_one(record)

    assert fake.commits == 1
    assert len(dlq.sent) == 1
    _, _, value, headers = dlq.sent[0]
    assert value == b"{not-json"
    assert headers is not None and "x-error" in headers


async def test_idempotency_check_failure_does_not_commit() -> None:
    async def handler(env: EventEnvelope) -> None:  # pragma: no cover
        raise AssertionError("must not be called")

    consumer, fake = _build(handler=handler, idempotency=_RaisingIdempotency())
    with pytest.raises(RuntimeError):
        await consumer._process_one(_record())
    assert fake.commits == 0

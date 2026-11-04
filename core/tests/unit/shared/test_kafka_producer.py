from unittest.mock import AsyncMock

from shared.kafka.envelope import EventEnvelope
from shared.kafka.producer import KafkaProducer
from shared.kafka.serializer import JsonSerializer


def _envelope() -> EventEnvelope:
    return EventEnvelope.new(
        type="orders.events.order-placed",
        version=1,
        producer="test",
        payload={"order_id": "o-1"},
    )


async def test_send_serializes_and_forwards_key() -> None:
    underlying = AsyncMock()
    producer = KafkaProducer(underlying, JsonSerializer())

    env = _envelope()
    await producer.send("orders.events.order-placed.v1", "order-1", env)

    underlying.send_and_wait.assert_awaited_once()
    args, kwargs = underlying.send_and_wait.call_args
    assert args[0] == "orders.events.order-placed.v1"
    assert kwargs["key"] == b"order-1"
    assert JsonSerializer().deserialize(kwargs["value"]) == env


async def test_send_raw_forwards_bytes_and_headers() -> None:
    underlying = AsyncMock()
    producer = KafkaProducer(underlying, JsonSerializer())

    await producer.send_raw(
        topic="orders.events.order-placed.v1.dlq.v1",
        key="order-1",
        value=b"raw-bytes",
        headers={"x-error": "boom"},
    )

    underlying.send_and_wait.assert_awaited_once()
    args, kwargs = underlying.send_and_wait.call_args
    assert args[0] == "orders.events.order-placed.v1.dlq.v1"
    assert kwargs["value"] == b"raw-bytes"
    assert kwargs["key"] == b"order-1"
    assert kwargs["headers"] == [("x-error", b"boom")]


async def test_start_stop_delegate() -> None:
    underlying = AsyncMock()
    producer = KafkaProducer(underlying, JsonSerializer())
    await producer.start()
    await producer.stop()
    underlying.start.assert_awaited_once()
    underlying.stop.assert_awaited_once()

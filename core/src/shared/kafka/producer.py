from __future__ import annotations

from typing import Any, Protocol

import structlog

from shared.config.settings import Settings
from shared.kafka.envelope import EventEnvelope
from shared.kafka.serializer import JsonSerializer, Serializer

log = structlog.get_logger(__name__)


class _AsyncProducer(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send_and_wait(self, topic: str, **kwargs: Any) -> Any: ...


class KafkaProducer:
    """Async wrapper around aiokafka's producer.

    Publishes envelopes with ``key = aggregate_id`` (per docs/architecture.md §5.3).
    The underlying producer is injected so the class is trivially mockable.
    """

    def __init__(self, producer: _AsyncProducer, serializer: Serializer) -> None:
        self._producer = producer
        self._serializer = serializer

    @classmethod
    def from_settings(
        cls, settings: Settings, serializer: Serializer | None = None
    ) -> KafkaProducer:
        from aiokafka import AIOKafkaProducer

        underlying = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        return cls(underlying, serializer or JsonSerializer())

    async def start(self) -> None:
        await self._producer.start()
        log.info("kafka_producer_started")

    async def stop(self) -> None:
        await self._producer.stop()
        log.info("kafka_producer_stopped")

    async def send(self, topic: str, key: str, envelope: EventEnvelope) -> None:
        value = self._serializer.serialize(envelope)
        await self._producer.send_and_wait(topic, value=value, key=key.encode("utf-8"))
        log.debug(
            "kafka_message_sent",
            topic=topic,
            key=key,
            message_id=str(envelope.message_id),
            type=envelope.type,
        )

    async def send_raw(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Send raw bytes — used for DLQ forwarding where the envelope may not parse."""
        headers_list = [(k, v.encode("utf-8")) for k, v in (headers or {}).items()]
        await self._producer.send_and_wait(
            topic,
            value=value,
            key=key.encode("utf-8"),
            headers=headers_list or None,
        )

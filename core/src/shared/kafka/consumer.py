from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

import structlog

from shared.idempotency.store import IdempotencyStore
from shared.kafka.envelope import EventEnvelope
from shared.kafka.serializer import Serializer
from shared.kafka.topics import dlq_topic

log = structlog.get_logger(__name__)

MessageHandler = Callable[[EventEnvelope], Awaitable[None]]


class _AsyncConsumer(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def getone(self) -> Any: ...
    async def commit(self) -> None: ...


class _DlqPublisher(Protocol):
    async def send_raw(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: dict[str, str] | None = None,
    ) -> None: ...


class KafkaConsumer:
    """Async consumer with manual commit, idempotency, and best-effort DLQ.

    Failure policy per docs/architecture.md §5.5:
      - handler / deserialization failure → forward raw bytes to
        ``{topic}.dlq.v1`` (if a DLQ publisher is wired), commit offset;
      - idempotency-store failure (e.g. DB down) → do NOT commit, let the
        message be redelivered;
      - duplicate ``messageId`` → skip handler, commit offset.

    Known limitation: ``mark_processed`` runs *after* the handler, outside
    the handler's DB transaction.  A crash between handler-commit and
    ``mark_processed`` can re-run the handler on redelivery.  Mitigation
    planned for a later iteration: move the idempotency write into the
    UoW alongside the aggregate write.
    """

    def __init__(
        self,
        *,
        consumer: _AsyncConsumer,
        serializer: Serializer,
        idempotency: IdempotencyStore,
        handler: MessageHandler,
        dlq_producer: _DlqPublisher | None = None,
        getone_timeout_s: float = 1.0,
        error_backoff_s: float = 1.0,
    ) -> None:
        self._consumer = consumer
        self._serializer = serializer
        self._idempotency = idempotency
        self._handler = handler
        self._dlq_producer = dlq_producer
        self._getone_timeout_s = getone_timeout_s
        self._error_backoff_s = error_backoff_s
        self._stopped = False

    async def start(self) -> None:
        await self._consumer.start()
        log.info("kafka_consumer_started")

    async def stop(self) -> None:
        self._stopped = True

    async def run(self) -> None:
        await self.start()
        try:
            while not self._stopped:
                try:
                    msg = await asyncio.wait_for(
                        self._consumer.getone(), timeout=self._getone_timeout_s
                    )
                except TimeoutError:
                    continue
                except asyncio.CancelledError:
                    return
                except Exception:
                    if self._stopped:
                        return
                    log.exception("kafka_consumer_getone_failed")
                    continue

                try:
                    await self._process_one(msg)
                except Exception:
                    # Unhandled failure (typically idempotency store down).
                    # Do not commit; back off and let redelivery retry.
                    log.exception("kafka_process_one_unhandled")
                    await asyncio.sleep(self._error_backoff_s)
        finally:
            await self._consumer.stop()
            log.info("kafka_consumer_stopped")

    async def _process_one(self, msg: Any) -> None:
        topic: str = msg.topic
        key = (msg.key or b"").decode("utf-8")

        try:
            envelope = self._serializer.deserialize(msg.value)
        except Exception as exc:
            log.warning("kafka_deserialize_failed", topic=topic, key=key, error=str(exc))
            await self._forward_to_dlq(topic, key, msg.value, str(exc))
            await self._consumer.commit()
            return

        already = await self._idempotency.is_processed(envelope.message_id)
        if already:
            log.info(
                "kafka_message_skipped_duplicate",
                message_id=str(envelope.message_id),
                type=envelope.type,
            )
            await self._consumer.commit()
            return

        try:
            await self._handler(envelope)
        except Exception as exc:
            log.exception(
                "kafka_handler_failed",
                message_id=str(envelope.message_id),
                type=envelope.type,
            )
            await self._forward_to_dlq(topic, key, msg.value, str(exc))
            await self._consumer.commit()
            return

        await self._idempotency.mark_processed(envelope.message_id)
        await self._consumer.commit()
        log.info(
            "kafka_message_processed",
            message_id=str(envelope.message_id),
            type=envelope.type,
        )

    async def _forward_to_dlq(
        self, source_topic: str, key: str, raw_value: bytes, error: str
    ) -> None:
        if self._dlq_producer is None:
            log.warning(
                "dlq_disabled_message_dropped",
                source_topic=source_topic,
                key=key,
                error=error,
            )
            return
        try:
            await self._dlq_producer.send_raw(
                topic=dlq_topic(source_topic),
                key=key,
                value=raw_value,
                headers={"x-error": error[:1000]},
            )
        except Exception:
            log.exception("dlq_publish_failed", source_topic=source_topic)

from __future__ import annotations

import asyncio
from typing import Protocol

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.kafka.envelope import EventEnvelope
from shared.outbox.repository import OutboxRepository

log = structlog.get_logger(__name__)


class _Publisher(Protocol):
    async def send(self, topic: str, key: str, envelope: EventEnvelope) -> None: ...


class OutboxRelay:
    """Async worker that polls the outbox and publishes unpublished messages.

    Semantics: **at-least-once**. A message is marked published only after
    ``send`` returns successfully. A crash between ``send`` and ``mark_published``
    causes a redelivery on restart — consumers must be idempotent
    (they are, per ``shared/idempotency``).
    """

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        producer: _Publisher,
        poll_interval_ms: int = 500,
        batch_size: int = 50,
        max_attempts: int = 10,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._poll_interval_s = poll_interval_ms / 1000.0
        self._batch_size = batch_size
        self._max_attempts = max_attempts

    async def run(self, stop_event: asyncio.Event) -> None:
        """Loop until ``stop_event`` is set. Never raises; logs and backs off."""
        log.info("outbox_relay_started", batch_size=self._batch_size)
        try:
            while not stop_event.is_set():
                processed = 0
                try:
                    processed = await self.process_batch()
                except Exception:
                    log.exception("outbox_relay_batch_failed")
                if processed == 0:
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(), timeout=self._poll_interval_s
                        )
                    except TimeoutError:
                        pass
        finally:
            log.info("outbox_relay_stopped")

    async def process_batch(self) -> int:
        """Process one batch. Returns number of messages attempted."""
        async with self._session_factory() as session:
            repo = OutboxRepository(session)
            messages = await repo.fetch_unpublished(
                limit=self._batch_size, max_attempts=self._max_attempts
            )
            for message in messages:
                try:
                    envelope = EventEnvelope.model_validate(message.payload)
                    await self._producer.send(
                        topic=message.topic,
                        key=message.aggregate_id,
                        envelope=envelope,
                    )
                    await repo.mark_published(message.id)
                except Exception as exc:
                    log.warning(
                        "outbox_publish_failed",
                        message_id=str(message.id),
                        event_type=message.event_type,
                        error=str(exc),
                    )
                    await repo.record_failure(message.id, str(exc))
            await session.commit()
            return len(messages)

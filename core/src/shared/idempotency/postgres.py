from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.idempotency.model import ProcessedMessage
from shared.idempotency.store import IdempotencyStore


class PostgresIdempotencyStore(IdempotencyStore):
    """Idempotency store backed by ``processed_messages``.

    Each method opens its own short transaction from ``session_factory``
    and commits. This is the "post-handler" mode of deduplication, consistent
    with the current ``KafkaConsumer`` design (see its docstring).

    A future iteration may introduce a transaction-scoped variant that shares
    the handler's UoW session so that aggregate write + idempotency insert
    commit atomically.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def is_processed(self, message_id: UUID) -> bool:
        async with self._session_factory() as session:
            stmt = select(ProcessedMessage.message_id).where(
                ProcessedMessage.message_id == message_id
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none() is not None

    async def mark_processed(self, message_id: UUID) -> None:
        async with self._session_factory() as session:
            stmt = (
                pg_insert(ProcessedMessage)
                .values(message_id=message_id, processed_at=datetime.now(UTC))
                .on_conflict_do_nothing(index_elements=["message_id"])
            )
            await session.execute(stmt)
            await session.commit()

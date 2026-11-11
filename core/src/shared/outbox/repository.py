from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from shared.outbox.model import OutboxMessage


class OutboxRepository:
    """CRUD for the outbox table, bound to a caller-provided ``AsyncSession``.

    The caller controls the transaction: in the write path, ``add()`` is called
    inside the same transaction as the aggregate mutation. In the relay path,
    the relay owns the session and commits once per batch.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, message: OutboxMessage) -> None:
        self._session.add(message)

    async def fetch_unpublished(
        self, *, limit: int, max_attempts: int = 10
    ) -> list[OutboxMessage]:
        """Lock and return a batch of unpublished messages.

        ``SKIP LOCKED`` allows multiple relay instances to process disjoint
        batches without duplicating work. Messages that exceeded
        ``max_attempts`` stay in the table as poison records for manual triage.
        """
        stmt = (
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .where(OutboxMessage.attempts < max_attempts)
            .order_by(OutboxMessage.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def mark_published(self, message_id: UUID) -> None:
        stmt = (
            update(OutboxMessage)
            .where(OutboxMessage.id == message_id)
            .values(published_at=datetime.now(UTC))
        )
        await self._session.execute(stmt)

    async def record_failure(self, message_id: UUID, error: str) -> None:
        stmt = (
            update(OutboxMessage)
            .where(OutboxMessage.id == message_id)
            .values(
                attempts=OutboxMessage.attempts + 1,
                last_error=error[:2000],
            )
        )
        await self._session.execute(stmt)

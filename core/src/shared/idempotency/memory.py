from __future__ import annotations

from uuid import UUID

from shared.idempotency.store import IdempotencyStore


class InMemoryIdempotencyStore(IdempotencyStore):
    """Non-persistent store for unit tests and local smoke runs.

    Not safe across processes or event loops — purely a test double.
    """

    def __init__(self) -> None:
        self._seen: set[UUID] = set()

    async def is_processed(self, message_id: UUID) -> bool:
        return message_id in self._seen

    async def mark_processed(self, message_id: UUID) -> None:
        self._seen.add(message_id)

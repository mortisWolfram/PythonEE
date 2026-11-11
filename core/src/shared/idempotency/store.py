from __future__ import annotations

from abc import ABC, abstractmethod
from uuid import UUID


class IdempotencyStore(ABC):
    """Port for message deduplication (docs/architecture.md §5.5).

    Concrete implementation lives in the infrastructure layer (Postgres,
    backed by the ``processed_messages`` table).
    """

    @abstractmethod
    async def is_processed(self, message_id: UUID) -> bool: ...

    @abstractmethod
    async def mark_processed(self, message_id: UUID) -> None: ...

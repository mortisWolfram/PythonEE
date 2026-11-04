from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import DateTime
from sqlalchemy.orm import Mapped, mapped_column

from shared.persistence.base import Base


class ProcessedMessage(Base):
    """Deduplication table (docs/architecture.md §5.5).

    Insert is idempotent-safe via ON CONFLICT DO NOTHING in the repository.
    """

    __tablename__ = "processed_messages"

    message_id: Mapped[UUID] = mapped_column(primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )

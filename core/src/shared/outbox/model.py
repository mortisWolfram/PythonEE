from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from shared.persistence.base import Base


def _utc_now() -> datetime:
    return datetime.now(UTC)


class OutboxMessage(Base):
    """Transactional outbox record (docs/architecture.md §5.4).

    ``payload`` holds the full ``EventEnvelope`` dumped with
    ``model_dump(mode="json")`` (snake_case, JSON-safe types).
    The relay re-validates it via ``EventEnvelope.model_validate`` at publish time.

    ``topic`` carries the target Kafka topic, captured at enqueue time so that
    a future topic rename cannot silently route already-stored messages.
    """

    __tablename__ = "outbox_messages"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    aggregate_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

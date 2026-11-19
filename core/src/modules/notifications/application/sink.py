from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol
from uuid import UUID

import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class NotificationEvent:
    """Sink-agnostic view of a notification. ``kind`` is one of
    ``"order_placed"`` / ``"order_shipped"``."""

    kind: str
    order_id: UUID
    customer_id: UUID | None
    seller_id: UUID | None
    occurred_at: datetime
    metadata: dict[str, str] = field(default_factory=dict)


class NotificationSink(Protocol):
    """Port. Implementations decide *where* notifications go."""

    async def send(self, event: NotificationEvent) -> None: ...


class LogSink:
    """Production stub — logs instead of sending email/push."""

    async def send(self, event: NotificationEvent) -> None:
        log.info(
            "notification would be sent",
            kind=event.kind,
            order_id=str(event.order_id),
            customer_id=str(event.customer_id) if event.customer_id else None,
            seller_id=str(event.seller_id) if event.seller_id else None,
            occurred_at=event.occurred_at.isoformat(),
        )


class InMemorySink:
    """Test double — captures notifications in memory.

    Used by e2e tests (``tests/e2e/test_order_flow.py``) to assert that the
    notifications module received the expected events without parsing logs.
    """

    def __init__(self) -> None:
        self.events: list[NotificationEvent] = []

    async def send(self, event: NotificationEvent) -> None:
        self.events.append(event)

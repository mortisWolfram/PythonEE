from __future__ import annotations

from datetime import datetime
from uuid import UUID

import structlog
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from modules.notifications.application.sink import NotificationEvent, NotificationSink
from shared.kafka.envelope import EventEnvelope

log = structlog.get_logger(__name__)

_ORDER_PLACED_TYPE = "orders.events.order-placed"
_ORDER_SHIPPED_TYPE = "orders.events.order-shipped"


class _OrderPlacedPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    order_id: UUID = Field(alias="orderId")
    customer_id: UUID = Field(alias="customerId")
    placed_at: datetime = Field(alias="placedAt")
    total_amount: dict[str, str] = Field(alias="totalAmount")


class _OrderShippedPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    order_id: UUID = Field(alias="orderId")
    seller_id: UUID = Field(alias="sellerId")
    shipped_at: datetime = Field(alias="shippedAt")


class NotificationsEventHandler:
    """``EventEnvelope`` → ``NotificationSink.send``.

    Dispatches by ``envelope.type``. Unknown types are logged and dropped —
    notifications must never block the consumer. Bad payloads are logged and
    dropped for the same reason; they do *not* go to DLQ (the message is
    structurally valid, only the notifications module can't use it).
    """

    def __init__(self, sink: NotificationSink) -> None:
        self._sink = sink

    async def __call__(self, envelope: EventEnvelope) -> None:
        if envelope.type == _ORDER_PLACED_TYPE:
            await self._handle_placed(envelope)
        elif envelope.type == _ORDER_SHIPPED_TYPE:
            await self._handle_shipped(envelope)
        else:
            log.debug(
                "notifications_unhandled_type",
                message_id=str(envelope.message_id),
                type=envelope.type,
            )

    async def _handle_placed(self, envelope: EventEnvelope) -> None:
        try:
            payload = _OrderPlacedPayload.model_validate(envelope.payload)
        except ValidationError:
            log.exception(
                "notifications_bad_payload",
                message_id=str(envelope.message_id),
                type=envelope.type,
            )
            return
        await self._sink.send(
            NotificationEvent(
                kind="order_placed",
                order_id=payload.order_id,
                customer_id=payload.customer_id,
                seller_id=None,
                occurred_at=payload.placed_at,
                metadata={"total": str(payload.total_amount)},
            )
        )

    async def _handle_shipped(self, envelope: EventEnvelope) -> None:
        try:
            payload = _OrderShippedPayload.model_validate(envelope.payload)
        except ValidationError:
            log.exception(
                "notifications_bad_payload",
                message_id=str(envelope.message_id),
                type=envelope.type,
            )
            return
        await self._sink.send(
            NotificationEvent(
                kind="order_shipped",
                order_id=payload.order_id,
                customer_id=None,
                seller_id=payload.seller_id,
                occurred_at=payload.shipped_at,
            )
        )

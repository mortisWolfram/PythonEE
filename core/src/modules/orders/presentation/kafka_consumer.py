from __future__ import annotations

from typing import Protocol

import structlog

from modules.orders.application.commands import (
    CreateOrder,
    CreateOrderItemInput,
    ShipOrder,
)
from modules.orders.application.handlers import (
    CreateOrderHandler,
    ShipOrderHandler,
)
from modules.orders.presentation.dto import (
    CreateOrderPayloadDTO,
    ShipOrderPayloadDTO,
)
from shared.config.settings import Settings
from shared.idempotency.store import IdempotencyStore
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.envelope import EventEnvelope
from shared.kafka.producer import KafkaProducer
from shared.kafka.serializer import JsonSerializer

from typing import Any

log = structlog.get_logger(__name__)

ORDERS_CREATE_COMMANDS_TOPIC = "orders.commands.create.v1"
ORDERS_SHIP_COMMANDS_TOPIC = "orders.commands.ship.v1"


class OrdersCreateEnvelopeHandler:
    """Adapter: ``EventEnvelope`` → ``CreateOrder`` → ``CreateOrderHandler``.

    Extracts ``messageId`` / ``correlationId`` / ``tenantId`` from the envelope
    and forwards them as command context (arch §4.1).
    """

    def __init__(self, handler: CreateOrderHandler) -> None:
        self._handler = handler

    async def __call__(self, envelope: EventEnvelope) -> None:
        dto = CreateOrderPayloadDTO.model_validate(envelope.payload)
        command = CreateOrder(
            message_id=envelope.message_id,
            correlation_id=envelope.correlation_id,
            tenant_id=envelope.tenant_id,
            customer_id=dto.customer_id,
            items=tuple(
                CreateOrderItemInput(
                    product_id=i.product_id,
                    seller_id=i.seller_id,
                    quantity=i.quantity,
                    unit_price=i.unit_price,
                )
                for i in dto.items
            ),
        )
        await self._handler.handle(command)


class OrdersShipEnvelopeHandler:
    """Adapter: ``EventEnvelope`` → ``ShipOrder`` → ``ShipOrderHandler``."""

    def __init__(self, handler: ShipOrderHandler) -> None:
        self._handler = handler

    async def __call__(self, envelope: EventEnvelope) -> None:
        dto = ShipOrderPayloadDTO.model_validate(envelope.payload)
        command = ShipOrder(
            message_id=envelope.message_id,
            correlation_id=envelope.correlation_id,
            tenant_id=envelope.tenant_id,
            order_id=dto.order_id,
            seller_id=dto.seller_id,
        )
        await self._handler.handle(command)

def _build_aiokafka_consumer(
    *, topic: str, settings: Settings, group_id_suffix: str
) -> Any:
    from aiokafka import AIOKafkaConsumer

    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"{settings.kafka_group_id}.{group_id_suffix}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )


def build_orders_create_consumer(
    *,
    settings: Settings,
    idempotency: IdempotencyStore,
    handler: CreateOrderHandler,
    dlq_producer: KafkaProducer | None = None,
) -> KafkaConsumer:
    underlying = _build_aiokafka_consumer(
        topic=ORDERS_CREATE_COMMANDS_TOPIC,
        settings=settings,
        group_id_suffix="orders.create",
    )
    return KafkaConsumer(
        consumer=underlying,  # type: ignore[arg-type]
        serializer=JsonSerializer(),
        idempotency=idempotency,
        handler=OrdersCreateEnvelopeHandler(handler),
        dlq_producer=dlq_producer,
    )


def build_orders_ship_consumer(
    *,
    settings: Settings,
    idempotency: IdempotencyStore,
    handler: ShipOrderHandler,
    dlq_producer: KafkaProducer | None = None,
) -> KafkaConsumer:
    underlying = _build_aiokafka_consumer(
        topic=ORDERS_SHIP_COMMANDS_TOPIC,
        settings=settings,
        group_id_suffix="orders.ship",
    )
    return KafkaConsumer(
        consumer=underlying,  # type: ignore[arg-type]
        serializer=JsonSerializer(),
        idempotency=idempotency,
        handler=OrdersShipEnvelopeHandler(handler),
        dlq_producer=dlq_producer,
    )

from __future__ import annotations

from typing import Any

from modules.notifications.application.handlers import NotificationsEventHandler
from modules.notifications.application.sink import NotificationSink
from shared.config.settings import Settings
from shared.idempotency.store import IdempotencyStore
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.producer import KafkaProducer
from shared.kafka.serializer import JsonSerializer

ORDER_PLACED_EVENTS_TOPIC = "orders.events.order-placed.v1"
ORDER_SHIPPED_EVENTS_TOPIC = "orders.events.order-shipped.v1"


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


def build_order_placed_consumer(
    *,
    settings: Settings,
    idempotency: IdempotencyStore,
    sink: NotificationSink,
    dlq_producer: KafkaProducer | None = None,
) -> KafkaConsumer:
    return KafkaConsumer(
        consumer=_build_aiokafka_consumer(
            topic=ORDER_PLACED_EVENTS_TOPIC,
            settings=settings,
            group_id_suffix="notifications.order-placed",
        ),
        serializer=JsonSerializer(),
        idempotency=idempotency,
        handler=NotificationsEventHandler(sink),
        dlq_producer=dlq_producer,
    )


def build_order_shipped_consumer(
    *,
    settings: Settings,
    idempotency: IdempotencyStore,
    sink: NotificationSink,
    dlq_producer: KafkaProducer | None = None,
) -> KafkaConsumer:
    return KafkaConsumer(
        consumer=_build_aiokafka_consumer(
            topic=ORDER_SHIPPED_EVENTS_TOPIC,
            settings=settings,
            group_id_suffix="notifications.order-shipped",
        ),
        serializer=JsonSerializer(),
        idempotency=idempotency,
        handler=NotificationsEventHandler(sink),
        dlq_producer=dlq_producer,
    )

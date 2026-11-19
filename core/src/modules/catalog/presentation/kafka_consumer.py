from __future__ import annotations

from typing import Any

from modules.catalog.application.commands import CreateProduct
from modules.catalog.application.handlers import CreateProductHandler
from modules.catalog.presentation.dto import CreateProductPayloadDTO
from shared.config.settings import Settings
from shared.idempotency.store import IdempotencyStore
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.envelope import EventEnvelope
from shared.kafka.producer import KafkaProducer
from shared.kafka.serializer import JsonSerializer

CATALOG_CREATE_PRODUCT_COMMANDS_TOPIC = "catalog.commands.create-product.v1"


class CatalogCreateProductEnvelopeHandler:
    """``EventEnvelope`` → ``CreateProduct`` → ``CreateProductHandler``."""

    def __init__(self, handler: CreateProductHandler) -> None:
        self._handler = handler

    async def __call__(self, envelope: EventEnvelope) -> None:
        dto = CreateProductPayloadDTO.model_validate(envelope.payload)
        command = CreateProduct(
            message_id=envelope.message_id,
            correlation_id=envelope.correlation_id,
            tenant_id=envelope.tenant_id,
            seller_id=dto.seller_id,
            name=dto.name,
            price=dto.price,
            quantity=dto.quantity,
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


def build_catalog_create_product_consumer(
    *,
    settings: Settings,
    idempotency: IdempotencyStore,
    handler: CreateProductHandler,
    dlq_producer: KafkaProducer | None = None,
) -> KafkaConsumer:
    underlying = _build_aiokafka_consumer(
        topic=CATALOG_CREATE_PRODUCT_COMMANDS_TOPIC,
        settings=settings,
        group_id_suffix="catalog.create-product",
    )
    return KafkaConsumer(
        consumer=underlying,
        serializer=JsonSerializer(),
        idempotency=idempotency,
        handler=CatalogCreateProductEnvelopeHandler(handler),
        dlq_producer=dlq_producer,
    )

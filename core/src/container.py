from __future__ import annotations

import asyncio

import structlog
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from modules.catalog.application.handlers import CreateProductHandler
from modules.catalog.infrastructure.unit_of_work import (
    SQLAlchemyProductUnitOfWork,
)
from modules.catalog.presentation.kafka_consumer import (
    build_catalog_create_product_consumer,
)
from modules.notifications.application.sink import LogSink, NotificationSink
from modules.notifications.presentation.kafka_consumer import (
    build_order_placed_consumer,
    build_order_shipped_consumer,
)
from modules.orders.application.handlers import (
    CreateOrderHandler,
    ShipOrderHandler,
)
from modules.orders.infrastructure.unit_of_work import (
    SQLAlchemyOrderUnitOfWork,
)
from modules.orders.presentation.kafka_consumer import (
    build_orders_create_consumer,
    build_orders_ship_consumer,
)
from shared.config.settings import Settings
from shared.idempotency.postgres import PostgresIdempotencyStore
from shared.kafka.consumer import KafkaConsumer
from shared.kafka.producer import KafkaProducer
from shared.outbox.relay import OutboxRelay

log = structlog.get_logger(__name__)


class Container:
    """Composition root.

    Owns the lifecycle of every long-running component: Kafka producer,
    five consumers (2 orders, 1 catalog, 2 notifications), outbox relay,
    and the SQLAlchemy engine. ``start()`` launches asyncio tasks;
    ``stop()`` performs a graceful shutdown (SIGTERM-friendly).
    """

    def __init__(
        self,
        *,
        settings: Settings,
        engine: AsyncEngine,
        session_factory: async_sessionmaker[AsyncSession],
        producer: KafkaProducer,
        outbox_relay: OutboxRelay,
        consumers: list[KafkaConsumer],
    ) -> None:
        self._settings = settings
        self._engine = engine
        self._session_factory = session_factory
        self._producer = producer
        self._outbox_relay = outbox_relay
        self._consumers = consumers
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._started = False

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        return self._session_factory

    @property
    def consumers(self) -> list[KafkaConsumer]:
        return self._consumers

    async def start(self) -> None:
        if self._started:
            return
        await self._producer.start()
        for consumer in self._consumers:
            task = asyncio.create_task(
                consumer.run(),
                name=f"consumer:{type(consumer).__name__}:{id(consumer)}",
            )
            self._tasks.append(task)
        relay_task = asyncio.create_task(
            self._outbox_relay.run(self._stop_event), name="outbox-relay"
        )
        self._tasks.append(relay_task)
        self._started = True
        log.info("core_started", consumers=len(self._consumers))

    async def stop(self, timeout_s: float = 30.0) -> None:
        if not self._started:
            return
        log.info("core_stopping")

        self._stop_event.set()
        for consumer in self._consumers:
            await consumer.stop()

        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=timeout_s,
                )
            except TimeoutError:
                log.warning("core_shutdown_timeout", timeout_s=timeout_s)
                for task in self._tasks:
                    task.cancel()
                await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

        await self._producer.stop()
        await self._engine.dispose()
        self._started = False
        log.info("core_stopped")


def build_container(
    settings: Settings,
    *,
    sink: NotificationSink | None = None,
) -> Container:
    """Assemble dependencies.

    ``sink`` is injectable so e2e tests can swap ``LogSink`` for
    ``InMemorySink`` without touching production wiring.
    """
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    producer = KafkaProducer.from_settings(settings)
    idempotency = PostgresIdempotencyStore(session_factory)
    effective_sink: NotificationSink = sink or LogSink()

    order_uow_factory = lambda: SQLAlchemyOrderUnitOfWork(  # noqa: E731
        session_factory
    )
    catalog_uow_factory = lambda: SQLAlchemyProductUnitOfWork(  # noqa: E731
        session_factory
    )

    create_order_handler = CreateOrderHandler(order_uow_factory)
    ship_order_handler = ShipOrderHandler(order_uow_factory)
    create_product_handler = CreateProductHandler(catalog_uow_factory)

    consumers: list[KafkaConsumer] = [
        build_orders_create_consumer(
            settings=settings,
            idempotency=idempotency,
            handler=create_order_handler,
            dlq_producer=producer,
        ),
        build_orders_ship_consumer(
            settings=settings,
            idempotency=idempotency,
            handler=ship_order_handler,
            dlq_producer=producer,
        ),
        build_catalog_create_product_consumer(
            settings=settings,
            idempotency=idempotency,
            handler=create_product_handler,
            dlq_producer=producer,
        ),
        build_order_placed_consumer(
            settings=settings,
            idempotency=idempotency,
            sink=effective_sink,
            dlq_producer=producer,
        ),
        build_order_shipped_consumer(
            settings=settings,
            idempotency=idempotency,
            sink=effective_sink,
            dlq_producer=producer,
        ),
    ]

    relay = OutboxRelay(
        session_factory=session_factory,
        producer=producer,
        poll_interval_ms=settings.outbox_poll_interval_ms,
    )

    return Container(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        producer=producer,
        outbox_relay=relay,
        consumers=consumers,
    )

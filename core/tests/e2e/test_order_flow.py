"""End-to-end test for the §5 scenario.

Covers, in order:

1. ``catalog.commands.create-product.v1`` -> row in ``products``
2. ``orders.commands.create.v1`` -> row in ``orders`` + ``orders.events.order-placed.v1``
3. ``notifications`` ``InMemorySink`` received ``order_placed``
4. Republish same create command (same ``messageId``) -> no duplicate in DB, no second event
5. ``orders.commands.ship.v1`` -> ``status=shipped`` + ``orders.events.order-shipped.v1``
6. Double-ship with new ``messageId`` -> handler raises ``CannotShipOrderError`` -> no event
7. Ship with foreign ``sellerId`` -> handler raises ``AccessDeniedError`` -> no event
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from sqlalchemy import func, select
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from container import Container, build_container
from modules.catalog.infrastructure.models import ProductModel
from modules.notifications.application.sink import InMemorySink
from modules.orders.infrastructure.models import OrderModel
from shared.config.settings import Settings
from shared.kafka.envelope import EventEnvelope
from shared.kafka.producer import KafkaProducer
from shared.kafka.serializer import JsonSerializer

pytestmark = pytest.mark.e2e

_serializer = JsonSerializer()

_TOPICS: tuple[str, ...] = (
    "orders.commands.create.v1",
    "orders.commands.ship.v1",
    "catalog.commands.create-product.v1",
    "orders.events.order-placed.v1",
    "orders.events.order-shipped.v1",
)

_EVENT_WAIT_TIMEOUT_S = 10.0
_QUIET_WINDOW_S = 2.0
_DB_WAIT_TIMEOUT_S = 5.0
_STARTUP_GRACE_S = 2.0


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


async def _publish_command(
    producer: KafkaProducer,
    *,
    topic: str,
    key: str,
    type_: str,
    payload: dict[str, Any],
    message_id: UUID | None = None,
    correlation_id: UUID | None = None,
    tenant_id: str | None = "tenant-1",
) -> EventEnvelope:
    envelope = EventEnvelope(
        message_id=message_id or uuid4(),
        correlation_id=correlation_id or uuid4(),
        causation_id=None,
        type=type_,
        version=1,
        timestamp=datetime.now(UTC),
        producer="e2e",
        tenant_id=tenant_id,
        capabilities=[],
        payload=payload,
    )
    await producer.send(topic, key=key, envelope=envelope)
    return envelope


async def _wait_for_event(
    consumer: AIOKafkaConsumer,
    *,
    event_type: str,
    predicate: Callable[[EventEnvelope], bool],
    wait_s: float = _EVENT_WAIT_TIMEOUT_S,     # ← было timeout
) -> EventEnvelope:
    deadline = time.monotonic() + wait_s       # ← переименовано
    while time.monotonic() < deadline:
        remaining = max(deadline - time.monotonic(), 0.1)
        try:
            msg = await asyncio.wait_for(consumer.getone(), timeout=remaining)
        except TimeoutError:
            break
        envelope = _serializer.deserialize(msg.value)
        if envelope.type == event_type and predicate(envelope):
            return envelope
    raise AssertionError(f"did not receive {event_type!r} within {wait_s}s")  # ← в f-string

async def _drain(
    consumer: AIOKafkaConsumer,
    *,
    wait_s: float = _QUIET_WINDOW_S,            # ← было timeout
) -> list[EventEnvelope]:
    """Read events for ``wait_s`` seconds; return whatever was collected."""
    events: list[EventEnvelope] = []
    deadline = time.monotonic() + wait_s       # ← переименовано
    while time.monotonic() < deadline:
        remaining = max(deadline - time.monotonic(), 0.1)
        try:
            msg = await asyncio.wait_for(consumer.getone(), timeout=remaining)
        except TimeoutError:
            break
        events.append(_serializer.deserialize(msg.value))
    return events


async def _wait_db(
    container: Container,
    *,
    check: Callable[[Any], bool],
    model: type[Any],
    wait_s: float = _DB_WAIT_TIMEOUT_S,
) -> Any:
    deadline = time.monotonic() + wait_s
    while time.monotonic() < deadline:
        async with container.session_factory() as session:
            rows: Sequence[Any] = (
                (await session.execute(select(model))).scalars().all()
            )
        for row in rows:
            if check(row):
                return row
        await asyncio.sleep(0.1)
    raise AssertionError(f"{model.__name__} row not found within {wait_s}s")


async def _wait_condition(
    predicate: Callable[[], bool],
    *,
    wait_s: float,                             # ← было timeout
    description: str,
) -> None:
    deadline = time.monotonic() + wait_s       # ← переименовано
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"condition not met within {wait_s}s: {description}")  # ← f-string


async def _create_topics(bootstrap: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in _TOPICS]
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        await admin.close()


def _run_alembic_upgrade() -> None:
    cfg = AlembicConfig("alembic.ini")
    alembic_command.upgrade(cfg, "head")


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def postgres_url(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[str]:
    with PostgresContainer("postgres:16-alpine") as pg:
        raw = pg.get_connection_url()
        url = raw.replace("postgresql+psycopg2", "postgresql+asyncpg")

        # ``alembic/env.py`` calls ``asyncio.run(...)`` internally, which fails
        # from within an already-running event loop (pytest-asyncio). Offload
        # the migration to a worker thread — there is no running loop there.
        monkeypatch.setenv("DATABASE_URL", url)
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "unused-for-migrations")
        await asyncio.to_thread(_run_alembic_upgrade)

        yield url


@pytest_asyncio.fixture
async def kafka_bootstrap() -> AsyncIterator[str]:
    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        yield kafka.get_bootstrap_server()


@pytest_asyncio.fixture
async def settings(
    postgres_url: str, kafka_bootstrap: str
) -> Settings:
    return Settings(
        database_url=postgres_url,
        kafka_bootstrap_servers=kafka_bootstrap,
        kafka_group_id=f"e2e-core-{uuid4()}",
        schema_registry_url=None,
        log_level="INFO",
        outbox_poll_interval_ms=100,
    )


@pytest_asyncio.fixture
async def sink() -> InMemorySink:
    return InMemorySink()


@pytest_asyncio.fixture
async def kafka_topics(settings: Settings) -> AsyncIterator[None]:
    await _create_topics(settings.kafka_bootstrap_servers)
    yield


@pytest_asyncio.fixture
async def container(
    settings: Settings, sink: InMemorySink, kafka_topics: None
) -> AsyncIterator[Container]:
    c = build_container(settings, sink=sink)
    await c.start()
    # ``Container.start()`` creates consumer tasks asynchronously; give
    # aiokafka a moment to join consumer groups before publishing.
    await asyncio.sleep(_STARTUP_GRACE_S)
    try:
        yield c
    finally:
        await c.stop()


@pytest_asyncio.fixture
async def producer(
    settings: Settings, kafka_topics: None
) -> AsyncIterator[KafkaProducer]:
    p = KafkaProducer.from_settings(settings)
    await p.start()
    try:
        yield p
    finally:
        await p.stop()


@pytest_asyncio.fixture
async def reader(
    settings: Settings, kafka_topics: None
) -> AsyncIterator[AIOKafkaConsumer]:
    consumer = AIOKafkaConsumer(
        "orders.events.order-placed.v1",
        "orders.events.order-shipped.v1",
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"e2e-reader-{uuid4()}",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.stop()


# ---------------------------------------------------------------------------
# the one and only e2e test
# ---------------------------------------------------------------------------


async def test_order_flow_end_to_end(
    container: Container,
    sink: InMemorySink,
    producer: KafkaProducer,
    reader: AIOKafkaConsumer,
) -> None:
    seller_id = uuid4()
    product_id = uuid4()
    customer_id = uuid4()

    # ---- step 1: create product (catalog consumer writes to DB) -------------
    await _publish_command(
        producer,
        topic="catalog.commands.create-product.v1",
        key=str(product_id),
        type_="catalog.commands.create-product",
        payload={
            "sellerId": str(seller_id),
            "name": "Widget",
            "price": "100.00",
            "quantity": 10,
        },
    )
    product = await _wait_db(
        container,
        model=ProductModel,
        check=lambda r: r.seller_id == seller_id,
    )
    assert product.name == "Widget"
    assert str(product.price_amount) == "100.00"

    # ---- step 2: create order ----------------------------------------------
    create_msg = await _publish_command(
        producer,
        topic="orders.commands.create.v1",
        key=str(customer_id),
        type_="orders.commands.create",
        payload={
            "customerId": str(customer_id),
            "items": [
                {
                    "productId": str(product_id),
                    "sellerId": str(seller_id),
                    "quantity": 2,
                    "unitPrice": "100.00",
                }
            ],
        },
    )

    # ---- step 3: order-placed event + causation/correlation ----------------
    placed = await _wait_for_event(
        reader,
        event_type="orders.events.order-placed",
        predicate=lambda e: e.payload["customerId"] == str(customer_id),
    )
    assert placed.causation_id == create_msg.message_id
    assert placed.correlation_id == create_msg.correlation_id
    assert placed.tenant_id == "tenant-1"
    assert placed.producer == "core.orders"
    assert placed.payload["totalAmount"] == {"amount": "200.00", "currency": "RUB"}
    assert len(placed.payload["items"]) == 1
    order_id = UUID(placed.payload["orderId"])

    # ---- step 4: notifications sink ----------------------------------------
    await _wait_condition(
        lambda: any(
            e.kind == "order_placed" and e.order_id == order_id for e in sink.events
        ),
        wait_s=5.0,  # ← было timeout
        description="sink received order_placed",
    )

    # ---- step 5: idempotency for create ------------------------------------
    await _publish_command(
        producer,
        topic="orders.commands.create.v1",
        key=str(customer_id),
        type_="orders.commands.create",
        message_id=create_msg.message_id,
        correlation_id=create_msg.correlation_id,
        payload=create_msg.payload,
    )
    extra_events = await _drain(reader)
    assert not any(
        e.type == "orders.events.order-placed" for e in extra_events
    ), "idempotency violated: duplicate order-placed event"
    async with container.session_factory() as session:
        count = (
            await session.execute(
                select(func.count())
                .select_from(OrderModel)
                .where(OrderModel.id == order_id)
            )
        ).scalar_one()
    assert count == 1, "idempotency violated: duplicate order in DB"

    # ---- step 6: ship ------------------------------------------------------
    ship_msg = await _publish_command(
        producer,
        topic="orders.commands.ship.v1",
        key=str(order_id),
        type_="orders.commands.ship",
        payload={
            "orderId": str(order_id),
            "sellerId": str(seller_id),
        },
    )
    shipped = await _wait_for_event(
        reader,
        event_type="orders.events.order-shipped",
        predicate=lambda e: e.payload["orderId"] == str(order_id),
    )
    assert shipped.causation_id == ship_msg.message_id
    assert shipped.correlation_id == ship_msg.correlation_id
    async with container.session_factory() as session:
        order_row = (
            await session.execute(
                select(OrderModel).where(OrderModel.id == order_id)
            )
        ).scalar_one()
    assert order_row.status == "shipped"
    assert order_row.shipped_at is not None

    # ---- step 7: double-ship with a *new* messageId -> CannotShipOrderError
    await _publish_command(
        producer,
        topic="orders.commands.ship.v1",
        key=str(order_id),
        type_="orders.commands.ship",
        payload={
            "orderId": str(order_id),
            "sellerId": str(seller_id),
        },
    )
    extra_events = await _drain(reader)
    assert not any(
        e.type == "orders.events.order-shipped" for e in extra_events
    ), "double-ship produced an unexpected event"

    # ---- step 8: foreign seller -> AccessDeniedError -----------------------
    await _publish_command(
        producer,
        topic="orders.commands.ship.v1",
        key=str(order_id),
        type_="orders.commands.ship",
        payload={
            "orderId": str(order_id),
            "sellerId": str(uuid4()),
        },
    )
    extra_events = await _drain(reader)
    assert not any(
        e.type == "orders.events.order-shipped" for e in extra_events
    ), "access-denied was not enforced"

    # ---- final: order state is unchanged after rejected transitions -------
    async with container.session_factory() as session:
        final_order = (
            await session.execute(
                select(OrderModel).where(OrderModel.id == order_id)
            )
        ).scalar_one()
    assert final_order.status == "shipped"

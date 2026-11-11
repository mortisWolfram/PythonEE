from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.idempotency.postgres import PostgresIdempotencyStore

pytestmark = pytest.mark.integration


async def test_not_processed_initially(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    store = PostgresIdempotencyStore(session_factory)
    assert await store.is_processed(uuid4()) is False


async def test_mark_then_check(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    store = PostgresIdempotencyStore(session_factory)
    mid = uuid4()
    await store.mark_processed(mid)
    assert await store.is_processed(mid) is True


async def test_mark_is_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    store = PostgresIdempotencyStore(session_factory)
    mid = uuid4()
    await store.mark_processed(mid)
    await store.mark_processed(mid)  # no error, no duplicate row
    assert await store.is_processed(mid) is True


async def test_distinct_ids_independent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    store = PostgresIdempotencyStore(session_factory)
    a, b = uuid4(), uuid4()
    await store.mark_processed(a)
    assert await store.is_processed(a) is True
    assert await store.is_processed(b) is False

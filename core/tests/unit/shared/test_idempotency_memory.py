from uuid import uuid4

from shared.idempotency.memory import InMemoryIdempotencyStore


async def test_not_processed_initially() -> None:
    store = InMemoryIdempotencyStore()
    assert await store.is_processed(uuid4()) is False


async def test_mark_then_check() -> None:
    store = InMemoryIdempotencyStore()
    mid = uuid4()
    await store.mark_processed(mid)
    assert await store.is_processed(mid) is True


async def test_message_ids_are_independent() -> None:
    store = InMemoryIdempotencyStore()
    a, b = uuid4(), uuid4()
    await store.mark_processed(a)
    assert await store.is_processed(a) is True
    assert await store.is_processed(b) is False


async def test_mark_is_idempotent() -> None:
    store = InMemoryIdempotencyStore()
    mid = uuid4()
    await store.mark_processed(mid)
    await store.mark_processed(mid)
    assert await store.is_processed(mid) is True

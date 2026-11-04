from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from shared.kafka.envelope import EventEnvelope


def _valid_kwargs() -> dict[str, object]:
    return {
        "message_id": uuid4(),
        "correlation_id": uuid4(),
        "type": "orders.commands.create",
        "version": 1,
        "timestamp": datetime.now(UTC),
        "producer": "test",
        "payload": {"k": "v"},
    }


def test_construct_with_snake_case() -> None:
    e = EventEnvelope(**_valid_kwargs())  # type: ignore[arg-type]
    assert isinstance(e.message_id, UUID)
    assert e.capabilities == []
    assert e.causation_id is None
    assert e.tenant_id is None


def test_construct_with_camel_case_aliases() -> None:
    raw = {
        "messageId": str(uuid4()),
        "correlationId": str(uuid4()),
        "type": "t",
        "version": 2,
        "timestamp": datetime.now(UTC).isoformat(),
        "producer": "p",
        "tenantId": "t-1",
        "capabilities": ["a.b"],
        "payload": {},
    }
    e = EventEnvelope.model_validate(raw)
    assert e.tenant_id == "t-1"
    assert e.capabilities == ["a.b"]


def test_wire_format_is_camel_case() -> None:
    e = EventEnvelope(**_valid_kwargs())  # type: ignore[arg-type]
    wire = e.model_dump(by_alias=True)
    assert "messageId" in wire
    assert "correlationId" in wire
    assert "causationId" in wire
    assert "tenantId" in wire


def test_timestamp_must_be_aware() -> None:
    kwargs = _valid_kwargs()
    kwargs["timestamp"] = datetime(2026, 1, 1, 0, 0, 0)  # naive
    with pytest.raises(ValidationError):
        EventEnvelope(**kwargs)  # type: ignore[arg-type]


def test_version_must_be_positive() -> None:
    kwargs = _valid_kwargs()
    kwargs["version"] = 0
    with pytest.raises(ValidationError):
        EventEnvelope(**kwargs)  # type: ignore[arg-type]


def test_missing_required_field_raises() -> None:
    kwargs = _valid_kwargs()
    del kwargs["producer"]
    with pytest.raises(ValidationError):
        EventEnvelope(**kwargs)  # type: ignore[arg-type]


def test_extra_field_forbidden() -> None:
    kwargs = _valid_kwargs()
    kwargs["unexpected"] = "x"
    with pytest.raises(ValidationError):
        EventEnvelope(**kwargs)  # type: ignore[arg-type]


def test_frozen() -> None:
    e = EventEnvelope(**_valid_kwargs())  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        e.producer = "other"  # type: ignore[misc]


def test_new_populates_defaults() -> None:
    e = EventEnvelope.new(
        type="orders.events.order-placed",
        version=1,
        producer="test",
        payload={"x": 1},
    )
    assert e.causation_id is None
    assert e.tenant_id is None
    assert e.capabilities == []
    assert e.timestamp.tzinfo is not None


def test_new_preserves_explicit_ids() -> None:
    corr = uuid4()
    cause = uuid4()
    e = EventEnvelope.new(
        type="t",
        version=1,
        producer="p",
        payload={},
        correlation_id=corr,
        causation_id=cause,
        tenant_id="t-1",
        capabilities=["a.b"],
    )
    assert e.correlation_id == corr
    assert e.causation_id == cause
    assert e.tenant_id == "t-1"
    assert e.capabilities == ["a.b"]

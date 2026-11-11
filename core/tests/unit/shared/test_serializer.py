import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from shared.kafka.envelope import EventEnvelope
from shared.kafka.errors import SerializationError
from shared.kafka.serializer import JsonSerializer


def _envelope() -> EventEnvelope:
    return EventEnvelope.new(
        type="orders.events.order-placed",
        version=1,
        producer="test",
        payload={"order_id": "o-1", "total": "100.00"},
        tenant_id="t-1",
        capabilities=["orders.order.create"],
    )


def test_round_trip() -> None:
    s = JsonSerializer()
    e = _envelope()
    assert s.deserialize(s.serialize(e)) == e


def test_serialized_form_uses_camel_case_and_omits_nulls() -> None:
    s = JsonSerializer()
    raw = s.serialize(_envelope())
    obj = json.loads(raw.decode("utf-8"))
    assert "messageId" in obj
    assert "causationId" not in obj  # None → excluded
    assert "tenantId" in obj
    assert obj["type"] == "orders.events.order-placed"
    assert obj["capabilities"] == ["orders.order.create"]


def test_invalid_json_raises_serialization_error() -> None:
    s = JsonSerializer()
    with pytest.raises(SerializationError):
        s.deserialize(b"{not-json")


def test_non_utf8_raises_serialization_error() -> None:
    s = JsonSerializer()
    with pytest.raises(SerializationError):
        s.deserialize(b"\xff\xfe\x00")


def test_missing_required_field_raises_serialization_error() -> None:
    s = JsonSerializer()
    bad = json.dumps(
        {
            "messageId": str(uuid4()),
            # "correlationId" missing
            "type": "t",
            "version": 1,
            "timestamp": datetime.now(UTC).isoformat(),
            "producer": "p",
            "payload": {},
        }
    ).encode("utf-8")
    with pytest.raises(SerializationError):
        s.deserialize(bad)

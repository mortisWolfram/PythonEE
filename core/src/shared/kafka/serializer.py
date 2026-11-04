from __future__ import annotations

from abc import ABC, abstractmethod

from shared.kafka.envelope import EventEnvelope
from shared.kafka.errors import SerializationError


class Serializer(ABC):
    @abstractmethod
    def serialize(self, envelope: EventEnvelope) -> bytes: ...

    @abstractmethod
    def deserialize(self, data: bytes) -> EventEnvelope: ...


class JsonSerializer(Serializer):
    """JSON serializer using camelCase aliases and no null fields.

    # TODO: replace with AvroSerializer backed by Schema Registry
    #       (see docs/adr/0001-kafka-client.md).
    """

    def serialize(self, envelope: EventEnvelope) -> bytes:
        return envelope.model_dump_json(by_alias=True, exclude_none=True).encode("utf-8")

    def deserialize(self, data: bytes) -> EventEnvelope:
        try:
            return EventEnvelope.model_validate_json(data)
        except Exception as exc:  # pydantic ValidationError, UnicodeDecodeError, ...
            raise SerializationError(f"failed to deserialize envelope: {exc}") from exc

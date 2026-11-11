from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class EventEnvelope(BaseModel):
    """Message envelope per docs/architecture.md §5.2.

    Wire format is camelCase (via aliases); Python code uses snake_case.
    Construct with either name thanks to ``populate_by_name=True``.
    Serialize with ``by_alias=True`` for the Kafka wire format.
    """

    model_config = ConfigDict(
        frozen=True,
        populate_by_name=True,
        extra="forbid",
    )

    message_id: UUID = Field(alias="messageId")
    correlation_id: UUID = Field(alias="correlationId")
    causation_id: UUID | None = Field(default=None, alias="causationId")
    type: str
    version: int = Field(ge=1)
    timestamp: datetime
    producer: str
    tenant_id: str | None = Field(default=None, alias="tenantId")
    capabilities: list[str] = Field(default_factory=list)
    payload: dict[str, Any]

    @field_validator("timestamp")
    @classmethod
    def _timestamp_must_be_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return v

    @classmethod
    def new(
        cls,
        *,
        type: str,
        version: int,
        producer: str,
        payload: dict[str, Any],
        correlation_id: UUID | None = None,
        causation_id: UUID | None = None,
        tenant_id: str | None = None,
        capabilities: list[str] | None = None,
    ) -> EventEnvelope:
        """Build a fresh envelope with generated ``messageId``/``timestamp``."""
        return cls(
            message_id=uuid4(),
            correlation_id=correlation_id or uuid4(),
            causation_id=causation_id,
            type=type,
            version=version,
            timestamp=datetime.now(UTC),
            producer=producer,
            tenant_id=tenant_id,
            capabilities=capabilities or [],
            payload=payload,
        )

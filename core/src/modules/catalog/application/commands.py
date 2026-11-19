from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from shared.application.command import Command


@dataclass(frozen=True)
class CreateProduct(Command):
    message_id: UUID
    correlation_id: UUID
    tenant_id: str | None
    seller_id: UUID
    name: str
    price: str  # wire format "100.00"; converted via Money.of (RUB default)
    quantity: int

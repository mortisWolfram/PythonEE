from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class CreateProductPayloadDTO(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="forbid")

    seller_id: UUID = Field(alias="sellerId")
    name: str = Field(min_length=1, max_length=255)
    price: str  # "100.00", converted via Money.of (RUB default)
    quantity: int = Field(ge=0)

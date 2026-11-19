from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class CreateOrderItemDTO(BaseModel):
    """Item inside a ``orders.commands.create.v1`` payload.

    Wire format is camelCase; ``populate_by_name`` also accepts snake_case.
    """

    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="forbid")

    product_id: UUID = Field(alias="productId")
    seller_id: UUID = Field(alias="sellerId")
    quantity: int = Field(gt=0)
    unit_price: str = Field(alias="unitPrice")


class CreateOrderPayloadDTO(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="forbid")

    customer_id: UUID = Field(alias="customerId")
    items: list[CreateOrderItemDTO] = Field(min_length=1)


class ShipOrderPayloadDTO(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="forbid")

    order_id: UUID = Field(alias="orderId")
    seller_id: UUID = Field(alias="sellerId")

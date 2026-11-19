from __future__ import annotations

from collections.abc import Callable

import structlog

from modules.catalog.application.commands import CreateProduct
from modules.catalog.application.unit_of_work import ProductUnitOfWork
from modules.catalog.domain.entities import Product
from modules.catalog.domain.value_objects import (
    Money,
    ProductName,
    SellerId,
)
from shared.application.command_handler import CommandHandler

log = structlog.get_logger(__name__)


class CreateProductHandler(CommandHandler[CreateProduct]):
    def __init__(self, uow_factory: Callable[[], ProductUnitOfWork]) -> None:
        self._uow_factory = uow_factory

    async def handle(self, command: CreateProduct) -> None:
        product = Product.create(
            seller_id=SellerId(command.seller_id),
            name=ProductName(command.name),
            price=Money.of(command.price),
            quantity=command.quantity,
            tenant_id=command.tenant_id,
        )

        async with self._uow_factory() as uow:
            await uow.products.add(product)
            await uow.commit()

        log.info(
            "product_created",
            product_id=str(product.id.value),
            seller_id=str(command.seller_id),
            message_id=str(command.message_id),
            correlation_id=str(command.correlation_id),
        )

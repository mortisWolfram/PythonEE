from __future__ import annotations

from collections.abc import Callable

import structlog

from modules.orders.application.commands import CreateOrder, ShipOrder
from modules.orders.application.outbox import to_outbox
from modules.orders.application.unit_of_work import OrderUnitOfWork
from modules.orders.domain.entities import Order, OrderItem
from modules.orders.domain.exceptions import OrderNotFoundError
from modules.orders.domain.value_objects import (
    CustomerId,
    Money,
    OrderId,
    ProductId,
    SellerId,
)
from shared.application.command_handler import CommandHandler

log = structlog.get_logger(__name__)


class CreateOrderHandler(CommandHandler[CreateOrder]):
    def __init__(self, uow_factory: Callable[[], OrderUnitOfWork]) -> None:
        self._uow_factory = uow_factory

    async def handle(self, command: CreateOrder) -> None:
        items = [
            OrderItem(
                product_id=ProductId(i.product_id),
                seller_id=SellerId(i.seller_id),
                quantity=i.quantity,
                unit_price=Money.of(i.unit_price),
            )
            for i in command.items
        ]
        order = Order.create(
            customer_id=CustomerId(command.customer_id),
            items=items,
            tenant_id=command.tenant_id,
        )

        async with self._uow_factory() as uow:
            await uow.orders.add(order)
            for event in order.pull_events():
                await uow.outbox.add(
                    to_outbox(
                        event,
                        aggregate_id=str(order.id.value),
                        correlation_id=command.correlation_id,
                        causation_id=command.message_id,
                        tenant_id=command.tenant_id,
                    )
                )
            await uow.commit()

        log.info(
            "order_created",
            order_id=str(order.id.value),
            customer_id=str(command.customer_id),
            message_id=str(command.message_id),
            correlation_id=str(command.correlation_id),
        )


class ShipOrderHandler(CommandHandler[ShipOrder]):
    def __init__(self, uow_factory: Callable[[], OrderUnitOfWork]) -> None:
        self._uow_factory = uow_factory

    async def handle(self, command: ShipOrder) -> None:
        async with self._uow_factory() as uow:
            order = await uow.orders.get(OrderId(command.order_id))
            if order is None:
                raise OrderNotFoundError(str(command.order_id))

            order.ship(SellerId(command.seller_id))
            await uow.orders.update(order)

            for event in order.pull_events():
                await uow.outbox.add(
                    to_outbox(
                        event,
                        aggregate_id=str(order.id.value),
                        correlation_id=command.correlation_id,
                        causation_id=command.message_id,
                        tenant_id=command.tenant_id,
                    )
                )
            await uow.commit()

        log.info(
            "order_shipped",
            order_id=str(command.order_id),
            seller_id=str(command.seller_id),
            message_id=str(command.message_id),
            correlation_id=str(command.correlation_id),
        )

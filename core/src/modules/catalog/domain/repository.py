from __future__ import annotations

from abc import abstractmethod

from modules.catalog.domain.entities import Product
from modules.catalog.domain.value_objects import ProductId
from shared.domain.repository import Repository


class ProductRepository(Repository[Product, ProductId]):
    """Port for Product persistence."""

    @abstractmethod
    async def get_or_raise(self, id: ProductId) -> Product: ...

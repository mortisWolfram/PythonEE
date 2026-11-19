from __future__ import annotations

from abc import abstractmethod

from modules.catalog.domain.repository import ProductRepository
from shared.application.unit_of_work import UnitOfWork


class ProductUnitOfWork(UnitOfWork):
    """Transaction boundary for catalog aggregates.

    No outbox: catalog does not emit events in this iteration.
    """

    @property
    @abstractmethod
    def products(self) -> ProductRepository: ...

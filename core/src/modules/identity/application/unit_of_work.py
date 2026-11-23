from __future__ import annotations

from abc import abstractmethod

from modules.identity.domain.repository import UserRepository
from shared.application.unit_of_work import UnitOfWork


class IdentityUnitOfWork(UnitOfWork):
    """Transaction boundary for identity aggregates.

    No outbox — identity does not emit events in this iteration.
    """

    @property
    @abstractmethod
    def users(self) -> UserRepository: ...

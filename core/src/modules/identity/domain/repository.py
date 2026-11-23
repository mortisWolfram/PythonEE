from __future__ import annotations

from abc import abstractmethod

from modules.identity.domain.entities import User
from modules.identity.domain.value_objects import Email, UserId
from shared.domain.repository import Repository


class UserRepository(Repository[User, UserId]):
    """Port for User persistence. ``add``/``get`` inherited."""

    @abstractmethod
    async def find_by_email(self, email: Email) -> User | None: ...

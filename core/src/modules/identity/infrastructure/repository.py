from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.identity.domain.entities import User
from modules.identity.domain.repository import UserRepository
from modules.identity.domain.value_objects import (
    Capabilities,
    Email,
    Role,
    UserId,
)
from modules.identity.infrastructure.models import UserModel


class SQLAlchemyUserRepository(UserRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, user: User) -> None:
        self._session.add(_to_model(user))

    async def get(self, id: UserId) -> User | None:
        stmt = select(UserModel).where(UserModel.id == id.value)
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return _to_domain(row) if row is not None else None

    async def find_by_email(self, email: Email) -> User | None:
        stmt = select(UserModel).where(UserModel.email == email.value)
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return _to_domain(row) if row is not None else None


def _to_model(user: User) -> UserModel:
    return UserModel(
        id=user.id.value,
        email=user.email.value,
        role=user.role.value,
        capabilities=sorted(user.capabilities.values),
        is_active=user.is_active,
        tenant_id=user.tenant_id,
        created_at=user.created_at,
    )


def _to_domain(model: UserModel) -> User:
    return User(
        id=UserId(model.id),
        email=Email(model.email),
        role=Role(model.role),
        capabilities=Capabilities(frozenset(model.capabilities)),
        is_active=model.is_active,
        tenant_id=model.tenant_id,
        created_at=model.created_at,
    )

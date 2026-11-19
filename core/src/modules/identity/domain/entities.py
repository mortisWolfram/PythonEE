from __future__ import annotations

from datetime import UTC, datetime

from modules.identity.domain.exceptions import UserAlreadyDeactivatedError
from modules.identity.domain.value_objects import (
    Capabilities,
    Email,
    Role,
    UserId,
)
from shared.domain.aggregate_root import AggregateRoot


class User(AggregateRoot[UserId]):
    """User aggregate root.

    Minimal for this iteration: no password hash, no JWT, no domain events.
    Auth flow arrives in a later iteration.
    """

    def __init__(
        self,
        *,
        id: UserId,
        email: Email,
        role: Role,
        capabilities: Capabilities,
        is_active: bool = True,
        tenant_id: str | None = None,
        created_at: datetime | None = None,
    ) -> None:
        super().__init__()
        self.id = id
        self.email = email
        self.role = role
        self.capabilities = capabilities
        self.is_active = is_active
        self.tenant_id = tenant_id
        self.created_at = created_at or datetime.now(UTC)

    @classmethod
    def register(
        cls,
        *,
        email: Email,
        role: Role,
        capabilities: Capabilities,
        tenant_id: str | None = None,
    ) -> User:
        return cls(
            id=UserId.generate(),
            email=email,
            role=role,
            capabilities=capabilities,
            tenant_id=tenant_id,
        )

    def deactivate(self) -> None:
        if not self.is_active:
            raise UserAlreadyDeactivatedError(str(self.id))
        self.is_active = False

    def change_role(self, role: Role, capabilities: Capabilities) -> None:
        """Role change always replaces the capability set — no drift."""
        self.role = role
        self.capabilities = capabilities

    def grant(self, capability: str) -> None:
        self.capabilities = self.capabilities.with_added(capability)

    def revoke(self, capability: str) -> None:
        self.capabilities = self.capabilities.with_removed(capability)

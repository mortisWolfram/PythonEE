import pytest

from modules.identity.domain.entities import User
from modules.identity.domain.exceptions import UserAlreadyDeactivatedError
from modules.identity.domain.value_objects import (
    Capabilities,
    Email,
    Role,
)


def _make_user(
    *,
    email: str = "alice@example.com",
    role: Role = Role.CUSTOMER,
    caps: frozenset[str] = frozenset({"orders.order.create"}),
    tenant_id: str | None = None,
) -> User:
    return User.register(
        email=Email(email),
        role=role,
        capabilities=Capabilities(caps),
        tenant_id=tenant_id,
    )


def test_register_creates_active_user() -> None:
    user = _make_user()
    assert user.is_active is True
    assert user.email.value == "alice@example.com"
    assert user.role is Role.CUSTOMER
    assert user.capabilities.has("orders.order.create") is True
    assert user.tenant_id is None
    assert user.created_at.tzinfo is not None


def test_register_with_tenant() -> None:
    user = _make_user(tenant_id="tenant-42")
    assert user.tenant_id == "tenant-42"


def test_deactivate() -> None:
    user = _make_user()
    user.deactivate()
    assert user.is_active is False


def test_deactivate_twice_raises() -> None:
    user = _make_user()
    user.deactivate()
    with pytest.raises(UserAlreadyDeactivatedError):
        user.deactivate()


def test_change_role_replaces_capabilities() -> None:
    user = _make_user()
    user.change_role(
        Role.SELLER, Capabilities(frozenset({"catalog.product.write"}))
    )
    assert user.role is Role.SELLER
    assert user.capabilities.has("catalog.product.write") is True
    assert user.capabilities.has("orders.order.create") is False


def test_grant_then_revoke() -> None:
    user = _make_user()
    user.grant("catalog.product.read")
    assert user.capabilities.has("catalog.product.read") is True
    user.revoke("catalog.product.read")
    assert user.capabilities.has("catalog.product.read") is False


def test_no_domain_events_yet() -> None:
    user = _make_user()
    assert user.pull_events() == []


def test_equality_by_user_id() -> None:
    user = _make_user()
    assert user == user
    assert user != _make_user()

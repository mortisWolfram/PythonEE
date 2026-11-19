import pytest

from modules.identity.domain.exceptions import InvalidEmailError
from modules.identity.domain.value_objects import (
    Capabilities,
    Email,
    Role,
    UserId,
)


def test_user_id_generate_unique() -> None:
    assert UserId.generate() != UserId.generate()


def test_user_id_str() -> None:
    uid = UserId.generate()
    assert str(uid) == str(uid.value)


def test_email_lowercases() -> None:
    assert Email("User@Example.COM").value == "user@example.com"


@pytest.mark.parametrize("bad", ["", "no-at-sign", "with space@x", "@", " @"])
def test_email_rejects_invalid(bad: str) -> None:
    with pytest.raises(InvalidEmailError):
        Email(bad)


def test_capabilities_empty_and_has() -> None:
    caps = Capabilities.empty()
    assert caps.has("anything") is False


def test_capabilities_add_remove() -> None:
    caps = Capabilities.empty().with_added("orders.order.create")
    assert caps.has("orders.order.create") is True
    assert caps.with_removed("orders.order.create").has("orders.order.create") is False


def test_capabilities_are_immutable() -> None:
    a = Capabilities(frozenset({"x"}))
    b = a.with_added("y")
    assert a.has("x") is True
    assert a.has("y") is False
    assert b.has("x") is True
    assert b.has("y") is True


def test_role_values() -> None:
    assert Role.CUSTOMER.value == "customer"
    assert Role("seller") is Role.SELLER
    assert Role("courier") is Role.COURIER
    assert Role("admin") is Role.ADMIN
    assert Role("support") is Role.SUPPORT

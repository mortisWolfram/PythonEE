from __future__ import annotations

from shared.domain.exceptions import DomainError


class OrderError(DomainError):
    """Base class for order-domain rule violations."""


class EmptyOrderError(OrderError):
    """Order must contain at least one item."""


class InvalidOrderItemError(OrderError):
    """Item violates quantity/price invariants."""


class CannotShipOrderError(OrderError):
    """Requested ship transition is not allowed from the current state."""


class AccessDeniedError(OrderError):
    """Actor is not a participant of this order."""


class OrderNotFoundError(OrderError):
    """Order aggregate is not present in the store."""

from __future__ import annotations

from shared.domain.exceptions import DomainError


class ProductError(DomainError):
    """Base class for catalog-domain rule violations."""


class InvalidProductError(ProductError):
    """Product fields violate invariants."""


class ProductNotFoundError(ProductError):
    """Product aggregate is not present in the store."""

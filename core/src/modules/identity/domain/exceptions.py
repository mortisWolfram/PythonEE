from __future__ import annotations

from shared.domain.exceptions import DomainError


class IdentityError(DomainError):
    """Base class for identity-domain rule violations."""


class InvalidEmailError(IdentityError):
    """Email does not satisfy format invariants."""


class UserAlreadyDeactivatedError(IdentityError):
    """Attempt to deactivate an already-inactive user."""


class UserNotFoundError(IdentityError):
    """User aggregate is not present in the store."""

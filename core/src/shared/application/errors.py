from __future__ import annotations


class ApplicationError(Exception):
    """Base class for application-level failures (mapped to Kafka DLQ / HTTP errors)."""


class NotFoundError(ApplicationError):
    """Requested aggregate/entity does not exist."""


class ValidationError(ApplicationError):
    """Command/query payload violates application-level invariants."""

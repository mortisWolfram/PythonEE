class DomainError(Exception):
    """Base class for domain rule violations.

    Application layer maps these to ``shared.application.errors``
    and/or to DLQ payloads where appropriate.
    """

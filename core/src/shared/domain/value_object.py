class ValueObject:
    """Marker base for immutable, equality-by-value types.

    Concrete value objects should be ``@dataclass(frozen=True)``.
    """

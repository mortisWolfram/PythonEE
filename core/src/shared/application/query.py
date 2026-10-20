from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Query:
    """Marker base for read-side requests (not consumed from Kafka in this iteration)."""

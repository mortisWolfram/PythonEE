from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Command:
    """Marker base for write-side intents (typically consumed from Kafka)."""

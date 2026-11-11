from __future__ import annotations


def dlq_topic(topic: str) -> str:
    """Per docs/architecture.md §5.5: ``{topic}.dlq.v1``.

    The suffix is appended literally to the *full* topic name, so a
    source topic ``orders.commands.create.v1`` maps to
    ``orders.commands.create.v1.dlq.v1``.  # TODO: revisit naming with arch §5.1
    """
    return f"{topic}.dlq.v1"

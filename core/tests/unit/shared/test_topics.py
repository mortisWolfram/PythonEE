from shared.kafka.topics import dlq_topic


def test_dlq_appends_suffix() -> None:
    assert dlq_topic("orders.commands.create.v1") == "orders.commands.create.v1.dlq.v1"
    assert dlq_topic("orders.events.order-placed.v1") == (
        "orders.events.order-placed.v1.dlq.v1"
    )

class KafkaError(Exception):
    """Base class for Kafka-layer errors."""


class SerializationError(KafkaError):
    """Envelope (de)serialization failed."""

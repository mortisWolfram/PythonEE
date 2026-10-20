import pytest
from pydantic import ValidationError

from shared.config.settings import Settings


def test_loads_required_and_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@h:5432/db")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    s = Settings(_env_file=None)

    assert s.database_url == "postgresql+asyncpg://u:p@h:5432/db"
    assert s.kafka_bootstrap_servers == "localhost:9092"
    assert s.kafka_group_id == "python-ee-core"
    assert s.schema_registry_url is None
    assert s.log_level == "INFO"
    assert s.outbox_poll_interval_ms == 500


def test_overrides_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@h/db")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("OUTBOX_POLL_INTERVAL_MS", "1000")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    s = Settings(_env_file=None)
    assert s.outbox_poll_interval_ms == 1000
    assert s.log_level == "DEBUG"


def test_missing_required_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)

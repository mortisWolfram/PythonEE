from __future__ import annotations

from functools import cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration, loaded from environment variables (and optional .env)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    database_url: str
    kafka_bootstrap_servers: str
    kafka_group_id: str = "python-ee-core"
    schema_registry_url: str | None = None
    log_level: str = "INFO"
    outbox_poll_interval_ms: int = 500


@cache
def get_settings() -> Settings:
    # pydantic-settings fills required fields from the environment; mypy cannot see that
    # because the generated __init__ signature requires them explicitly.
    return Settings()  # type: ignore[call-arg]

"""Configuration skeleton. Values come from environment / .env."""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:29092"
    schema_registry_url: str = "http://localhost:8081"
    database_url: str = "postgresql+asyncpg://core:core@localhost:5432/core"
    redis_url: str = "redis://localhost:6379/0"
    jwt_secret: str = "change-me"
    jwt_algorithm: str = "HS256"


settings = Settings()

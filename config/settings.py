"""Pydantic-based application settings, loaded from environment variables.

The settings object is a process-wide singleton accessed via :func:`get_settings`.
All other modules should depend on the settings object rather than reading
environment variables directly, so configuration is testable and overridable.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Top-level runtime settings for the FraudNet engine."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ---- Runtime ---------------------------------------------------------
    environment: Literal["development", "staging", "production", "test"] = "development"
    log_level: str = "INFO"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    cors_origins: list[str] = Field(
        default_factory=lambda: [
            "http://localhost:5173",
            "http://localhost:8080",
            "http://localhost:3000",
        ]
    )

    # ---- Neo4j -----------------------------------------------------------
    neo4j_uri: str = "bolt://neo4j:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: SecretStr = SecretStr("fraudnet_dev_pw")
    neo4j_database: str = "neo4j"
    neo4j_max_pool_size: int = 50
    neo4j_connection_timeout_s: float = 30.0

    # ---- Kafka -----------------------------------------------------------
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_consumer_group: str = "fraudnet-engine"

    # ---- Redis -----------------------------------------------------------
    redis_url: str = "redis://redis:6379/0"

    # ---- PostgreSQL ------------------------------------------------------
    database_url: str = "postgresql+asyncpg://fraudnet:fraudnet_dev_pw@postgres:5432/fraudnet"
    database_url_sync: str = "postgresql://fraudnet:fraudnet_dev_pw@postgres:5432/fraudnet"
    db_pool_size: int = 10
    db_max_overflow: int = 20

    # ---- MinIO -----------------------------------------------------------
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "fraudnet"
    minio_secret_key: SecretStr = SecretStr("fraudnet_dev_pw")
    minio_bucket: str = "fraudnet-evidence"
    minio_secure: bool = False

    # ---- Auth ------------------------------------------------------------
    jwt_secret: SecretStr = SecretStr("please-change-me-in-prod-this-is-a-dev-only-default")
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 480
    auth_required: bool = False
    """When False (default in dev), routes that would require auth still
    accept anonymous traffic. Login + token issuance always work; this only
    governs whether RBAC dependencies *block* unauthenticated callers."""

    # ---- Celery ----------------------------------------------------------
    celery_broker_url: str = "redis://redis:6379/1"
    celery_result_backend: str = "redis://redis:6379/2"

    # ---- Cache TTLs (seconds) -------------------------------------------
    confidence_cache_ttl_s: int = 300
    dashboard_cache_ttl_s: int = 30
    rules_cache_ttl_s: int = 60

    # ---- CORS / parsing helpers -----------------------------------------
    @field_validator("cors_origins", mode="before")
    @classmethod
    def _split_csv_origins(cls, v: object) -> object:
        if isinstance(v, str):
            return [item.strip() for item in v.split(",") if item.strip()]
        return v


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide :class:`Settings` instance.

    Cached via :func:`functools.lru_cache` so that environment variables are
    only read once. Tests can override settings by clearing the cache and
    constructing a new instance.
    """

    return Settings()

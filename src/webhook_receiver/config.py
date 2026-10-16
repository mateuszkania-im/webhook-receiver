from pathlib import Path
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="WEBHOOK_", env_file=".env", extra="ignore", frozen=True
    )

    host: str = "0.0.0.0"  # noqa: S104
    port: int = Field(8000, ge=1, le=65_535)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    db_path: Path = Path("webhooks.db")
    api_key: SecretStr | None = Field(None, min_length=16)

    workers: int = Field(100, ge=0, le=1_000)
    poll_interval_s: float = Field(1.0, gt=0)
    processing_min_s: float = Field(2.0, ge=0)
    processing_max_s: float = Field(5.0, ge=0)
    processing_timeout_s: float = Field(60.0, gt=0)
    simulation_faults: bool = False
    max_attempts: int = Field(5, ge=1)
    lease_s: float = Field(30.0, gt=0)
    heartbeat_s: float = Field(10.0, gt=0)
    backoff_base_s: float = Field(2.0, gt=0)
    backoff_cap_s: float = Field(300.0, gt=0)

    max_body_bytes: int = Field(65_536, ge=1_024)
    rate_limit_per_s: float = Field(50.0, gt=0)
    rate_limit_burst: int = Field(100, ge=1)
    backlog_limit: int = Field(5_000, ge=1)

    retention_s: int = Field(7 * 86_400, ge=1)
    retention_interval_s: float = Field(60.0, gt=0)
    readiness_stale_s: float = Field(10.0, gt=0)

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if self.processing_min_s > self.processing_max_s:
            raise ValueError("processing_min_s must not exceed processing_max_s")
        if self.heartbeat_s >= self.lease_s:
            raise ValueError("heartbeat_s must be shorter than lease_s")
        if self.poll_interval_s >= self.readiness_stale_s:
            raise ValueError("poll_interval_s must be shorter than readiness_stale_s")
        if self.processing_max_s >= self.processing_timeout_s:
            raise ValueError("processing_max_s must be shorter than processing_timeout_s")
        return self

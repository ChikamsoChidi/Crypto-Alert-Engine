from enum import StrEnum
from functools import lru_cache
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class AppEnvironment (StrEnum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"

class Settings (BaseSettings):
    """
    Central configuration object for the entire application.

    Pydantic-Settings reads values from environmentn variables and .env files,
    validates their types, and exposes them as a typed, immutable object. All
    downstream code receives a Settings instance -- it never reads os.environ directly.
    This makes configuration explicit and testable.
    """

    model_config = SettingsConfigDict(
    env_file = ".env",
    env_file_encoding = "utf-8",
    case_sensitive = False,
    frozen = True, #Settings are immutable after construction
    )

    # Application
    app_env: AppEnvironment = AppEnvironment.DEVELOPMENT
    log_level: str = Field(default = "INFO",pattern= r"^(DEBUG|INFO|WARNIGN|ERROR|CRITICAL)$")

    # Feed 
    binance_ws_base_url: str = "wss://stream.binance.com:9443"
    feed_reconnect_delay_seconds: float = Field(default=5.0, ge=1.0, le=60.0)
    feed_max_reconnect_attempts: int = Field(default=10, ge=1, le=100)

    # Pipeline 
    pipeline_queue_max_size: int = Field(default=1000, ge=10, le = 100_000)

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_log_level(cls, v:object) -> str:
        if isinstance(v, str):
            return v.upper()
        raise ValueError(f"log_level must be a string, got {type(v)}")
    
@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Returns a cached singleton Settings instance.

    Using lru_cache means the .env file is read and validated exactly 
    once per process lifetime. All callers share the same object with 
    zero overhead on subsequent calls. The cache can be cleared in tests
    via get_settings.cache_clear()
    """
    return Settings()
    

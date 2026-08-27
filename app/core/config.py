from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.config.database import DatabaseSettings


BASE_DIR = Path(__file__).resolve().parents[2]
ENV_CONFIG = SettingsConfigDict(
    env_file=str(BASE_DIR / ".env"),
    env_file_encoding="utf-8",
    extra="ignore",
    populate_by_name=True,
)


class CORSSettings(BaseSettings):
    allowed_origins_raw: str = Field(
        default="*",
        validation_alias=AliasChoices("CORS_ALLOWED_ORIGINS", "allowed_origins_raw"),
    )
    allowed_methods_raw: str = Field(
        default="*",
        validation_alias=AliasChoices("CORS_ALLOWED_METHODS", "allowed_methods_raw"),
    )
    allowed_headers_raw: str = Field(
        default="*",
        validation_alias=AliasChoices("CORS_ALLOWED_HEADERS", "allowed_headers_raw"),
    )
    allow_credentials: bool = Field(
        default=False,
        validation_alias=AliasChoices("CORS_ALLOW_CREDENTIALS", "allow_credentials"),
    )

    model_config = ENV_CONFIG

    @staticmethod
    def _list(value: str) -> List[str]:
        return [item.strip() for item in value.split(",") if item.strip()]

    @property
    def allowed_origins(self) -> List[str]:
        return self._list(self.allowed_origins_raw)

    @property
    def allowed_methods(self) -> List[str]:
        return self._list(self.allowed_methods_raw)

    @property
    def allowed_headers(self) -> List[str]:
        return self._list(self.allowed_headers_raw)


class SecuritySettings(BaseSettings):
    usermanagement_api_url: str = Field(
        default="http://localhost:8070",
        validation_alias=AliasChoices(
            "USERMANAGEMENT_API_URL", "usermanagement_api_url"
        ),
    )

    model_config = ENV_CONFIG


class CelerySettings(BaseSettings):
    broker_url: str = Field(
        default="amqp://guest:guest@localhost:5672/",
        validation_alias=AliasChoices("CELERY_BROKER_URL", "broker_url"),
    )
    result_backend: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("CELERY_RESULT_BACKEND", "result_backend"),
    )

    model_config = ENV_CONFIG


class LoggingSettings(BaseSettings):
    level: str = Field(
        default="INFO", validation_alias=AliasChoices("LOG_LEVEL", "level")
    )
    json_format: bool = Field(
        default=False,
        validation_alias=AliasChoices("LOG_JSON_FORMAT", "json_format"),
    )
    format: str = Field(
        default="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        validation_alias=AliasChoices("LOG_FORMAT", "format"),
    )
    file_path: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("LOG_FILE", "file_path")
    )
    max_bytes: int = Field(
        default=10 * 1024 * 1024,
        validation_alias=AliasChoices("LOG_MAX_BYTES", "max_bytes"),
    )
    backup_count: int = Field(
        default=5,
        validation_alias=AliasChoices("LOG_BACKUP_COUNT", "backup_count"),
    )

    model_config = ENV_CONFIG


class Settings(BaseSettings):
    APP_NAME: str = Field(
        default="ETL API", validation_alias=AliasChoices("APP_NAME", "app_name")
    )
    VERSION: str = Field(
        default="1.0.0", validation_alias=AliasChoices("VERSION", "version")
    )
    DEBUG: bool = Field(
        default=True, validation_alias=AliasChoices("DEBUG", "debug")
    )
    ENVIRONMENT: str = Field(
        default="development",
        validation_alias=AliasChoices("ENVIRONMENT", "environment"),
    )
    HOST: str = Field(
        default="localhost", validation_alias=AliasChoices("HOST", "host")
    )
    PORT: int = Field(
        default=8000, validation_alias=AliasChoices("PORT", "port")
    )

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    cors_settings: CORSSettings = Field(default_factory=CORSSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    celery_settings: CelerySettings = Field(default_factory=CelerySettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    model_config = ENV_CONFIG


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

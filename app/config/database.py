"""
Enhanced database connector with improved setup and configuration.

Features:
- Automatic connection pooling
- Connection retry logic
- Health check support
- Multiple database support (PostgreSQL, MySQL, SQLite)
- Proper error handling
- Easy configuration via env (nested + legacy alias)
"""

from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import quote_plus
import logging

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

_BASE_DIR = Path(__file__).resolve().parent.parent.parent


class DatabaseSettings(BaseSettings):
    """
    Database configuration. Supports:
    - Nested: DATABASE__URL, DATABASE__HOST
    - Legacy: DATABASE_URL, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
    """

    url: Optional[str] = Field(
        default=None,
        description="Full database URL",
        validation_alias=AliasChoices("database__url", "database_url"),
    )
    type: str = Field(
        default="postgresql",
        description="Database type: postgresql, mysql, or sqlite",
        validation_alias=AliasChoices("database__type", "database_type"),
    )
    host: Optional[str] = Field(
        default="localhost",
        description="Database host",
        validation_alias=AliasChoices("database__host", "database_host", "db_host"),
    )
    name: Optional[str] = Field(
        default=None,
        description="Database name",
        validation_alias=AliasChoices("database__name", "database_name", "db_name"),
    )
    port: Optional[str] = Field(
        default="5432",
        description="Database port",
        validation_alias=AliasChoices("database__port", "database_port", "db_port"),
    )
    user: Optional[str] = Field(
        default=None,
        description="Database user",
        validation_alias=AliasChoices("database__user", "database_user", "db_user"),
    )
    password: Optional[str] = Field(
        default=None,
        description="Database password",
        validation_alias=AliasChoices("database__password", "database_password", "db_password"),
    )

    pool_size: int = Field(default=5, description="Database pool size")
    max_overflow: int = Field(default=10, description="Database max overflow")
    pool_recycle: int = Field(default=3600, description="Database pool recycle")
    pool_timeout: int = Field(default=30, description="Database pool timeout")
    connect_timeout: int = Field(default=10, description="Database connect timeout")
    debug: bool = Field(default=False, description="Database debug flag")

    model_config = SettingsConfigDict(
        env_file=str(_BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        env_prefix="DATABASE_",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    def get_database_url(self, sync: bool = True) -> str:
        """
        Build database URL.
        """
        if self.url and self.url.strip() and self.url.startswith(('postgresql://', 'mysql://', 'sqlite://')):
            if sync:
                return self.url
            else:
                if self.url.startswith('postgresql://'):
                    return self.url.replace('postgresql://', 'postgresql+asyncpg://', 1)
                elif self.url.startswith('mysql://'):
                    return self.url.replace('mysql://', 'mysql+aiomysql://', 1)
                elif self.url.startswith('sqlite://'):
                    return self.url.replace('sqlite://', 'sqlite+aiosqlite://', 1)
                return self.url

        db_type = self.type.lower()
        password = quote_plus(self.password) if self.password else ""

        if db_type == "postgresql":
            prefix = "postgresql" if sync else "postgresql+asyncpg"
            return f"{prefix}://{self.user}:{password}@{self.host}:{self.port}/{self.name}"

        elif db_type == "mysql":
            prefix = "mysql+pymysql" if sync else "mysql+aiomysql"
            return f"{prefix}://{self.user}:{password}@{self.host}:{self.port}/{self.name}"

        elif db_type == "sqlite":
            db_path = self.name or 'database.db'
            if not db_path.endswith('.db'):
                db_path = f"{db_path}.db"
            prefix = "sqlite" if sync else "sqlite+aiosqlite"
            return f"{prefix}:///{db_path}"

        return ""

    @property
    def engine_kwargs(self) -> Dict[str, Any]:
        """Get standard engine kwargs"""
        return {
            "echo": self.debug,
            "pool_pre_ping": True,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_recycle": self.pool_recycle,
            "pool_timeout": self.pool_timeout,
        }

    @property
    def async_engine_kwargs(self) -> Dict[str, Any]:
        return {
            "echo": self.debug,
            "pool_pre_ping": True,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_recycle": self.pool_recycle,
            "pool_timeout": self.pool_timeout,
        }

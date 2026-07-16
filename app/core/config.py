from typing import List, Optional
from pydantic import AliasChoices, EmailStr, Field
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

from app.config.database import DatabaseSettings


BASE_DIR = Path(__file__).resolve().parent.parent.parent


class CORSSettings(BaseSettings):
    """CORS configuration settings.

    Reads from .env file. List fields accept a comma-separated string
    (e.g. "http://localhost,http://localhost:3000").

    Raw env vars are stored as strings to avoid pydantic-settings
    attempting JSON decode on CSV values before validators run.
    Access the parsed lists via the @property helpers.
    """

    # Raw string fields — pydantic-settings reads these directly from env/file
    CORS_ALLOWED_ORIGINS: str = Field(default="*")
    CORS_ALLOWED_METHODS: str = Field(default="*")
    CORS_ALLOWED_HEADERS: str = Field(default="*")
    CORS_ALLOW_CREDENTIALS: bool = Field(default=True)

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        extra="allow",
        env_file_encoding="utf-8",
    )

    @property
    def allowed_origins(self) -> List[str]:
        """Parsed list of allowed origins."""
        return [x.strip() for x in self.CORS_ALLOWED_ORIGINS.split(",") if x.strip()]

    @property
    def allowed_methods(self) -> List[str]:
        """Parsed list of allowed HTTP methods."""
        return [x.strip() for x in self.CORS_ALLOWED_METHODS.split(",") if x.strip()]

    @property
    def allowed_headers(self) -> List[str]:
        """Parsed list of allowed request headers."""
        return [x.strip() for x in self.CORS_ALLOWED_HEADERS.split(",") if x.strip()]

    @property
    def allow_credentials(self) -> bool:
        """Whether credentials (cookies, auth headers) are allowed."""
        return self.CORS_ALLOW_CREDENTIALS


class RedisSettings(BaseSettings):
    """Redis configuration settings."""

    # validation_alias used for port/host so bare PORT/HOST env vars cannot collide
    host: str = Field(
        default="localhost",
        validation_alias=AliasChoices("REDIS_HOST", "redis_host"),
    )
    port: int = Field(
        default=6379,
        validation_alias=AliasChoices("REDIS_PORT", "redis_port"),
    )
    password: Optional[str] = Field(default=None, env="REDIS_PASSWORD")
    db: int = Field(default=0, env="REDIS_DB")
    url: Optional[str] = Field(default=None, env="REDIS_URL")
    max_connections: int = Field(default=10, env="REDIS_MAX_CONNECTIONS")

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        extra="ignore",
        env_file_encoding="utf-8",
        populate_by_name=True,
    )

    @property
    def redis_url(self) -> str:
        """Computed Redis URL from individual fields, or the explicit REDIS_URL override."""
        if self.url:
            return self.url
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class RabbitMqSettings(BaseSettings):
    """RabbitMQ configuration settings."""

    # validation_alias used for port/host so bare PORT/HOST env vars cannot collide
    host: str = Field(
        default="localhost",
        validation_alias=AliasChoices("RABBITMQ_HOST", "rabbitmq_host"),
    )
    port: int = Field(
        default=5672,
        validation_alias=AliasChoices("RABBITMQ_PORT", "rabbitmq_port"),
    )
    password: Optional[str] = Field(default=None, env="RABBITMQ_PASSWORD")
    vhost: str = Field(default="/", env="RABBITMQ_VHOST")
    user: str = Field(default="guest", env="RABBITMQ_USER")
    url: Optional[str] = Field(default=None, env="RABBITMQ_URL")

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        extra="ignore",
        env_file_encoding="utf-8",
        populate_by_name=True,
    )


class CelerySettings(BaseSettings):
    """Celery configuration settings.

    List fields (CELERY_ACCEPT_CONTENT) accept a comma-separated string from env
    (e.g. "json,application/json").  Access the parsed list via the @property helper.
    """

    broker_url: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("CELERY_BROKER_URL", "celery_broker_url"),
    )
    result_backend: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("CELERY_RESULT_BACKEND", "celery_result_backend"),
    )
    task_serializer: str = Field(
        default="json",
        validation_alias=AliasChoices("CELERY_TASK_SERIALIZER", "celery_task_serializer"),
    )
    # Raw string — avoids pydantic-settings JSON-decode attempt on CSV env values
    CELERY_ACCEPT_CONTENT: str = Field(
        default="json",
        validation_alias=AliasChoices("CELERY_ACCEPT_CONTENT", "celery_accept_content"),
    )
    result_serializer: str = Field(
        default="json",
        validation_alias=AliasChoices("CELERY_RESULT_SERIALIZER", "celery_result_serializer"),
    )
    timezone: str = Field(
        default="UTC",
        validation_alias=AliasChoices("CELERY_TIMEZONE", "celery_timezone"),
    )
    enable_utc: bool = Field(
        default=True,
        validation_alias=AliasChoices("CELERY_ENABLE_UTC", "celery_enable_utc"),
    )
    SEND_TASK_FAILURE_NOTIFICATIONS: bool = Field(
        default=True,
        validation_alias=AliasChoices("SEND_TASK_FAILURE_NOTIFICATIONS", "send_task_failure_notifications"),
    )

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        extra="ignore",
        env_file_encoding="utf-8",
        populate_by_name=True,
    )

    @property
    def accept_content(self) -> List[str]:
        """Parsed list of accepted Celery content types."""
        return [x.strip() for x in self.CELERY_ACCEPT_CONTENT.split(",") if x.strip()]


class EmailSettings(BaseSettings):
    """Email configuration settings."""

    smtp_host: str = Field(..., env="EMAIL_SMTP_HOST")
    smtp_port: int = Field(default=587, env="EMAIL_SMTP_PORT")
    smtp_username: str = Field(..., env="EMAIL_SMTP_USERNAME")
    smtp_password: str = Field(..., env="EMAIL_SMTP_PASSWORD")
    use_tls: bool = Field(default=True, env="EMAIL_USE_TLS")
    use_ssl: bool = Field(default=False, env="EMAIL_USE_SSL")
    from_email: EmailStr = Field(..., env="EMAIL_FROM_EMAIL")
    from_name: str = Field(default="FastAPI App", env="EMAIL_FROM_NAME")

    # AWS SES settings (optional)
    aws_access_key_id: Optional[str] = Field(default=None, env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: Optional[str] = Field(default=None, env="AWS_SECRET_ACCESS_KEY")
    aws_region: Optional[str] = Field(default="us-east-1", env="AWS_REGION")

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        extra="ignore",
        env_file_encoding="utf-8",
    )


class StorageSettings(BaseSettings):
    """File storage configuration settings.

    List fields (ALLOWED_FILE_EXTENSIONS, THUMBNAIL_SIZES) accept comma-separated
    strings from env (e.g. ".csv,.json").  Access the parsed lists via the
    @property helpers.
    """

    default_storage: str = Field(default="local", env="DEFAULT_STORAGE")  # local, s3
    max_file_size: int = Field(default=50 * 1024 * 1024, env="MAX_FILE_SIZE")  # 50 MB
    local_storage_path: str = Field(default="./uploads", env="LOCAL_STORAGE_PATH")
    # Raw string — None means "allow all"; set ALLOWED_FILE_EXTENSIONS=.csv,.json to restrict
    ALLOWED_FILE_EXTENSIONS: Optional[str] = Field(default=None, env="ALLOWED_FILE_EXTENSIONS")
    preserve_filename: bool = Field(default=True, env="PRESERVE_FILENAME")

    # AWS S3 settings
    aws_s3_bucket: Optional[str] = Field(default=None, env="AWS_S3_BUCKET")
    aws_s3_region: Optional[str] = Field(default="us-east-1", env="AWS_S3_REGION")
    aws_s3_access_key_id: Optional[str] = Field(default=None, env="AWS_S3_ACCESS_KEY_ID")
    aws_s3_secret_access_key: Optional[str] = Field(default=None, env="AWS_S3_SECRET_ACCESS_KEY")

    # MinIO settings (uses S3-compatible API)
    minio_endpoint: Optional[str] = Field(default=None, env="MINIO_ENDPOINT")
    minio_access_key: Optional[str] = Field(default=None, env="MINIO_ACCESS_KEY")
    minio_secret_key: Optional[str] = Field(default=None, env="MINIO_SECRET_KEY")
    minio_bucket: Optional[str] = Field(default=None, env="MINIO_BUCKET")
    minio_region: str = Field(default="us-east-1", env="MINIO_REGION")
    minio_secure: bool = Field(default=True, env="MINIO_SECURE")

    # File validation settings
    validate_file_content: bool = Field(default=True, env="VALIDATE_FILE_CONTENT")
    scan_for_malware: bool = Field(default=False, env="SCAN_FOR_MALWARE")  # Future feature

    # Temporary file settings
    temp_file_expiry_hours: int = Field(default=24, env="TEMP_FILE_EXPIRY_HOURS")
    temp_cleanup_interval_hours: int = Field(default=6, env="TEMP_CLEANUP_INTERVAL_HOURS")

    # Image processing settings (future feature)
    auto_generate_thumbnails: bool = Field(default=False, env="AUTO_GENERATE_THUMBNAILS")
    # Raw string — CSV of ints; e.g. "150,300,600"
    THUMBNAIL_SIZES: str = Field(default="150,300,600", env="THUMBNAIL_SIZES")
    image_quality: int = Field(default=85, env="IMAGE_QUALITY")

    # Chunked upload settings
    chunk_size: int = Field(default=5 * 1024 * 1024, env="UPLOAD_CHUNK_SIZE")  # 5 MB per chunk
    chunk_upload_threshold: int = Field(default=10 * 1024 * 1024, env="UPLOAD_THRESHOLD")  # 10 MB
    upload_session_expire_hours: int = Field(default=24, env="UPLOAD_SESSION_EXPIRE_HOURS")

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        extra="ignore",
        env_file_encoding="utf-8",
    )

    @property
    def allowed_file_extensions(self) -> Optional[List[str]]:
        """Parsed list of allowed extensions, or None to allow all."""
        if self.ALLOWED_FILE_EXTENSIONS is None:
            return None
        return [x.strip() for x in self.ALLOWED_FILE_EXTENSIONS.split(",") if x.strip()]

    @property
    def thumbnail_sizes(self) -> List[int]:
        """Parsed list of thumbnail pixel widths."""
        return [int(x.strip()) for x in self.THUMBNAIL_SIZES.split(",") if x.strip()]


class SecuritySettings(BaseSettings):
    """Security configuration settings."""

    secret_key: str = Field(..., env="SECRET_KEY")
    algorithm: str = Field(default="HS256", env="JWT_ALGORITHM")
    access_token_expire: int = Field(default=30, env="ACCESS_TOKEN_EXPIRE")  # minutes
    refresh_token_expire: int = Field(default=7, env="REFRESH_TOKEN_EXPIRE")  # days
    usermanagement_api_url: str = Field(default="http://localhost:8000", env="USERMANAGEMENT_API_URL")
    # Comma-separated list of role NAMES that are allowed to perform
    # admin/privileged actions. Roles are managed dynamically in
    # fastapi_usermanagement, so this MUST stay configurable (env) rather than
    # hardcoded in code.
    admin_roles: str = Field(default="admin,superuser", env="ADMIN_ROLES")

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        extra="ignore",
        env_file_encoding="utf-8",
    )


class LoggingSettings(BaseSettings):
    """Logging configuration settings."""

    level: str = Field(default="INFO", env="LOG_LEVEL")
    json_format: bool = Field(default=False, env="LOG_JSON_FORMAT")
    format: str = Field(default="%(asctime)s - %(levelname)s - %(name)s - %(message)s", env="LOG_FORMAT")
    file_path: Optional[str] = Field(default=None, env="LOG_FILE")
    max_bytes: int = Field(default=10 * 1024 * 1024, env="LOG_MAX_BYTES")
    backup_count: int = Field(default=5, env="LOG_BACKUP_COUNT")

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        extra="ignore",
        env_file_encoding="utf-8",
    )


class Settings(BaseSettings):
    APP_NAME: str = Field(default="FastAPI ETL", env="APP_NAME")
    VERSION: str = Field(default="1.0.0", env="VERSION")
    DEBUG: bool = Field(default=True, env="DEBUG")
    ENVIRONMENT: str = Field(default="development", env="ENVIRONMENT")
    HOST: str = Field(default="localhost", env="HOST")
    PORT: int = Field(default=8000, env="PORT")

    redis_url: Optional[str] = Field(env="REDIS_URL", default=None)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)

    rabbitmq_settings: Optional[RabbitMqSettings] = Field(default_factory=RabbitMqSettings)
    # email_settings: Optional[EmailSettings] = EmailSettings()
    cors_settings: Optional[CORSSettings] = Field(default_factory=CORSSettings)
    redis_settings: Optional[RedisSettings] = Field(default_factory=RedisSettings)
    storage_settings: Optional[StorageSettings] = Field(default_factory=StorageSettings)
    security: Optional[SecuritySettings] = Field(default_factory=SecuritySettings)
    celery_settings: Optional[CelerySettings] = Field(default_factory=CelerySettings)
    logging: Optional[LoggingSettings] = Field(default_factory=LoggingSettings)

    rate_limit_requests: int = Field(default=100, env="RATE_LIMIT_REQUESTS")
    rate_limit_window: int = Field(default=60, env="RATE_LIMIT_WINDOW")  # seconds

    # Pagination
    default_page_size: int = Field(default=10, env="DEFAULT_PAGE_SIZE")
    max_page_size: int = Field(default=100, env="MAX_PAGE_SIZE")

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        extra="allow",
        env_file_encoding="utf-8",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get cached settings instance. Loads from .env on first call."""
    return Settings()


# Module-level singleton — allows `from app.core.config import settings`
settings = get_settings()

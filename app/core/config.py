import os
from typing import List, Optional
from pydantic import EmailStr, Field, field_validator
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class CORSSettings(BaseSettings):
    """CORS configuration settings."""
    
    allowed_origins: List[str] = Field(default=["*"], env="CORS_ALLOWED_ORIGINS")
    allowed_methods: List[str] = Field(default=["*"], env="CORS_ALLOWED_METHODS")
    allowed_headers: List[str] = Field(default=["*"], env="CORS_ALLOWED_HEADERS")
    allow_credentials: bool = Field(default=True, env="CORS_ALLOW_CREDENTIALS")


class RedisSettings(BaseSettings):
    """Redis configuration settings."""
    
    host: str = Field(default="localhost", env="REDIS_HOST")
    port: int = Field(default=6379, env="REDIS_PORT")
    password: Optional[str] = Field(default=None, env="REDIS_PASSWORD")
    db: int = Field(default=0, env="REDIS_DB")
    url: Optional[str] = Field(default=None, env="REDIS_URL")
    max_connections: int = Field(default=10, env="REDIS_MAX_CONNECTIONS")
    
    # @field_validator("url", always=True)
    def build_redis_url(cls, v, values):
        if v:
            return v
        
        host = values.get("host", "localhost")
        port = values.get("port", 6379)
        password = values.get("password")
        db = values.get("db", 0)
        
        if password:
            return f"redis://:{password}@{host}:{port}/{db}"
        return f"redis://{host}:{port}/{db}"


class RabbitMqSettings(BaseSettings):
    """RabbitMQ configuration settings."""
    
    host: str = Field(default="localhost", env="RABBITMQ_HOST")
    port: int = Field(default=6379, env="RABBITMQ_PORT")
    password: Optional[str] = Field(default=None, env="RABBITMQ_PASSWORD")
    vhost: str = Field(default="/", env="RABBITMQ_VHOST")
    user: str = Field(default="guest", env="RABBITMQ_USER")
    url: Optional[str] = Field(default=None, env="RABBITMQ_URL")


class CelerySettings(BaseSettings):
    """Celery configuration settings."""
    
    broker_url: Optional[str] = Field(default=None, env="CELERY_BROKER_URL")
    result_backend: Optional[str] = Field(default=None, env="CELERY_RESULT_BACKEND")
    task_serializer: str = Field(default="json", env="CELERY_TASK_SERIALIZER")
    accept_content: List[str] = Field(default=["json"], env="CELERY_ACCEPT_CONTENT")
    result_serializer: str = Field(default="json", env="CELERY_RESULT_SERIALIZER")
    timezone: str = Field(default="UTC", env="CELERY_TIMEZONE")
    enable_utc: bool = Field(default=True, env="CELERY_ENABLE_UTC")


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


class StorageSettings(BaseSettings):
    """File storage configuration settings."""
    
    default_storage: str = Field(default="local", env="DEFAULT_STORAGE")  # local, s3
    max_file_size: int = 50 * 1024 * 1024  # 50MB in bytes
    local_storage_path: str = Field(default="./uploads", env="LOCAL_STORAGE_PATH")
    allowed_file_extensions: Optional[List[str]] = None
    preserve_filename: bool = True
    
    # AWS S3 settings
    aws_s3_bucket: Optional[str] = Field(default=None, env="AWS_S3_BUCKET")
    aws_s3_region: Optional[str] = Field(default="us-east-1", env="AWS_S3_REGION")
    aws_s3_access_key_id: Optional[str] = Field(default=None, env="AWS_S3_ACCESS_KEY_ID")
    aws_s3_secret_access_key: Optional[str] = Field(default=None, env="AWS_S3_SECRET_ACCESS_KEY")

    # MinIO settings (uses S3-compatible API)
    minio_endpoint: Optional[str] = None
    minio_access_key: Optional[str] = None
    minio_secret_key: Optional[str] = None
    minio_bucket: Optional[str] = None
    minio_region: str = "us-east-1"
    minio_secure: bool = True

    # File validation settings
    validate_file_content: bool = True
    scan_for_malware: bool = False  # Future feature
    
    # Temporary file settings
    temp_file_expiry_hours: int = 24
    temp_cleanup_interval_hours: int = 6
    
    # Image processing settings (future feature)
    auto_generate_thumbnails: bool = False
    thumbnail_sizes: List[int] = [150, 300, 600]
    image_quality: int = 85

class SecuritySettings(BaseSettings):
    """Security configuration settings."""
    
    secret_key: str = Field(..., env="SECRET_KEY")
    algorithm: str = Field(default="HS256", env="JWT_ALGORITHM")
    access_token_expire: int = Field(default=30, env="ACCESS_TOKEN_EXPIRE") # minutes
    refresh_token_expire: int = Field(default=7, env="REFRESH_TOKEN_EXPIRE") # days


class LoggingSettings(BaseSettings):
    level: str = Field(default="INFO", env="LOG_LEVEL")
    json_format: bool = Field(default=False, env="LOG_JSON_FORMAT")
    format: str = Field(default="%(asctime)s - %(levelname)s - %(name)s - %(message)s", env="LOG_FORMAT")
    file_path: Optional[str] = Field(default=None, env="LOG_FILE")
    max_bytes: int = Field(default=10 * 1024 * 1024, env="LOG_MAX_BYTES")
    backup_count: int = Field(default=5, env="LOG_BACKUP_COUNT")


class Settings(BaseSettings):
    redis_url: Optional[str] = Field(env="REDIS_URL", default=None)
    database_url: str = Field(default="postgresql://user:password@db:5432/etl_db", env="DATABASE_URL")
    
    rabbitmq_settings: Optional[RabbitMqSettings] = Field(default_factory=RabbitMqSettings)
    # email_settings: Optional[EmailSettings] = EmailSettings()
    cors_settings: Optional[CORSSettings] = Field(default_factory=CORSSettings)
    redis_settings: Optional[RedisSettings] = Field(default_factory=RedisSettings)
    # storage_settings: Optional[StorageSettings] = StorageSettings()
    security: Optional[SecuritySettings] = Field(default_factory=SecuritySettings)
    celery_settings: Optional[CelerySettings] = Field(default_factory=CelerySettings)
    logging: Optional[LoggingSettings] = Field(default_factory=LoggingSettings)

    rate_limit_requests: int = Field(default=100, env="RATE_LIMIT_REQUESTS")
    rate_limit_window: int = Field(default=60, env="RATE_LIMIT_WINDOW")  # seconds

    # Pagination
    default_page_size: int = Field(default=10, env="DEFAULT_PAGE_SIZE")
    max_page_size: int = Field(default=100, env="MAX_PAGE_SIZE")

    model_config = SettingsConfigDict(env_file=".env", extra="allow", env_file_encoding="utf-8")
    

    # class Config:
    #     extra = "allow"
    #     env_file = ".env"
    #     env_file_encoding = "utf-8"

settings = Settings()

@lru_cache
def get_settings() -> Settings:
    return Settings()

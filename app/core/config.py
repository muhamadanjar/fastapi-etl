import os
from pydantic import BaseSettings, EmailStr
from functools import lru_cache


class Settings(BaseSettings):
    broker_url = os.getenv("RABBITMQ_URL", "pyamqp://guest@rabbitmq//")
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    database_url = os.getenv("DATABASE_URL", "postgresql://user:password@db:5432/etl_db")

    smtp_host: str = os.getenv("SMTP_HOST", "smtp.mailtrap.io")
    smtp_port: int = int(os.getenv("SMTP_PORT", 587))
    smtp_user: str = os.getenv("SMTP_USER", "your_smtp_user")
    smtp_password: str = os.getenv("SMTP_PASSWORD", "your_smtp_password")
    smtp_from_email: EmailStr = os.getenv("SMTP_FROM_EMAIL", "noreply@example.com")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()

@lru_cache
def get_settings() -> Settings:
    return Settings()

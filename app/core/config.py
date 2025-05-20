import os

class Settings:
    rabbitmq_broker_url = os.getenv("RABBITMQ_URL", "pyamqp://guest@rabbitmq//")
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    database_url = os.getenv("DATABASE_URL", "postgresql://user:password@db:5432/etl_db")

settings = Settings()
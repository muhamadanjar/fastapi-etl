FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Create a working directory for the app
WORKDIR /opt/app

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    libffi-dev \
    zlib1g-dev \
    libjpeg-dev \
    libmagic-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*


# Build from monorepo root: docker build -f services/etl_api/docker/Dockerfile .
COPY libs/service_auth /opt/service_auth
COPY services/etl_api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY services/etl_api/app ./app
COPY services/etl_api/.env.example ./.env.example
COPY services/etl_api/migrations ./migrations
COPY services/etl_api/alembic.ini ./alembic.ini

# Expose the port FastAPI will run on
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Create a working directory for the app
WORKDIR /usermanagement_app

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    libffi-dev \
    zlib1g-dev \
    libjpeg-dev \
    libmagic-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*


# Copy the requirements file and install dependencies
COPY ./app ./app
COPY ./requirements.txt .
COPY ./.env.example .
COPY ./migrations ./migrations
COPY ./alembic.ini .
# COPY . .
RUN pip install --no-cache-dir -r requirements.txt



# Expose the port FastAPI will run on
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]


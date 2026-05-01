# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FastAPI ETL system with clean architecture. Implements a scalable ETL pipeline with:
- REST API (FastAPI/Uvicorn)
- Background job processing (Celery + RabbitMQ)
- Caching layer (Redis with memory fallback)
- Database migrations (Alembic)
- Message publishing/consuming (RabbitMQ, Redis)
- WebSocket support

Database: PostgreSQL with multiple schemas (raw_data, staging, transformation, processed, config, etl_control, audit).

## Architecture

Clean architecture with strict layer separation:

### Interfaces Layer (`app/interfaces/`)
- **HTTP** (`http/`): REST API routes, controllers, serializers
- **WebSocket** (`websocket/`): Real-time connections and handlers
- **CLI** (`cli/`): Command-line tools via `manage.py`
- **Consumers** (`consumer/`): Message consumers (RabbitMQ, Kafka, SQS)
- **Middleware** (`middleware.py`): CORS, logging, trusted hosts

### Domain Layer (`app/domain/`)
Business logic and entities:
- **entities/**: Domain models (User, Job, etc.)
- **value_objects/**: Immutable value objects
- **services/**: Domain services (pure business logic)
- **repositories/**: Repository interfaces (not implementations)
- **exceptions/**: Domain-specific exceptions

### Application Layer (`app/application/`)
Use cases and application services orchestrating domain logic.

### Infrastructure Layer (`app/infrastructure/`)
Technical implementations:
- **database/**: PostgreSQL connection, repository implementations (SQLAlchemy/SQLModel), ORM models
- **cache/**: Redis + memory fallback
- **messaging/**: RabbitMQ/Kafka/SQS publishers and consumers
- **email/**: SMTP and SES implementations
- **storage/**: Local and S3 file storage
- **monitoring/**: Metrics and tracing
- **tasks/**: Celery app, periodic task scheduler, background job definitions

### Supporting Layers
- **core/** (`config.py`, `exceptions.py`, `security.py`, `logging.py`): Global configuration and utilities
- **schemas/**: Pydantic models for request/response validation
- **processors/** & **transformers/**: Data processing logic for ETL
- **handlers/**: Event handlers and business logic processors
- **utils/**: Helper utilities

## Key Commands

### Run Application
```bash
# Development with reload
python -m uvicorn app.main:app --reload

# Production
python -m app.main

# Via manage.py wrapper
python manage.py help
```

### Database
```bash
# Create migration
alembic revision --autogenerate -m "description"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1

# View migration status
alembic current
```

### CLI Commands (via `python manage.py`)
```bash
# Database migration
python manage.py migrate [upgrade|downgrade|current]

# Seed data
python manage.py seed [--seed-type TYPE]

# Celery worker (single and multi-queue)
python manage.py worker [--queues QUEUE1,QUEUE2]

# Interactive shell
python manage.py shell
```

### Celery Background Tasks
```bash
# Basic worker (default queue)
celery -A app.tasks.celery_app worker --loglevel=info

# Multi-queue worker
celery -A app.tasks.celery_app worker -Q etl,monitoring,cleanup --loglevel=info

# Worker with concurrency
celery -A app.tasks.celery_app worker --concurrency=4 --loglevel=info

# Beat scheduler (periodic tasks)
celery -A app.tasks.celery_app beat --loglevel=info

# Worker + Beat combined
celery -A app.tasks.celery_app worker --beat --loglevel=info

# Flower monitoring UI (http://localhost:5555)
celery -A app.tasks.celery_app flower
```

### Docker
```bash
# Full stack (API + Celery + Redis + RabbitMQ)
docker-compose up

# Specific services
docker-compose up api redis rabbitmq
```

## Development Setup

### Environment
- Python 3.8+ (check `.python-version`)
- PostgreSQL 12+
- Redis 7+
- RabbitMQ 4+

### Local Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Copy environment
cp .env.example .env

# Create database and run migrations
alembic upgrade head

# Optional: Seed initial data
python manage.py seed

# Start dev server (port 8000)
python -m uvicorn app.main:app --reload

# In separate terminals:
# Celery worker (listens for background tasks)
celery -A app.tasks.celery_app worker --loglevel=info

# Beat scheduler (triggers periodic tasks)
celery -A app.tasks.celery_app beat --loglevel=info
```

### Or Use Docker Compose
```bash
# All services
docker-compose up

# API: http://localhost:8007
# RabbitMQ Admin: http://localhost:15672 (guest/guest)
# Flower (Celery): http://localhost:5555
```

## Important Files

- `app/main.py`: FastAPI app initialization, lifespan setup, middleware config
- `app/core/config.py`: Settings management (database, Redis, RabbitMQ, CORS)
- `app/core/exceptions.py`: Global exception hierarchy
- `alembic.ini`: Database migration config
- `requirements.txt`: Dependencies
- `docker-compose.yaml`: Local development services
- `CELERY.md`: Detailed Celery worker configuration
- `SEQUENCE.md`: ETL workflow and data flow documentation
- `documentation.md`: Detailed API and feature documentation

## Database Schema

PostgreSQL with 7 schemas:
- `raw_data`: Source data as-is
- `staging`: Cleaned/standardized data
- `transformation`: Intermediate transformation results
- `processed`: Final processed data
- `config`: ETL metadata (data sources, jobs, parameters)
- `etl_control`: Execution logs and job status
- `audit`: Change tracking for compliance

## Cache & Messaging

**Cache**: Redis primary, in-memory LRU fallback if Redis unavailable
- Configuration: `app/core/config.py` (RedisSettings)
- Manager: `app/infrastructure/cache/`

**Messaging**: RabbitMQ for async operations
- Config: `app/core/config.py` (RabbitMqSettings)
- Publishers/consumers: `app/infrastructure/messaging/`

## Common Patterns

**Repository Pattern**: Domain interfaces in `app/domain/repositories/`, implementations in `app/infrastructure/database/repositories/`

**Dependency Injection**: Via FastAPI `depends()` in `app/interfaces/dependencies.py`

**Async**: Full async/await stack (FastAPI → Celery, SQLAlchemy async core)

**Error Handling**: Domain exceptions (app/domain/exceptions/) → HTTP errors via `app/core/exceptions.AppException` middleware

## Phase Implementation Details

### Phase 2: ETL Job Creation (IMPLEMENTED ✅)

**File**: `app/application/services/etl_service.py` → `create_etl_job()`

**Requirements** (SEQUENCE.md lines 39-69):
1. POST /api/v1/jobs endpoint
2. Accept job_config (name, type, source_type, rules, mappings)
3. Check job_dependencies status
4. BEGIN TRANSACTION
5. INSERT etl_jobs record
6. INSERT transformation_rules
7. INSERT field_mappings
8. COMMIT TRANSACTION
9. SET job:{job_id} config in cache
10. **Publish JobCreatedEvent** ✅ IMPLEMENTED
11. Return 201 Created with job details + job_id

**Implementation Details**:

1. **Explicit Transaction Management** (SQLAlchemy 2.0):
   - Uses `db.begin_nested()` to start explicit nested transaction
   - `db.flush()` after each INSERT for immediate visibility within transaction
   - `db.commit()` after all inserts to atomically commit all three: job + rules + mappings
   - `db.rollback()` on any exception to ensure data consistency

2. **Job Creation Flow**:
   - Validates required fields: job_name, job_type, source_type
   - Extracts transformation_rules and field_mappings from job_data
   - Inserts EtlJob record with `self.job_repo.create()`
   - Inserts QualityRule records (dual-purpose: transformation rules)
   - Inserts FieldMapping records
   - All three inserts happen within same transaction

3. **Event Publishing** (After COMMIT):
   - Publishes JobCreatedEvent via EventPublisher to Redis
   - Event includes: job_id, job_name, job_type, source_type, created_at, rule/mapping counts
   - Uses EventPriority.MEDIUM
   - Wrapped in try/except to prevent event failure from breaking job creation
   - Logs warning if event publishing fails (doesn't break flow)

4. **Cache Management** (After COMMIT):
   - Sets job:{job_id} cache key with full job config (TTL: 1 hour)
   - Invalidates jobs:* cache keys to refresh job list
   - Wrapped in try/except to prevent cache failure from breaking job creation
   - Falls back gracefully if cache unavailable

5. **Error Handling**:
   - Input validation happens before transaction
   - Catches any exception during transaction
   - Explicitly calls db.rollback() to undo all three inserts
   - Logs full error context with exc_info=True
   - Uses self.handle_error() for standardized error response

**Key Classes/Imports**:
- `EventType.JOB_CREATED` from `app/domain/events.py`
- `EventPriority.MEDIUM` from `app/domain/events.py`
- `QualityRule` from `app/infrastructure/db/models/etl_control/quality_rules.py`
- `FieldMapping` from `app/infrastructure/db/models/transformation/field_mappings.py`
- `cache_manager` from `app/infrastructure/cache/`
- `get_event_publisher()` from `app/utils/event_publisher.py`

**Testing**:
- Verify: POST /api/v1/jobs with valid job_data creates job record
- Verify: transformation_rules are inserted (if provided)
- Verify: field_mappings are inserted (if provided)
- Verify: JobCreatedEvent is published to Redis
- Verify: job:{job_id} is cached with 1 hour TTL
- Verify: jobs:* cache is invalidated
- Verify: On error, all inserts are rolled back atomically
- Verify: On cache/event failure, job creation still succeeds

---

## Debugging

- **Logs**: Configured in `app/core/logging.py`, output to console and files
- **Database queries**: Enable SQLAlchemy echo in config
- **Celery**: Use `--loglevel=debug` and monitor via Flower (http://localhost:5555)
- **RabbitMQ**: Admin UI at http://localhost:15672 (credentials in docker-compose)

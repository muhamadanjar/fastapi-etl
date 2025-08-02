```
fastapi-clean-arch-starter/
│
├── app/
│   ├── __init__.py
│   ├── main.py                  # Entry point aplikasi
│   ├── core/                    # Core modules
│   │   ├── __init__.py
│   │   ├── config.py            # Konfigurasi aplikasi
│   │   ├── exceptions.py        # Custom exceptions
│   │   ├── security.py          # Security utilities
│   │   └── logging.py           # Logging setup
│   │
│   ├── interfaces/              # Interface layer (presentation layer)
│   │   ├── __init__.py
│   │   ├── dependencies.py      # Shared dependencies
│   │   ├── middleware.py        # Custom middleware
│   │   │
│   │   ├── http/                # HTTP REST API
│   │   │   ├── __init__.py
│   │   │   ├── routes/          # API routes
│   │   │   │   ├── __init__.py
│   │   │   │   ├── auth.py
│   │   │   │   ├── users.py
│   │   │   │   └── ...
│   │   │   ├── controllers/     # Request handlers
│   │   │   │   ├── __init__.py
│   │   │   │   ├── auth_controller.py
│   │   │   │   ├── user_controller.py
│   │   │   │   └── ...
│   │   │   └── serializers/     # Response serializers
│   │   │       ├── __init__.py
│   │   │       └── ...
│   │   │
│   │   ├── websocket/           # WebSocket handlers
│   │   │   ├── __init__.py
│   │   │   ├── connections.py   # Connection manager
│   │   │   └── handlers/
│   │   │       ├── __init__.py
│   │   │       ├── chat_handler.py
│   │   │       └── ...
│   │   │
│   │   ├── consumer/            # Message consumers
│   │   │   ├── __init__.py
│   │   │   ├── rabbitmq/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── email_consumer.py
│   │   │   │   └── ...
│   │   │   ├── kafka/
│   │   │   │   ├── __init__.py
│   │   │   │   └── ...
│   │   │   └── sqs/
│   │   │       ├── __init__.py
│   │   │       └── ...
│   │   │
│   │   ├── cli/                 # Command line interface
│   │   │   ├── __init__.py
│   │   │   ├── commands/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── migrate.py
│   │   │   │   └── seed.py
│   │   │   └── main.py
│   │   │
│   │   └── graphql/             # GraphQL API (optional)
│   │       ├── __init__.py
│   │       ├── schema.py
│   │       └── resolvers/
│   │           ├── __init__.py
│   │           └── ...
│   │
│   ├── domain/                  # Domain model/business logic
│   │   ├── __init__.py
│   │   ├── entities/            # Entity definitions
│   │   │   ├── __init__.py
│   │   │   ├── user.py
│   │   │   └── ...
│   │   ├── value_objects/       # Value objects
│   │   │   ├── __init__.py
│   │   │   ├── email.py
│   │   │   └── ...
│   │   ├── services/            # Domain services
│   │   │   ├── __init__.py
│   │   │   ├── user_service.py
│   │   │   └── ...
│   │   ├── repositories/        # Repository interfaces
│   │   │   ├── __init__.py
│   │   │   ├── user_repository.py
│   │   │   └── ...
│   │   └── exceptions/          # Domain exceptions
│   │       ├── __init__.py
│   │       ├── user_exceptions.py
│   │       └── ...
│   │
│   ├── usecases/                # Application use cases
│   │   ├── __init__.py
│   │   ├── auth/
│   │   │   ├── __init__.py
│   │   │   ├── login.py
│   │   │   ├── register.py
│   │   │   └── ...
│   │   ├── user/
│   │   │   ├── __init__.py
│   │   │   ├── create_user.py
│   │   │   ├── get_user.py
│   │   │   └── ...
│   │   └── ...
│   │
│   ├── infrastructure/          # External services & frameworks
│   │   ├── __init__.py
│   │   ├── database/            # Database connection & repositories
│   │   │   ├── __init__.py
│   │   │   ├── connection.py
│   │   │   ├── repositories/    # Repository implementations
│   │   │   │   ├── __init__.py
│   │   │   │   ├── base.py
│   │   │   │   ├── user_repository.py
│   │   │   │   └── ...
│   │   │   └── models/          # ORM models
│   │   │       ├── __init__.py
│   │   │       ├── base.py
│   │   │       ├── user_model.py
│   │   │       └── ...
│   │   │
│   │   ├── cache/               # Caching implementation
│   │   │   ├── __init__.py
│   │   │   ├── base.py
│   │   │   ├── redis_cache.py
│   │   │   └── memory_cache.py
│   │   │
│   │   ├── messaging/           # Message broker implementation
│   │   │   ├── __init__.py
│   │   │   ├── base.py
│   │   │   ├── rabbitmq/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── publisher.py
│   │   │   │   └── consumer.py
│   │   │   ├── kafka/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── producer.py
│   │   │   │   └── consumer.py
│   │   │   └── sqs/
│   │   │       ├── __init__.py
│   │   │       └── ...
│   │   │
│   │   ├── email/               # Email service
│   │   │   ├── __init__.py
│   │   │   ├── base.py
│   │   │   ├── smtp_service.py
│   │   │   ├── ses_service.py
│   │   │   └── templates/
│   │   │       ├── base.html
│   │   │       ├── welcome.html
│   │   │       └── ...
│   │   │
│   │   ├── storage/             # File storage
│   │   │   ├── __init__.py
│   │   │   ├── base.py
│   │   │   ├── local_storage.py
│   │   │   └── s3_storage.py
│   │   │
│   │   ├── monitoring/          # Monitoring & observability
│   │   │   ├── __init__.py
│   │   │   ├── metrics.py
│   │   │   └── tracing.py
│   │   │
│   │   └── tasks/               # Background tasks
│   │       ├── __init__.py
│   │       ├── celery_app.py
│   │       ├── scheduler.py
│   │       └── tasks/
│   │           ├── __init__.py
│   │           ├── email_tasks.py
│   │           ├── data_sync_tasks.py
│   │           └── ...
│   │
│   └── schemas/                 # Pydantic models for data transfer
│       ├── __init__.py
│       ├── base.py
│       ├── auth.py
│       ├── user.py
│       └── ...
│
├── tests/                       # Tests directory
│   ├── __init__.py
│   ├── conftest.py              # Test fixtures
│   ├── unit/                    # Unit tests
│   │   ├── __init__.py
│   │   ├── domain/
│   │   ├── usecases/
│   │   └── infrastructure/
│   ├── integration/             # Integration tests
│   │   ├── __init__.py
│   │   ├── interfaces/
│   │   └── infrastructure/
│   └── e2e/                     # End-to-end tests
│       ├── __init__.py
│       └── ...
│
├── migrations/                  # Database migrations
│   ├── __init__.py
│   ├── versions/
│   │   └── ...
│   └── env.py
│
├── scripts/                     # Utility scripts
│   ├── __init__.py
│   ├── seed_data.py
│   ├── deploy.py
│   └── ...
│
├── docker/                      # Docker related files
│   ├── Dockerfile
│   ├── Dockerfile.dev
│   └── docker-compose.yml
│
├── docs/                        # Documentation
│   ├── api/
│   ├── deployment/
│   └── development/
│
├── .env.example                 # Example environment variables
├── requirements.txt             # Project dependencies
├── requirements-dev.txt         # Development dependencies
├── pyproject.toml               # Package metadata & build config
├── .gitignore                   # Git ignore file
├── .pre-commit-config.yaml      # Pre-commit hooks
└── README.md                    # Project documentation
```

# Schema DB
```sql
create schema audit;
create schema config;
create schema etl_control;
create schema processed;
create schema raw_data;
create schema staging;
create schema transformation;
```

## Migrate DB
```bash
alembic upgrade head
```

# Run
```bash
python -m app.main
```
### _OR_
```bash
python -m uvicorn app.main:app --reload
```

### Command Excuted
```bash
chmod +x start_worker.sh
```


### Enable Service
```bash
sudo systemctl daemon-reload
sudo systemctl enable celery-worker@priority
sudo systemctl enable celery-worker@background
sudo systemctl start celery-worker@priority
sudo systemctl start celery-worker@background
```


### Basic Run Celery
```bash
# Worker dasar
celery -A app.tasks.celery_app worker --loglevel=info
# Worker dengan queue spesifik
celery -A app.tasks.celery_app worker -Q etl,monitoring,cleanup --loglevel=info
celery -A app.tasks.celery_app worker --queues=default,etl,monitoring,cleanup --loglevel=info
# Run worker dengan eventlet
celery -A app.tasks.celery_app worker --pool=eventlet --queues=default,etl,monitoring,cleanup --concurrency=10 --loglevel=info
# Worker dengan concurrency (jumlah proses)
celery -A app.tasks.celery_app worker --concurrency=4 --loglevel=info
# Worker dengan nama spesifik
celery -A app.tasks.celery_app worker --hostname=worker1@%h --loglevel=info

# Beat scheduler
celery -A app.tasks.celery_app beat --loglevel=info
# Beat dengan persistent scheduler
celery -A app.tasks.celery_app beat --scheduler=celery.beat:PersistentScheduler --loglevel=info

```


Komponen Utama:
1. Metadata Management

data_sources: Konfigurasi sumber data (database, file, API)
etl_jobs: Definisi job ETL dan penjadwalannya
transformation_mappings: Pemetaan kolom dan aturan transformasi
configuration_parameters: Parameter konfigurasi yang fleksibel

2. Orchestration & Dependencies

job_dependencies: Mengatur urutan dan dependensi antar job
job_executions: Log eksekusi setiap job dengan status dan metrik

3. Quality & Monitoring

data_quality_rules: Definisi aturan validasi data
data_quality_results: Hasil pengecekan kualitas data
performance_metrics: Metrik performa untuk monitoring

4. Error Handling

error_logs: Log error detail dengan stack trace
rejected_records: Data yang ditolak beserta alasannya

5. Audit & Compliance

audit_trail: Jejak perubahan data untuk compliance
Views: Summary untuk monitoring dan reporting

Fitur Utama:
✅ Scalable: Mendukung multiple data sources dan job types
✅ Monitoring: Views dan metrik untuk monitoring real-time
✅ Error Handling: Comprehensive error logging dan retry mechanism
✅ Data Quality: Built-in data quality validation
✅ Security: Password encryption dan audit trail
✅ Performance: Indexes yang optimal untuk query cepat
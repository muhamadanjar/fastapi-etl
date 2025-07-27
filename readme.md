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
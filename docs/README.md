# ETL API MVP

ETL API has one responsibility: ingest immutable raw datasets from FILE or API Data Sources, then compose new immutable datasets through declarative JOIN or UNION Recipes.

The active HTTP surface is `/api/v1/overview`, `/api/v1/sources`, `/api/v1/datasets`, `/api/v1/recipes`, and `/api/v1/runs`. Jobs, local user management, quality orchestration, reporting, notifications, and scheduled maintenance are outside the MVP.

## Runtime

- FastAPI serves resource APIs and delegates every permission decision to User Management.
- PostgreSQL stores source definitions, encrypted credential versions, recipes, runs, datasets, and JSONB records.
- Celery executes only ingestion and transformation Runs through RabbitMQ. Run status and results remain in PostgreSQL.
- Upload API owns file bytes. ETL receives an `artifact_id` and materializes its content only while an ingestion Run executes.

## Environment

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | PostgreSQL URL, using the `postgresql://` form. |
| `USERMANAGEMENT_API_URL` | Authorization service containing `/auth/authorize`. |
| `DATA_SOURCE_SECRET_KEY` | Fernet key for API Source credentials. |
| `CELERY_BROKER_URL` | RabbitMQ broker URL. |
| `UPLOAD_API_URL` | Upload API artifact endpoint. |
| `OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET` | ETL service identity for Upload API. |
| `OAUTH_TOKEN_URL`, `OAUTH_AUDIENCE`, `OAUTH_SCOPES` | Service-to-service OAuth contract. |

Generate the source credential key with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Apply migrations and run the services:

```bash
alembic upgrade head
uvicorn app.main:app --reload
celery -A app.tasks.celery_app worker --loglevel=info
```

## Contracts and decisions

- [MVP refactor plan](./plans/etl-mvp-refactor.md)
- [Architecture decisions](./adr/)
- [OAuth Upload service client](./features/oauth-upload-service-client.md)
- [Upload artifact handoff](./features/upload-artifact-handoff.md)
- [Alembic migration repair](./features/alembic-migration-repair.md)

List responses use `{ "data": [], "metas": { "page": 1, "page_size": 20, "total_items": 0, "total_pages": 0 } }`. Object responses use `{ "data": { ... } }`.

# Delegate authorization to User Management

ETL does not own users, roles, or permissions. Every protected request carries a User Management-issued user or service token, and ETL asks User Management to validate the token and authorize the requested action; ETL stores only the actor identifier needed for audit metadata and does not synchronize a local user record.

## Consequences

Dashboard calls may forward a user token, while internal callers use service credentials issued by User Management. ETL availability for protected operations depends on User Management, and permission decisions fail closed when it cannot be reached.

The MVP permission contract is `etl.sources.read|write|ingest`, `etl.datasets.read|download|delete`, `etl.recipes.read|write|run`, and `etl.runs.read`. Overview derives its visibility from the actor's read permissions.

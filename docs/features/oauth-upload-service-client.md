Related Plan: [ETL API OAuth Service Authorization Migration](../plans/oauth-service-authorization-migration.md)

Execution Progress: [ETL API OAuth Service Authorization Progress](../progress/oauth-service-authorization-migration.md)

# OAuth Upload Service Client

ETL API and its synchronous worker obtain short-lived OAuth service tokens before calling Upload API. Configure:

```text
OAUTH_CLIENT_ID=<etl client id>
OAUTH_CLIENT_SECRET=<secret-manager reference>
OAUTH_TOKEN_URL=http://usermanagement:8000/oauth/token
UPLOAD_API_URL=http://upload-api:8010/api/v1
```

The ETL client requests audience `upload-api` with `upload.artifacts.read` and `upload.artifacts.lease`. Tokens stay in process memory, are single-flight renewed, and are never placed in task payloads or persisted.

`UPLOAD_API_SERVICE_TOKEN` remains a deprecated fallback only for the bounded dual-mode window. Partial OAuth credentials are rejected instead of silently falling back. Each fallback request emits a secret-safe `legacy_static_token` event with `outcome=used`; remove the static value after both caller and Upload resource metrics report zero legacy usage.

The internal package requires the monorepo services build context. `docker compose` is already configured accordingly; from `etl_api/` run `docker compose build`.

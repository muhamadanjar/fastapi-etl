Related Plan: [ETL API OAuth Service Authorization Migration](../plans/oauth-service-authorization-migration.md)

# ETL API OAuth Service Authorization Progress

## Status

In progress.

## Checklist

- [x] Integrate shared client-credentials token provider.
- [x] Migrate API-process Upload calls to OAuth.
- [x] Migrate worker-process Upload calls to OAuth.
- [x] Retain observable deprecated static-token fallback during dual mode.
- [x] Verify token renewal, concurrency, redaction, and downstream failures.
- [x] Confirm existing `/auth/authorize` behavior remains unchanged.
- [ ] Remove legacy configuration after Upload reports zero static usage.
- [x] Publish feature documentation.
- [x] Verify internal `service_auth` installation for API and worker container builds.
- [x] Complete `.env.example` audience/scope and secret-manager guidance.
- [x] Count deprecated static-token use at request time, not only at startup.

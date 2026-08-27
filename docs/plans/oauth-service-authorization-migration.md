# ETL API OAuth Service Authorization Migration Plan

Master Plan: [Service Principal OAuth Authorization](../../../usermanagement_api/docs/plans/service-principal-oauth-authorization.md)

Execution Progress: [ETL API OAuth Service Authorization Progress](../progress/oauth-service-authorization-migration.md)

## Objective

Migrate ETL's machine-originated calls to other internal HTTP services from static bearer credentials to OAuth `client_credentials`, without changing ETL's existing user authorization through User Management `/auth/authorize`.

## Unchanged contracts

- Protected user requests continue to delegate authorization to `/auth/authorize`.
- `etl.read`, `etl.manage`, and `etl.jobs.execute` remain the user Permissions enforced by ETL.
- User Management authorization failures remain fail closed.

## Deprecated targets

Mark the following as deprecated when the Upload dual-mode endpoint is available:

- `UPLOAD_API_SERVICE_TOKEN`;
- compatibility alias `UPLOAD_API_CALLER_TOKEN`;
- the static Authorization header construction in `UploadArtifactClient`.

The legacy value remains usable only during the agreed migration window and must emit startup and usage telemetry.

## OAuth client configuration

Create a separate client for each ETL environment. The production identity uses audience `upload-api` and receives only `upload.artifacts.read` and `upload.artifacts.lease` unless a later use case proves another Permission is necessary.

Runtime configuration must reference secret-manager values for client ID/secret, User Management token endpoint, and the intended audience. No access token is stored outside process memory.

## Implementation phases

1. Adopt the shared token client with single-flight in-memory renewal.
2. Update `UploadArtifactClient` to obtain a token for audience `upload-api` and the minimum operation scope.
3. Prefer OAuth in dual mode; allow the deprecated static header only behind the explicit compatibility setting.
4. Classify failures consistently: token acquisition unavailable as `503`, authorization rejection as the downstream `401/403` contract.
5. Verify all ETL workers/processes use OAuth, because background workers may not share the API process token cache.
6. Remove legacy environment variables and fallback construction after Upload reports zero ETL static-token use.

## Future inbound service requests

If another service calls ETL as a machine, ETL must expect audience `etl-api` and operation-specific service Permissions. If it calls on behalf of a User, ETL must require the agreed dual-identity contract; the existing user JWT alone remains valid only for direct user-originated requests.

## Verification

- API and worker processes renew tokens independently without persistence.
- Concurrent Upload calls result in one token acquisition per process renewal window.
- Wrong audience/scope and revoked tokens are rejected by Upload.
- Existing `/auth/authorize` tests and `etl.read`/`etl.manage`/`etl.jobs.execute` behavior remain unchanged.
- No token or secret is emitted in ETL logs or exception text.

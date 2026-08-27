# Upload Artifact Handoff

Related Plan: [Upload Artifact Handoff Plan](../plans/upload-artifact-handoff.md)

Related Progress: [Upload Artifact Handoff Progress](../progress/upload-artifact-handoff.md)

`POST /files/artifacts` registers an available upload_api artifact in `FileRegistry`. The request contains `artifact_id`, one-time `grant_id`, `source_system`, and optional batch metadata.

ETL uses its internal access token to exchange the grant for a durable Artifact Lease. The registry stores `artifact_id`, `artifact_lease_id`, and an `artifact://` source path; it does not duplicate the source binary. Processing workers download a temporary copy through upload_api, run the existing validation and processing pipeline, and delete the copy afterward.

Registration is idempotent by `artifact_id`. If database creation fails after lease acquisition, ETL releases the lease as compensation. Existing multipart, chunked, download, preview, and processing routes remain unchanged for legacy records.

Configure:

```env
UPLOAD_API_URL=http://upload-api:8010/api/v1
UPLOAD_API_SERVICE_TOKEN=<etl-specific-upload-api-service-token>
```

The service token must equal the `etl` entry in Upload API's `UPLOAD_API_SERVICE_TOKENS` map. `UPLOAD_API_CALLER_TOKEN` remains a deprecated fallback during migration.

Apply migration `0006_upload_artifact_ref` before enabling the dashboard cutover.

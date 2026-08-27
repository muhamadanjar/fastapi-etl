# Upload Artifact Handoff Plan

Related Progress: [Upload Artifact Handoff Progress](../progress/upload-artifact-handoff.md)

Accept new `upload_api` artifacts as ETL source files while retaining all legacy upload endpoints, paths, and FileRegistry behavior. New artifact-backed records store `artifact_id`, create/release leases idempotently, and materialize only an ephemeral processing copy.


# Upload API owns file artifacts

Upload API owns file transfer, chunks, resumability, storage, and artifact lifecycle, while ETL owns File Sources, ingestion, and Dataset snapshots. ETL receives an immutable `artifact_id`, associates it with a File Source, materializes the artifact temporarily through a service-to-service contract, and does not persist upload sessions, chunks, local file paths, or source file bytes.

## Consequences

A File Source can ingest multiple artifacts over time, each producing a distinct Raw Dataset. Existing ETL upload endpoints and storage models are migration targets rather than part of the final MVP surface.

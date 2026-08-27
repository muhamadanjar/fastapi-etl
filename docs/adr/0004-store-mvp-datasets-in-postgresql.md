# Store MVP datasets in PostgreSQL

MVP dataset metadata and records are stored in PostgreSQL, with flexible record values represented as JSONB. Each Dataset is limited to 100,000 records or 100 MB of parsed data, whichever is reached first; this favors operational simplicity over the higher scale offered by Parquet, object storage, or a distributed execution engine.

## Consequences

Ingestion rejects data beyond either limit without publishing a partial Dataset. Exceeding these limits as a product requirement triggers a storage and execution-engine redesign rather than silently weakening the guardrail.

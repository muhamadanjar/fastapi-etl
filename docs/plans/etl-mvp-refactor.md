# ETL MVP Refactor Plan

Status: accepted for implementation planning on 2026-08-27.

## Objective

Refocus ETL API on two outcomes:

1. collect repeatable snapshots from registered File and REST API sources;
2. combine immutable datasets into new immutable datasets through declarative Join or Union recipes.

The target flow is:

```text
Data Source
  -> Ingestion Run
  -> Raw Dataset
  -> Data Recipe
  -> Transformation Run
  -> Derived Dataset
```

`CONTEXT.md` is the canonical domain vocabulary. Decisions that constrain this plan are recorded in `docs/adr/0001` through `0009`.

## MVP boundaries

### Included

- File Sources backed by Upload API artifacts: CSV, Excel, and JSON.
- REST API Sources using GET, static query parameters and headers, encrypted API-key or bearer credentials, optional page-number pagination, and a configurable JSON record path.
- Immutable Raw and Derived Datasets stored in PostgreSQL JSONB.
- A maximum of 100,000 records or 100 MB parsed data per Dataset.
- Declarative select/drop/rename/cast/filter operations.
- Two-input inner/left Join and 2–10-input strict Union.
- Manually executed Recipe Chains without upstream auto-execution.
- Dataset-level lineage, paginated preview, CSV/JSON download, and explicit deletion guards.
- Four Run states: PENDING, RUNNING, SUCCEEDED, and FAILED.
- Celery as the executor for ingestion and transformation only.
- User Management as the owner of identity, service identity, and authorization.
- Dashboard/API surfaces for Overview, Sources, Datasets, Recipes, and Runs.

### Excluded

- ETL jobs, schedules, Celery Beat, job dependencies, and workflow orchestration.
- Direct database sources, XML, webhooks, streams, message-broker sources, GraphQL, POST-based extraction, and custom extraction scripts.
- Entity matching, deduplication, conflict resolution, quality-rule engines, rejected-record workflows, record-level lineage, and field-level change logs.
- Automatic PII discovery/masking, automatic schema repair, partial-success datasets, and arbitrary SQL/Python expressions.
- Notifications, alert center, resource monitoring dashboard, and system configuration UI.
- Publishing output to external databases, APIs, webhooks, or brokers.
- Automatic retention and automatic upstream Recipe execution.

## Target domain and persistence model

Define or update SQLModel models before generating any Alembic revision.

### `data_sources`

Stores the logical source, not an execution:

- `id`, unique case-insensitive `name`, optional description;
- `kind`: `FILE` or `API`;
- `status`: `ACTIVE` or `INACTIVE`;
- monotonically increasing `version`;
- non-secret declarative `config` JSONB;
- `active_credential_version`, nullable;
- `created_by_actor_id`, timestamps.

File config contains no file path, upload session, chunk, or file bytes. API config is limited to URL, static headers/query parameters, timeout, JSON record path, and optional page-number pagination settings.

### `data_source_credentials`

Stores versioned encrypted credentials:

- `source_id`, credential `version`, and auth type;
- encrypted API-key or bearer-token payload;
- creation actor and timestamp.

Secrets are write-only through the API. Source snapshots and errors store only the credential version, never decrypted values.

### `datasets`

Represents both Raw and Derived Dataset metadata:

- `id`, generated name, optional user label;
- `kind`: `RAW` or `DERIVED`;
- `schema` JSONB containing ordered column names and types;
- `record_count`, parsed `size_bytes`, timestamps;
- `source_id` for Raw Datasets;
- `produced_by_run_id` for Derived Datasets;
- `created_by_actor_id`.

Only complete Datasets are exposed by public queries. If implementation needs a private BUILDING row while streaming records, it must be invisible to public endpoints and removed on failure.

### `dataset_records`

- `dataset_id` foreign key with cascade deletion only where deletion guards permit it;
- stable `position` within the Dataset;
- original or transformed record `payload` JSONB.

Raw payloads are stored as received after parsing container formats. Logging record payloads is forbidden.

### `data_recipes`

- `id`, unique case-insensitive `name`, optional description;
- `status`: `ACTIVE` or `INACTIVE`;
- `operation`: `JOIN` or `UNION`;
- monotonically increasing `version`;
- declarative operation definition JSONB;
- `created_by_actor_id`, timestamps.

The definition schema is validated by typed request models. It must not accept executable SQL, Python, or unbounded expression strings.

### `recipe_inputs`

Stores ordered logical inputs:

- recipe and position;
- stable alias;
- selector kind: logical Data Source or pinned Dataset;
- referenced `source_id` or `dataset_id` with an exclusive database constraint.

A source selector resolves to its latest Raw Dataset by default at Run creation, with an optional older snapshot supplied by the caller. A derived input always references an existing Dataset. Save-time validation rejects cycles.

### `runs`

One durable table can represent both domain Run variants:

- `id`, `kind`: `INGESTION` or `TRANSFORMATION`;
- `status`: PENDING, RUNNING, SUCCEEDED, or FAILED;
- exactly one relevant owner reference: `source_id` or `recipe_id`;
- source/recipe version and complete non-secret definition snapshot;
- credential version reference for ingestion;
- Upload API `artifact_id` for File Source ingestion;
- `output_dataset_id`, present only after success;
- Celery task ID as operational metadata only;
- record/byte statistics, bounded error code/details, timestamps;
- `actor_id`.

Rerunning always inserts a new Run. Historical Runs are immutable except for their forward-only state transition and completion fields.

### `run_inputs`

Stores the exact ordered Dataset IDs resolved for a Transformation Run. This is the reproducibility boundary and the source of dataset-level lineage.

## Domain invariants

- A successful Ingestion Run produces exactly one Raw Dataset; a failed Run produces none.
- A successful Transformation Run produces exactly one Derived Dataset; a failed Run produces none.
- Dataset publication is atomic and enforces both size limits before success.
- A Run can transition PENDING -> RUNNING -> SUCCEEDED or FAILED only.
- A Data Source or Recipe with historical Runs is deactivated rather than deleted.
- A Dataset referenced by any Run cannot be deleted.
- An unreferenced Dataset may be deleted explicitly; source deletion never cascades to historical Datasets.
- Recipe edits increment the version. Run snapshots never change after creation.
- Source edits increment the version. Ingestion snapshots exclude secrets.
- Join accepts exactly two inputs and implements standard duplicate expansion and SQL null-key semantics.
- Union accepts 2–10 inputs and requires identical ordered schemas after explicit mapping/casts.
- Schema drift blocks execution with a structured error; no implicit repair occurs.
- Recipe chains reject cycles and never execute upstream Recipes automatically.

## API contract

Use a single envelope across all new endpoints.

Successful object response:

```json
{ "data": {} }
```

Successful list response:

```json
{
  "data": [],
  "metas": {
    "page": 1,
    "page_size": 20,
    "total_items": 0,
    "total_pages": 0
  }
}
```

All list endpoints accept `page` and `page_size`; default page size is 20 and maximum is 100.

Error response:

```json
{
  "error": {
    "code": "SCHEMA_DRIFT",
    "message": "Input dataset is incompatible with the recipe",
    "details": {}
  }
}
```

Do not return competing `success`, `result`, raw-object, or alternative pagination shapes.

### Overview

- `GET /overview`
- Counts active sources, Raw/Derived Datasets, and Recipes.
- Counts RUNNING and last-24-hour FAILED Runs.
- Returns the ten latest Runs.
- Does not expose CPU, memory, queue internals, alerts, or quality scores.

### Sources

- `GET /sources`
- `POST /sources`
- `GET /sources/{source_id}`
- `PATCH /sources/{source_id}`
- `POST /sources/{source_id}/activate`
- `POST /sources/{source_id}/deactivate`
- `POST /sources/{source_id}/test`
- `POST /sources/{source_id}/ingestions`

File ingestion accepts an Upload API `artifact_id`. API ingestion has no artifact. Test is read-only and does not create persistent connection status.

### Datasets

- `GET /datasets`
- `GET /datasets/{dataset_id}`
- `GET /datasets/{dataset_id}/records`
- `GET /datasets/{dataset_id}/lineage`
- `GET /datasets/{dataset_id}/download?format=csv|json`
- `DELETE /datasets/{dataset_id}`

Records are always paginated. Downloads require a distinct permission and stream their response without logging contents.

### Recipes

- `GET /recipes`
- `POST /recipes`
- `GET /recipes/{recipe_id}`
- `PATCH /recipes/{recipe_id}`
- `DELETE /recipes/{recipe_id}` only before the first Run
- `POST /recipes/{recipe_id}/activate`
- `POST /recipes/{recipe_id}/deactivate`
- `POST /recipes/{recipe_id}/validate`
- `POST /recipes/{recipe_id}/runs`
- `POST /recipes/{recipe_id}/clone`

Validation reports schema incompatibility, missing columns, invalid casts/filters, cycles, and potential many-to-many Join expansion. Warnings do not silently mutate a Recipe.

### Runs

- `GET /runs`
- `GET /runs/{run_id}`

There are no cancel, retry, pause, resume, status override, or schedule endpoints. Re-execution starts through the relevant Source or Recipe endpoint and creates a new Run.

## User Management authorization

Remove local authentication, user synchronization, roles, and permission persistence from ETL. Replace `/auth/info` plus local-user synchronization with a single authorization client/dependency that:

1. accepts the User Management-issued bearer token;
2. asks User Management to authorize the exact ETL permission;
3. accepts both delegated user and service identities;
4. returns only the actor identity needed for audit fields;
5. fails closed when User Management is unavailable.

Permission contract:

- `etl.sources.read`, `etl.sources.write`, `etl.sources.ingest`
- `etl.datasets.read`, `etl.datasets.download`, `etl.datasets.delete`
- `etl.recipes.read`, `etl.recipes.write`, `etl.recipes.run`
- `etl.runs.read`

Overview returns only sections authorized by the actor's read permissions.

## Upload API migration

Upload API owns upload initiation, chunks, resumability, storage, and artifact lifecycle. ETL owns the File Source and resulting Dataset.

Target interaction:

```text
Dashboard -> Upload API: upload file
Upload API -> Dashboard: artifact_id
Dashboard -> ETL: POST /sources/{source_id}/ingestions {artifact_id}
ETL -> Upload API: authorize/materialize artifact with service credential
ETL -> PostgreSQL: publish Raw Dataset atomically
```

Migration work:

- retain and simplify `UploadArtifactClient`;
- require an immutable artifact reference and validate artifact metadata before enqueueing;
- materialize only for the duration of ingestion and remove temporary files in task `finally` handling;
- remove ETL upload-session, chunk, local-path, and file-download responsibilities;
- preserve existing OAuth/service-credential integration tests while adapting them to the new ingestion endpoint.

## Execution implementation

Expose only two Celery tasks:

```text
run_ingestion(run_id)
run_transformation(run_id)
```

### Ingestion executor

1. Atomically claim a PENDING Run and mark it RUNNING.
2. Resolve the frozen source definition and credential version.
3. Materialize an Upload API artifact or fetch the configured REST API pages.
4. Parse CSV, Excel, or JSON into ordered JSON records.
5. Infer the ordered Dataset Schema.
6. Enforce record and byte limits while processing.
7. Publish Dataset metadata and records atomically.
8. Mark the Run SUCCEEDED with output/statistics, or clean private staging and mark it FAILED with a bounded structured error.

### Transformation executor

1. Atomically claim a PENDING Run and mark it RUNNING.
2. Load the frozen recipe definition and exact `run_inputs`.
3. Revalidate schemas and fail explicitly on drift.
4. Apply declarative mapping, cast, and filters.
5. Execute bounded hash Join or streaming Union while enforcing output limits.
6. Publish the Derived Dataset atomically.
7. Mark the Run SUCCEEDED, or remove private staging and mark it FAILED.

The executor must stop before unbounded many-to-many output exhausts memory or exceeds the Dataset guardrail.

## Implementation phases

### Phase 0 — Protect active integrations

- Preserve the current uncommitted Upload API/OAuth and migration-chain work.
- Make existing migration and UploadArtifactClient authentication tests green before structural deletion.
- Add contract fixtures for User Management authorization and Upload API artifact metadata/materialization.
- Capture the current Alembic head and verify upgrade from an empty database.

Exit gate: active integration tests pass and unrelated worktree changes are preserved.

### Phase 1 — Introduce shared contracts

- Add the new response envelope, pagination metadata, error codes, enums, and typed request/response schemas.
- Add authorization permission dependencies without yet removing legacy routes.
- Add model-level validators for source configs and declarative recipe definitions.

Exit gate: contract tests cover every envelope, pagination bound, secret redaction, and permission failure mode.

### Phase 2 — Define models and generate migration

- Define all SQLModel models and relationships listed above first.
- Import the models into Alembic metadata.
- Generate the revision through Alembic; do not hand-create a revision file.
- Review generated upgrade/downgrade operations, especially schema-qualified foreign keys and legacy table drops.
- Apply the migration to a clean database and run the migration-chain test.

Because legacy development data is disposable, do not implement semantic backfill from jobs/entities/quality data. Keep destructive legacy drops in this phase only after replacement tables are present in the generated ordering.

Exit gate: clean upgrade reaches head, expected tables/constraints exist, and downgrade behavior is explicitly tested or documented where destructive data loss is intentional.

### Phase 3 — Sources and ingestion

- Implement source repository/service/routes and credential encryption/redaction.
- Adapt CSV, Excel, JSON, and API processors to emit plain ordered records without job coupling.
- Implement Upload API artifact ingestion and REST GET/page-number ingestion.
- Implement atomic Raw Dataset publication and size guards.
- Add source activation, test, and ingestion endpoints.

Exit gate: File and API sources each produce complete Raw Datasets; failure and limit tests prove no public partial Dataset remains.

### Phase 4 — Dataset catalog

- Implement Dataset list/detail/schema/records/lineage/download/delete behavior.
- Add deletion-reference checks and source-history preservation.
- Add paginated preview and streaming CSV/JSON export.

Exit gate: pagination, lineage, download permission, payload-log redaction, and deletion guards pass integration tests.

### Phase 5 — Recipes and transformation

- Implement recipe versioning, activation, cloning, validation, and cycle detection.
- Implement typed Column Mapping and Dataset Filter operations.
- Implement two-input inner/left Join and strict 2–10-input Union.
- Resolve source selectors to latest or explicitly pinned snapshots when creating a Run.
- Freeze the recipe definition and exact Dataset IDs into the Run.

Exit gate: deterministic Join/Union outputs, null/duplicate semantics, schema drift, cycle rejection, and atomic failure are covered by tests.

### Phase 6 — Minimal run executor

- Replace the existing ETL task collection with the two Run tasks.
- Remove Celery Beat schedules and task autodiscovery for legacy task modules.
- Ensure idempotent claim behavior so duplicate delivery cannot execute one Run twice.
- Store task IDs for diagnostics only; PostgreSQL remains the status authority.

Exit gate: duplicate delivery, worker exception, and rerun tests preserve valid Run state and history.

### Phase 7 — Overview and route cutover

- Implement Overview aggregations and latest Runs.
- Register only Overview, Sources, Datasets, Recipes, and Runs routers.
- Remove legacy route registrations before deleting their implementation files.
- Verify the dashboard contract against generated OpenAPI.

Exit gate: OpenAPI contains only the MVP surface plus health/readiness endpoints, and all endpoint responses follow the shared envelope.

### Phase 8 — Remove legacy code

Delete only after Phases 3–7 pass:

- routes: jobs, dependencies, data quality, entities, rejected records, metrics, reports, legacy monitoring, transformation, ETL upload, local auth, errors-management, files/upload;
- services: ETL/job orchestration/dependency, quality, entity, rejected-record, metrics/report/monitoring, notification, field-mapping, legacy transformation/file/config/auth/user services;
- tasks: legacy ETL, cleanup, monitoring, task helpers, schedules, and notification tasks;
- models: `etl_control`, `processed`, `staging`, transformation rules/mappings, audit change-log/record-lineage, upload sessions, file registry, rejected records, column structure, system config, local auth/user models;
- transformers: entity matcher, quality validator, normalizer, aggregator, and legacy rule framework after their supported mapping/filter behavior is replaced;
- infrastructure: unused email, messaging, cache abstractions, worker wrappers, and event publishers after inbound references reach zero;
- schemas and commands that exist only for removed modules.

Use graph inbound traces before each deletion batch. Remove router registration and imports first, run tests, then remove unreachable files. Preserve generic logging, database, configuration, UploadArtifactClient, supported parsers, and Celery bootstrap only where the new flow still imports them.

Exit gate: graph/search finds no legacy route imports, job terminology outside migrations/ADRs, local-user synchronization, or Celery Beat registration.

### Phase 9 — Documentation cleanup

- Replace lowercase `readme.md` with a concise root `README.md` for the actual MVP.
- Keep `CONTEXT.md`, `docs/adr/`, this plan, and active Upload API/User Management contract docs.
- Delete stale job/Celery-orchestration/production-readiness/quality/entity guides and completed progress plans.
- Prefer generated OpenAPI over a manually duplicated endpoint guide.
- Update `docs/README.md` only after final route cutover so it cannot advertise removed features.

Exit gate: every retained Markdown link resolves and every documented endpoint exists in generated OpenAPI.

### Phase 10 — Final verification

- Run format/lint/type checks configured by the repository.
- Run the complete unit and integration suite.
- Apply Alembic from an empty database.
- Exercise File Source ingestion with a mocked Upload API.
- Exercise paginated API Source ingestion.
- Exercise Join, Union, Recipe Chain, schema drift, size limits, and deletion guards.
- Verify permission fail-closed behavior for each permission family.
- Confirm no record payload or credential appears in captured logs.
- Re-index the code graph and verify the architecture exposes only the intended product surface.

## Required acceptance tests

- User Management authorizes user and service tokens and ETL fails closed when unavailable.
- Upload API artifact ingestion creates one immutable Raw Dataset.
- API Source single-response and page-number modes produce equivalent snapshot semantics.
- Parsed record/byte limits reject the Run atomically.
- Failed ingestion and transformation leave no public partial Dataset.
- Latest-source resolution and explicit snapshot pinning store exact Run inputs.
- Recipe edits increment versions; historical Run snapshots do not change.
- Join validates two inputs, duplicate expansion, inner/left behavior, and null semantics.
- Union validates 2–10 strictly compatible mapped schemas.
- Mapping, basic casts, and filters are deterministic and reject unsupported expressions.
- Schema drift returns the stable error envelope.
- Recipe cycles are rejected and upstream Recipes never auto-run.
- Referenced Dataset, Source, and Recipe lifecycle guards preserve history.
- Every list endpoint uses `data` plus singular `meta`, with page-size bounds.
- Preview/download permissions are distinct and secret/payload redaction holds.
- Migration upgrade succeeds from an empty database and creates only intended MVP tables.

## Completion criteria

The refactor is complete only when:

- no public jobs API or job-oriented dashboard concept remains;
- Data Source ingestion and Dataset composition work end to end;
- only the five agreed dashboard areas are supported;
- legacy modules have zero inbound references before deletion;
- the required acceptance tests pass;
- migration and retained documentation are consistent with the final code.

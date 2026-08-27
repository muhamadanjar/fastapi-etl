# Alembic Migration Repair

Related Plan: [Repair Alembic Migrations Plan](../plans/repair-alembic-migrations.md)

Related Progress: [Repair Alembic Migrations Progress](../progress/repair-alembic-migrations.md)

## Result

The ETL migration chain has one linear head, `0007_align_model_columns`, and supports a fresh PostgreSQL database as well as an existing database at an earlier revision.

The repaired chain now:

- creates the ETL-owned schemas before schema-qualified tables;
- uses revision identifiers that fit Alembic's standard 32-character version column;
- removes migration-owned PostgreSQL enum types during downgrade-to-base;
- includes non-public schemas in autogenerate and drift checks;
- preserves existing `config.data_sources.metadata` content while renaming it to `source_metadata`;
- adds model-required job execution columns and missing indexes;
- keeps the legacy upload session table and adds the upload artifact reference columns.

Schemas are deliberately retained during downgrade. Dropping a shared schema can remove objects not owned by this migration chain, so schema deletion remains an explicit operator action.

## Operations

Inspect the chain before deployment:

```bash
alembic heads
alembic history
alembic current
```

Apply all migrations:

```bash
alembic upgrade head
```

Confirm that SQLModel metadata and the live database have no pending operations:

```bash
alembic check
```

The expected head is:

```text
0007_align_model_columns (head)
```

## Upgrade compatibility

- A database at `0005_add_job_id_to_rules` upgrades through `0006_upload_artifact_ref` and `0007_align_model_columns`.
- The former long `0006_add_upload_artifact_reference` identifier could not be stored in Alembic's default `VARCHAR(32)` version column. PostgreSQL transaction rollback leaves affected databases at `0005`, so they can safely retry with the repaired identifier.
- Revision `0007` renames `metadata` rather than copying or recreating it, preserving existing JSONB data.
- Revision `0007` accepts legacy `metadata`, current `source_metadata`, or both columns. When both exist, JSONB values are merged and `source_metadata` wins only for duplicate keys.
- Revisions `0006` and `0007` reuse compatible pre-existing columns and indexes, allowing a failed transactional upgrade to be retried safely.
- Upload session and file registry records are not deleted or rewritten during upgrade.

## Verification strategy

`tests/test_migration_chain.py` validates the single linear head, the 32-character revision limit, schema bootstrap order, artifact DDL, and enum cleanup in offline SQL. Release verification should additionally run upgrade, check, downgrade, and re-upgrade on disposable PostgreSQL matching the production major version.

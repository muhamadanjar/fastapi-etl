# Repair Alembic Migrations Plan

Related Progress: [Repair Alembic Migrations Progress](../progress/repair-alembic-migrations.md)

## Objective

Make the ETL Alembic chain deterministic and reversible for both a fresh PostgreSQL database and an existing deployment, including the upload artifact columns introduced at revision `0006`.

## Confirmed failure

`alembic upgrade head` fails on an empty PostgreSQL 16 database at revision `0001_initial` with `InvalidSchemaName: schema "audit" does not exist`. The initial revision creates schema-qualified tables without first provisioning the owned schemas.

After fixing schema bootstrap, the same clean upgrade reaches revision `0006` but fails while updating `alembic_version`: `0006_add_upload_artifact_reference` exceeds Alembic's standard `VARCHAR(32)` revision column. PostgreSQL rolls the `0006` transaction back and leaves the database at `0005`.

Once the chain reaches head, `alembic check` also fails because the environment does not inspect non-public schemas and the revision hook reads attributes that do not exist on Alembic's `MigrationScript` object.

## Delivery

1. Provision all ETL-owned PostgreSQL schemas before the first table is created.
2. Keep schema creation idempotent so environments that provision schemas separately are unaffected.
3. Shorten the `0006` revision identifier while retaining its sequence and dependency.
4. Make downgrade-to-base clean up migration-owned enum types so a subsequent upgrade does not fail with duplicate types.
5. Keep schemas themselves after downgrade to avoid deleting unrelated objects in shared databases.
6. Enable schema-aware autogenerate/check behavior and correct sequential revision assignment.
7. Add migration-chain tests that validate revision integrity and required bootstrap operations without requiring a developer database.
8. Verify on a disposable PostgreSQL database: fresh upgrade, current revision, downgrade to base, and re-upgrade to head.
9. Update ETL migration documentation and knowledge graphs.

## Existing-database compatibility

Some installations were initialized with `SQLModel.metadata.create_all()` before Alembic became authoritative. A production migration must therefore tolerate compatible columns and indexes that already exist. For `config.data_sources`, revision `0007` must preserve data in all three states: legacy `metadata` only, target `source_metadata` only, or both columns during a partially reconciled deployment.

## Definition of Done

- A blank PostgreSQL database upgrades from base to the single Alembic head.
- `alembic downgrade base` succeeds and a second `alembic upgrade head` succeeds.
- Revision `0006` adds the artifact reference columns and unique index.
- Existing schema/data are not dropped by the upgrade path.
- Static migration tests and Python compilation pass.

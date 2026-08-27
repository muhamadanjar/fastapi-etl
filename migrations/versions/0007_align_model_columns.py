"""align database columns required by current models

Revision ID: 0007_align_model_columns
Revises: 0006_upload_artifact_ref
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0007_align_model_columns"
down_revision: Union[str, None] = "0006_upload_artifact_ref"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _execute(statement: str) -> None:
    op.execute(sa.text(statement))


def upgrade() -> None:
    # Reconcile databases created by the legacy migration (`metadata`), current
    # SQLModel (`source_metadata`), or a partial deployment containing both.
    # When both exist, merge JSONB objects with source_metadata taking priority
    # for duplicate keys, then remove only the redundant legacy column.
    _execute("""
        DO $migration$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'config' AND table_name = 'data_sources'
                  AND column_name = 'metadata'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'config' AND table_name = 'data_sources'
                  AND column_name = 'source_metadata'
            ) THEN
                ALTER TABLE config.data_sources RENAME COLUMN metadata TO source_metadata;
            ELSIF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'config' AND table_name = 'data_sources'
                  AND column_name = 'metadata'
            ) AND EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'config' AND table_name = 'data_sources'
                  AND column_name = 'source_metadata'
            ) THEN
                UPDATE config.data_sources
                SET source_metadata = CASE
                    WHEN source_metadata IS NULL THEN metadata
                    WHEN metadata IS NULL THEN source_metadata
                    ELSE metadata || source_metadata
                END;
                ALTER TABLE config.data_sources DROP COLUMN metadata;
            ELSIF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'config' AND table_name = 'data_sources'
                  AND column_name = 'source_metadata'
            ) THEN
                ALTER TABLE config.data_sources ADD COLUMN source_metadata JSONB;
            END IF;
        END
        $migration$;
    """)

    _execute("""
        ALTER TABLE config.data_sources
        DROP CONSTRAINT IF EXISTS data_sources_source_name_key
    """)
    _execute("DROP INDEX IF EXISTS config.ix_config_data_sources_source_name")
    _execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ix_config_data_sources_source_name
        ON config.data_sources (source_name)
    """)

    for column_name in ("records_extracted", "records_transformed", "records_loaded"):
        _execute(
            "ALTER TABLE etl_control.job_executions "
            f"ADD COLUMN IF NOT EXISTS {column_name} INTEGER DEFAULT 0"
        )

    for column_name in ("triggered_by_parent_job_id", "parent_execution_id"):
        _execute(
            "ALTER TABLE etl_control.job_executions "
            f"ADD COLUMN IF NOT EXISTS {column_name} UUID"
        )
        _execute(
            f"CREATE INDEX IF NOT EXISTS ix_etl_control_job_executions_{column_name} "
            f"ON etl_control.job_executions ({column_name})"
        )

    # BaseModel declares indexed UUID primary keys. Earlier hand-written
    # revisions omitted these indexes for tables introduced after 0001.
    for schema_name, table_name in (
        ("config", "data_sources"),
        ("etl_control", "error_logs"),
        ("etl_control", "job_dependencies"),
        ("etl_control", "performance_metrics"),
        ("raw_data", "rejected_records"),
        ("raw_data", "upload_sessions"),
    ):
        _execute(
            f"CREATE INDEX IF NOT EXISTS ix_{schema_name}_{table_name}_id "
            f"ON {schema_name}.{table_name} (id)"
        )

    for column_name in ("status", "created_by", "expires_at"):
        _execute(f"DROP INDEX IF EXISTS raw_data.ix_upload_sessions_{column_name}")
        _execute(
            f"CREATE INDEX IF NOT EXISTS ix_raw_data_upload_sessions_{column_name} "
            f"ON raw_data.upload_sessions ({column_name})"
        )


def downgrade() -> None:
    for column_name in reversed(("status", "created_by", "expires_at")):
        _execute(
            f"DROP INDEX IF EXISTS raw_data.ix_raw_data_upload_sessions_{column_name}"
        )
        _execute(
            f"CREATE INDEX IF NOT EXISTS ix_upload_sessions_{column_name} "
            f"ON raw_data.upload_sessions ({column_name})"
        )

    for schema_name, table_name in reversed((
        ("config", "data_sources"),
        ("etl_control", "error_logs"),
        ("etl_control", "job_dependencies"),
        ("etl_control", "performance_metrics"),
        ("raw_data", "rejected_records"),
        ("raw_data", "upload_sessions"),
    )):
        _execute(f"DROP INDEX IF EXISTS {schema_name}.ix_{schema_name}_{table_name}_id")

    for column_name in reversed(("triggered_by_parent_job_id", "parent_execution_id")):
        _execute(
            f"DROP INDEX IF EXISTS "
            f"etl_control.ix_etl_control_job_executions_{column_name}"
        )
        _execute(
            "ALTER TABLE etl_control.job_executions "
            f"DROP COLUMN IF EXISTS {column_name}"
        )

    for column_name in reversed(("records_extracted", "records_transformed", "records_loaded")):
        _execute(
            "ALTER TABLE etl_control.job_executions "
            f"DROP COLUMN IF EXISTS {column_name}"
        )

    _execute("""
        DO $migration$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'config' AND table_name = 'data_sources'
                  AND column_name = 'source_metadata'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'config' AND table_name = 'data_sources'
                  AND column_name = 'metadata'
            ) THEN
                ALTER TABLE config.data_sources RENAME COLUMN source_metadata TO metadata;
            ELSIF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'config' AND table_name = 'data_sources'
                  AND column_name = 'source_metadata'
            ) AND EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'config' AND table_name = 'data_sources'
                  AND column_name = 'metadata'
            ) THEN
                UPDATE config.data_sources
                SET metadata = CASE
                    WHEN metadata IS NULL THEN source_metadata
                    WHEN source_metadata IS NULL THEN metadata
                    ELSE metadata || source_metadata
                END;
                ALTER TABLE config.data_sources DROP COLUMN source_metadata;
            ELSIF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'config' AND table_name = 'data_sources'
                  AND column_name = 'metadata'
            ) THEN
                ALTER TABLE config.data_sources ADD COLUMN metadata JSONB;
            END IF;
        END
        $migration$;
    """)

    _execute("DROP INDEX IF EXISTS config.ix_config_data_sources_source_name")
    _execute("""
        CREATE INDEX IF NOT EXISTS ix_config_data_sources_source_name
        ON config.data_sources (source_name)
    """)
    _execute("""
        DO $migration$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint constraint_record
                JOIN pg_class table_record ON table_record.oid = constraint_record.conrelid
                JOIN pg_namespace schema_record ON schema_record.oid = table_record.relnamespace
                WHERE schema_record.nspname = 'config'
                  AND table_record.relname = 'data_sources'
                  AND constraint_record.conname = 'data_sources_source_name_key'
            ) THEN
                ALTER TABLE config.data_sources
                ADD CONSTRAINT data_sources_source_name_key UNIQUE (source_name);
            END IF;
        END
        $migration$;
    """)

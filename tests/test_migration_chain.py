from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_REVISIONS = [
    '0001_initial',
    '0002_add_missing_tables',
    '0003_add_upload_sessions',
    '0004_fix_phase5_6_models',
    '0005_add_job_id_to_rules',
    '0006_upload_artifact_ref',
    '0007_align_model_columns',
    '0008_add_etl_mvp_workspace',
]
EXPECTED_SCHEMAS = (
    'audit',
    'config',
    'etl_control',
    'processed',
    'raw_data',
    'staging',
    'transformation',
    'etl',
)


def _offline_sql(*arguments: str) -> str:
    environment = os.environ.copy()
    environment['DATABASE_URL'] = 'postgresql://offline:offline@localhost/offline'
    result = subprocess.run(
        [sys.executable, '-m', 'alembic', *arguments],
        cwd=PROJECT_ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def test_revision_chain_is_linear_and_has_one_head() -> None:
    config = Config(str(PROJECT_ROOT / 'alembic.ini'))
    script = ScriptDirectory.from_config(config)
    revisions = list(reversed(list(script.walk_revisions(base='base', head='heads'))))

    assert script.get_heads() == [EXPECTED_REVISIONS[-1]]
    assert [revision.revision for revision in revisions] == EXPECTED_REVISIONS
    assert all(len(revision.revision) <= 32 for revision in revisions)


def test_fresh_upgrade_creates_schemas_before_tables() -> None:
    sql = _offline_sql('upgrade', 'head', '--sql')
    first_domain_table = sql.index('CREATE TABLE audit.change_log')

    for schema_name in EXPECTED_SCHEMAS:
        statement = f'CREATE SCHEMA IF NOT EXISTS {schema_name}'
        assert statement in sql
        assert sql.index(statement) < first_domain_table

    assert 'ADD COLUMN IF NOT EXISTS artifact_id VARCHAR(36)' in sql
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS ix_raw_data_file_registry_artifact_id' in sql
    assert "column_name = 'metadata'" in sql
    assert "column_name = 'source_metadata'" in sql
    assert 'ELSE metadata || source_metadata' in sql


def test_downgrade_removes_owned_enum_types() -> None:
    sql = _offline_sql('downgrade', 'heads:base', '--sql')

    for enum_name in (
        'jobtype',
        'jobcategory',
        'sourcetype',
        'qualityruletype',
        'executionstatus',
        'filetypeenum',
        'processingstatus',
        'qualitycheckresult',
        'datatype',
        'validationstatus',
    ):
        assert f'DROP TYPE IF EXISTS "{enum_name}"' in sql


def test_existing_database_reconciliation_is_retryable() -> None:
    upgrade_sql = _offline_sql('upgrade', 'head', '--sql')
    downgrade_sql = _offline_sql('downgrade', 'heads:base', '--sql')

    assert 'DROP CONSTRAINT IF EXISTS data_sources_source_name_key' in upgrade_sql
    assert 'ADD COLUMN IF NOT EXISTS records_extracted INTEGER DEFAULT 0' in upgrade_sql
    assert 'ADD COLUMN IF NOT EXISTS parent_execution_id UUID' in upgrade_sql
    assert 'DROP COLUMN IF EXISTS artifact_id' in downgrade_sql
    assert 'DROP INDEX IF EXISTS config.ix_config_data_sources_source_name' in downgrade_sql

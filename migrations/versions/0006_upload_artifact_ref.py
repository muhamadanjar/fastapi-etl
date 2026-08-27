"""add upload artifact reference to file registry

Revision ID: 0006_upload_artifact_ref
Revises: 0005_add_job_id_to_rules
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0006_upload_artifact_ref"
down_revision: Union[str, None] = "0005_add_job_id_to_rules"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Some existing environments created SQLModel tables before Alembic was
    # authoritative. Reusing compatible pre-existing columns is safe and keeps
    # the upgrade retryable after a transactional failure.
    op.execute(sa.text(
        "ALTER TABLE raw_data.file_registry "
        "ADD COLUMN IF NOT EXISTS artifact_id VARCHAR(36)"
    ))
    op.execute(sa.text(
        "ALTER TABLE raw_data.file_registry "
        "ADD COLUMN IF NOT EXISTS artifact_lease_id VARCHAR(36)"
    ))
    op.execute(sa.text(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_raw_data_file_registry_artifact_id "
        "ON raw_data.file_registry (artifact_id)"
    ))


def downgrade() -> None:
    op.execute(sa.text(
        "DROP INDEX IF EXISTS raw_data.ix_raw_data_file_registry_artifact_id"
    ))
    op.execute(sa.text(
        "ALTER TABLE raw_data.file_registry DROP COLUMN IF EXISTS artifact_lease_id"
    ))
    op.execute(sa.text(
        "ALTER TABLE raw_data.file_registry DROP COLUMN IF EXISTS artifact_id"
    ))

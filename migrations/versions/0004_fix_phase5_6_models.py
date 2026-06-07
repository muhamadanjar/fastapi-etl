"""fix Phase 5-6 models: add validation_status, duplicate_count, master_entity_id, job_id

Revision ID: 0004_fix_phase5_6_models
Revises: 0003_add_upload_sessions
Create Date: 2026-05-21 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0004_fix_phase5_6_models'
down_revision: Union[str, None] = '0003_add_upload_sessions'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add validation_status to staging.standardized_data
    op.add_column(
        'standardized_data',
        sa.Column('validation_status', sa.String(length=20), nullable=False,
                  server_default='pending'),
        schema='staging'
    )
    op.create_index(
        'ix_staging_standardized_data_validation_status',
        'standardized_data', ['validation_status'], schema='staging'
    )

    # 2. Add duplicate_count to processed.entities
    op.add_column(
        'entities',
        sa.Column('duplicate_count', sa.Integer(), nullable=False, server_default='0'),
        schema='processed'
    )

    # 3. Add master_entity_id to processed.entities
    op.add_column(
        'entities',
        sa.Column('master_entity_id', postgresql.UUID(as_uuid=True), nullable=True),
        schema='processed'
    )
    op.create_index(
        'ix_processed_entities_master_entity_id',
        'entities', ['master_entity_id'], schema='processed'
    )

    # 4. Add job_id to transformation.field_mappings
    op.add_column(
        'field_mappings',
        sa.Column('job_id', postgresql.UUID(as_uuid=True), nullable=True),
        schema='transformation'
    )
    op.create_foreign_key(
        'fk_field_mappings_job_id',
        'field_mappings', 'etl_jobs',
        ['job_id'], ['id'],
        source_schema='transformation',
        referent_schema='etl_control',
        ondelete='SET NULL'
    )
    op.create_index(
        'ix_transformation_field_mappings_job_id',
        'field_mappings', ['job_id'], schema='transformation'
    )


def downgrade() -> None:
    # Reverse order: drop what was last created first

    # 4. Remove job_id from transformation.field_mappings
    op.drop_index('ix_transformation_field_mappings_job_id', schema='transformation')
    op.drop_constraint('fk_field_mappings_job_id', 'field_mappings', schema='transformation', type_='foreignkey')
    op.drop_column('field_mappings', 'job_id', schema='transformation')

    # 3. Remove master_entity_id from processed.entities
    op.drop_index('ix_processed_entities_master_entity_id', schema='processed')
    op.drop_column('entities', 'master_entity_id', schema='processed')

    # 2. Remove duplicate_count from processed.entities
    op.drop_column('entities', 'duplicate_count', schema='processed')

    # 1. Remove validation_status from staging.standardized_data
    op.drop_index('ix_staging_standardized_data_validation_status', schema='staging')
    op.drop_column('standardized_data', 'validation_status', schema='staging')

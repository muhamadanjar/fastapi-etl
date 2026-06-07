"""add job_id to transformation_rules and quality_rules

Revision ID: 0005_add_job_id_to_rules
Revises: 0004_fix_phase5_6_models
Create Date: 2026-05-21 20:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0005_add_job_id_to_rules'
down_revision: Union[str, None] = '0004_fix_phase5_6_models'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add job_id to transformation.transformation_rules
    op.add_column(
        'transformation_rules',
        sa.Column('job_id', postgresql.UUID(as_uuid=True), nullable=True),
        schema='transformation'
    )
    op.create_index(
        'ix_transformation_transformation_rules_job_id',
        'transformation_rules', ['job_id'], schema='transformation'
    )
    op.create_foreign_key(
        'fk_transformation_rules_job_id',
        'transformation_rules', 'etl_jobs',
        ['job_id'], ['id'],
        source_schema='transformation',
        referent_schema='etl_control',
        ondelete='SET NULL'
    )

    # 2. Add job_id to etl_control.quality_rules
    op.add_column(
        'quality_rules',
        sa.Column('job_id', postgresql.UUID(as_uuid=True), nullable=True),
        schema='etl_control'
    )
    op.create_index(
        'ix_etl_control_quality_rules_job_id',
        'quality_rules', ['job_id'], schema='etl_control'
    )
    op.create_foreign_key(
        'fk_quality_rules_job_id',
        'quality_rules', 'etl_jobs',
        ['job_id'], ['id'],
        source_schema='etl_control',
        referent_schema='etl_control',
        ondelete='SET NULL'
    )


def downgrade() -> None:
    # Reverse order: drop what was last created first

    # 2. Remove job_id from etl_control.quality_rules
    op.drop_index('ix_etl_control_quality_rules_job_id', schema='etl_control')
    op.drop_constraint('fk_quality_rules_job_id', 'quality_rules', schema='etl_control', type_='foreignkey')
    op.drop_column('quality_rules', 'job_id', schema='etl_control')

    # 1. Remove job_id from transformation.transformation_rules
    op.drop_index('ix_transformation_transformation_rules_job_id', schema='transformation')
    op.drop_constraint('fk_transformation_rules_job_id', 'transformation_rules', schema='transformation', type_='foreignkey')
    op.drop_column('transformation_rules', 'job_id', schema='transformation')

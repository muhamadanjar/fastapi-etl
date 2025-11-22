"""add missing tables for etl system

Revision ID: a1b2c3d4e5f6
Revises: e7e5b2e30a6b
Create Date: 2025-11-22 16:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = 'e7e5b2e30a6b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create job_dependencies table
    op.create_table(
        'job_dependencies',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('parent_job_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('child_job_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('dependency_type', sa.String(length=50), nullable=False, server_default='SUCCESS'),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('description', sa.String(length=500), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['parent_job_id'], ['etl_control.etl_jobs.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['child_job_id'], ['etl_control.etl_jobs.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        schema='etl_control'
    )
    op.create_index('ix_etl_control_job_dependencies_parent_job_id', 'job_dependencies', ['parent_job_id'], schema='etl_control')
    op.create_index('ix_etl_control_job_dependencies_child_job_id', 'job_dependencies', ['child_job_id'], schema='etl_control')
    op.create_index('ix_etl_control_job_dependencies_is_active', 'job_dependencies', ['is_active'], schema='etl_control')

    # Create error_logs table
    op.create_table(
        'error_logs',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('job_execution_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('error_type', sa.String(length=50), nullable=False, server_default='UNKNOWN_ERROR'),
        sa.Column('error_severity', sa.String(length=20), nullable=False, server_default='MEDIUM'),
        sa.Column('error_message', sa.Text(), nullable=False),
        sa.Column('error_details', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('stack_trace', sa.Text(), nullable=True),
        sa.Column('context', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('is_resolved', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('resolved_at', sa.DateTime(), nullable=True),
        sa.Column('resolved_by', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('occurred_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['job_execution_id'], ['etl_control.job_executions.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['resolved_by'], ['users.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        schema='etl_control'
    )
    op.create_index('ix_etl_control_error_logs_job_execution_id', 'error_logs', ['job_execution_id'], schema='etl_control')
    op.create_index('ix_etl_control_error_logs_error_type', 'error_logs', ['error_type'], schema='etl_control')
    op.create_index('ix_etl_control_error_logs_error_severity', 'error_logs', ['error_severity'], schema='etl_control')
    op.create_index('ix_etl_control_error_logs_is_resolved', 'error_logs', ['is_resolved'], schema='etl_control')
    op.create_index('ix_etl_control_error_logs_occurred_at', 'error_logs', ['occurred_at'], schema='etl_control')

    # Create performance_metrics table
    op.create_table(
        'performance_metrics',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('execution_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('records_per_second', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column('memory_usage_mb', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column('cpu_usage_percent', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('disk_io_mb', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column('network_io_mb', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column('duration_seconds', sa.Integer(), nullable=True),
        sa.Column('peak_memory_mb', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column('avg_cpu_percent', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('cache_hit_rate', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('error_rate', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('recorded_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['execution_id'], ['etl_control.job_executions.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        schema='etl_control'
    )
    op.create_index('ix_etl_control_performance_metrics_execution_id', 'performance_metrics', ['execution_id'], schema='etl_control')
    op.create_index('ix_etl_control_performance_metrics_recorded_at', 'performance_metrics', ['recorded_at'], schema='etl_control')

    # Create rejected_records table
    op.create_table(
        'rejected_records',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('source_file_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('source_record_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('row_number', sa.Integer(), nullable=True),
        sa.Column('raw_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('rejection_reason', sa.Text(), nullable=False),
        sa.Column('validation_errors', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('can_retry', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('retry_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_retry_at', sa.DateTime(), nullable=True),
        sa.Column('is_resolved', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('resolved_at', sa.DateTime(), nullable=True),
        sa.Column('batch_id', sa.String(length=50), nullable=True),
        sa.Column('rejected_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['source_file_id'], ['raw_data.file_registry.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['source_record_id'], ['raw_data.raw_records.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        schema='raw_data'
    )
    op.create_index('ix_raw_data_rejected_records_source_file_id', 'rejected_records', ['source_file_id'], schema='raw_data')
    op.create_index('ix_raw_data_rejected_records_source_record_id', 'rejected_records', ['source_record_id'], schema='raw_data')
    op.create_index('ix_raw_data_rejected_records_can_retry', 'rejected_records', ['can_retry'], schema='raw_data')
    op.create_index('ix_raw_data_rejected_records_is_resolved', 'rejected_records', ['is_resolved'], schema='raw_data')
    op.create_index('ix_raw_data_rejected_records_batch_id', 'rejected_records', ['batch_id'], schema='raw_data')
    op.create_index('ix_raw_data_rejected_records_rejected_at', 'rejected_records', ['rejected_at'], schema='raw_data')

    # Create data_sources table
    op.create_table(
        'data_sources',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('source_name', sa.String(length=100), nullable=False),
        sa.Column('source_type', sa.String(length=50), nullable=False),
        sa.Column('description', sa.String(length=500), nullable=True),
        sa.Column('connection_config', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('credentials_encrypted', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('connection_status', sa.String(length=20), nullable=False, server_default='INACTIVE'),
        sa.Column('last_connection_test', sa.DateTime(), nullable=True),
        sa.Column('connection_pool_size', sa.Integer(), nullable=True, server_default='5'),
        sa.Column('timeout_seconds', sa.Integer(), nullable=True, server_default='30'),
        sa.Column('retry_attempts', sa.Integer(), nullable=True, server_default='3'),
        sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('created_by', postgresql.UUID(as_uuid=True), nullable=True),
        sa.ForeignKeyConstraint(['created_by'], ['users.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('source_name'),
        schema='config'
    )
    op.create_index('ix_config_data_sources_source_name', 'data_sources', ['source_name'], schema='config')
    op.create_index('ix_config_data_sources_source_type', 'data_sources', ['source_type'], schema='config')
    op.create_index('ix_config_data_sources_is_active', 'data_sources', ['is_active'], schema='config')
    op.create_index('ix_config_data_sources_connection_status', 'data_sources', ['connection_status'], schema='config')


def downgrade() -> None:
    # Drop tables in reverse order
    op.drop_table('data_sources', schema='config')
    op.drop_table('rejected_records', schema='raw_data')
    op.drop_table('performance_metrics', schema='etl_control')
    op.drop_table('error_logs', schema='etl_control')
    op.drop_table('job_dependencies', schema='etl_control')

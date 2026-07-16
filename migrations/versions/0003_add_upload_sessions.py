"""add upload_sessions table for chunked uploads

Revision ID: 0003_add_upload_sessions
Revises: 0002_add_missing_tables
Create Date: 2026-05-19 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0003_add_upload_sessions'
down_revision: Union[str, None] = '0002_add_missing_tables'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'upload_sessions',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('file_name', sa.String(length=255), nullable=False),
        sa.Column('file_path', sa.String(length=500), nullable=False),
        sa.Column('file_size', sa.BigInteger(), nullable=False),
        sa.Column('file_type', sa.String(length=50), nullable=False),
        sa.Column('chunk_size', sa.Integer(), nullable=False),
        sa.Column('total_chunks', sa.Integer(), nullable=False),
        sa.Column('uploaded_chunks', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('received_bytes', sa.BigInteger(), nullable=False, server_default='0'),
        sa.Column('chunk_map', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='pending'),
        sa.Column('source_system', sa.String(length=100), nullable=False),
        sa.Column('batch_id', sa.String(length=100), nullable=True),
        sa.Column('file_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_by', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('file_registry_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('expires_at', sa.DateTime(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['file_registry_id'], ['raw_data.file_registry.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        schema='raw_data'
    )
    op.create_index('ix_upload_sessions_status', 'upload_sessions', ['status'], schema='raw_data')
    op.create_index('ix_upload_sessions_created_by', 'upload_sessions', ['created_by'], schema='raw_data')
    op.create_index('ix_upload_sessions_expires_at', 'upload_sessions', ['expires_at'], schema='raw_data')


def downgrade() -> None:
    op.drop_index('ix_upload_sessions_expires_at', schema='raw_data')
    op.drop_index('ix_upload_sessions_created_by', schema='raw_data')
    op.drop_index('ix_upload_sessions_status', schema='raw_data')
    op.drop_table('upload_sessions', schema='raw_data')

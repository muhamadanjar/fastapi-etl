from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel

from app.infrastructure.db.models.base import BaseModel


ETL_SCHEMA = "etl"


class SourceKind(str, Enum):
    FILE = "FILE"
    API = "API"


class ResourceStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class CredentialType(str, Enum):
    NONE = "NONE"
    API_KEY = "API_KEY"
    BEARER = "BEARER"


class DatasetKind(str, Enum):
    RAW = "RAW"
    DERIVED = "DERIVED"


class RecipeOperation(str, Enum):
    JOIN = "JOIN"
    UNION = "UNION"


class SelectorKind(str, Enum):
    SOURCE = "SOURCE"
    DATASET = "DATASET"


class RunKind(str, Enum):
    INGESTION = "INGESTION"
    TRANSFORMATION = "TRANSFORMATION"


class RunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


def _created_at_column() -> Column:
    return Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


def _updated_at_column() -> Column:
    return Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class DataSource(BaseModel, table=True):
    __tablename__ = "data_sources"
    __table_args__ = (
        Index("uq_etl_data_sources_name_lower", text("lower(name)"), unique=True),
        {"schema": ETL_SCHEMA},
    )

    name: str = Field(sa_column=Column(String(100), nullable=False))
    description: Optional[str] = Field(
        default=None,
        sa_column=Column(String(500), nullable=True),
    )
    kind: SourceKind = Field(sa_column=Column(String(20), nullable=False, index=True))
    status: ResourceStatus = Field(
        default=ResourceStatus.ACTIVE,
        sa_column=Column(String(20), nullable=False, index=True),
    )
    version: int = Field(default=1, ge=1, sa_column=Column(Integer, nullable=False))
    config: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSONB, nullable=False),
    )
    active_credential_version: Optional[int] = Field(default=None, ge=1)
    created_by_actor_id: str = Field(sa_column=Column(String(255), nullable=False))
    created_at: datetime = Field(default_factory=datetime.utcnow, sa_column=_created_at_column())
    updated_at: datetime = Field(default_factory=datetime.utcnow, sa_column=_updated_at_column())


class DataSourceCredential(BaseModel, table=True):
    __tablename__ = "data_source_credentials"
    __table_args__ = (
        UniqueConstraint("source_id", "version", name="uq_etl_source_credential_version"),
        {"schema": ETL_SCHEMA},
    )

    source_id: UUID = Field(
        sa_column=Column(
            ForeignKey(f"{ETL_SCHEMA}.data_sources.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
    )
    version: int = Field(ge=1, sa_column=Column(Integer, nullable=False))
    credential_type: CredentialType = Field(
        sa_column=Column(String(20), nullable=False)
    )
    encrypted_secret: Optional[str] = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
    )
    header_name: Optional[str] = Field(
        default=None,
        sa_column=Column(String(100), nullable=True),
    )
    created_by_actor_id: str = Field(sa_column=Column(String(255), nullable=False))
    created_at: datetime = Field(default_factory=datetime.utcnow, sa_column=_created_at_column())


class DataRecipe(BaseModel, table=True):
    __tablename__ = "data_recipes"
    __table_args__ = (
        Index("uq_etl_data_recipes_name_lower", text("lower(name)"), unique=True),
        {"schema": ETL_SCHEMA},
    )

    name: str = Field(sa_column=Column(String(100), nullable=False))
    description: Optional[str] = Field(
        default=None,
        sa_column=Column(String(500), nullable=True),
    )
    status: ResourceStatus = Field(
        default=ResourceStatus.ACTIVE,
        sa_column=Column(String(20), nullable=False, index=True),
    )
    operation: RecipeOperation = Field(
        sa_column=Column(String(20), nullable=False, index=True)
    )
    version: int = Field(default=1, ge=1, sa_column=Column(Integer, nullable=False))
    definition: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSONB, nullable=False),
    )
    created_by_actor_id: str = Field(sa_column=Column(String(255), nullable=False))
    created_at: datetime = Field(default_factory=datetime.utcnow, sa_column=_created_at_column())
    updated_at: datetime = Field(default_factory=datetime.utcnow, sa_column=_updated_at_column())


class Run(BaseModel, table=True):
    __tablename__ = "runs"
    __table_args__ = (
        CheckConstraint(
            "(kind = 'INGESTION' AND source_id IS NOT NULL AND recipe_id IS NULL) OR "
            "(kind = 'TRANSFORMATION' AND source_id IS NULL AND recipe_id IS NOT NULL)",
            name="ck_etl_runs_owner_matches_kind",
        ),
        CheckConstraint(
            "(status = 'SUCCEEDED' AND completed_at IS NOT NULL) OR "
            "(status = 'FAILED' AND completed_at IS NOT NULL) OR "
            "status IN ('PENDING', 'RUNNING')",
            name="ck_etl_runs_completion_matches_status",
        ),
        {"schema": ETL_SCHEMA},
    )

    kind: RunKind = Field(sa_column=Column(String(20), nullable=False, index=True))
    status: RunStatus = Field(
        default=RunStatus.PENDING,
        sa_column=Column(String(20), nullable=False, index=True),
    )
    source_id: Optional[UUID] = Field(
        default=None,
        sa_column=Column(
            ForeignKey(f"{ETL_SCHEMA}.data_sources.id", ondelete="RESTRICT"),
            nullable=True,
            index=True,
        ),
    )
    recipe_id: Optional[UUID] = Field(
        default=None,
        sa_column=Column(
            ForeignKey(f"{ETL_SCHEMA}.data_recipes.id", ondelete="RESTRICT"),
            nullable=True,
            index=True,
        ),
    )
    definition_version: int = Field(ge=1, sa_column=Column(Integer, nullable=False))
    definition_snapshot: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSONB, nullable=False),
    )
    credential_version: Optional[int] = Field(default=None, ge=1)
    artifact_id: Optional[str] = Field(
        default=None,
        sa_column=Column(String(100), nullable=True),
    )
    celery_task_id: Optional[str] = Field(
        default=None,
        sa_column=Column(String(100), nullable=True, index=True),
    )
    records_processed: int = Field(default=0, ge=0)
    output_size_bytes: int = Field(
        default=0,
        ge=0,
        sa_column=Column(BigInteger, nullable=False, server_default="0"),
    )
    error_code: Optional[str] = Field(
        default=None,
        sa_column=Column(String(100), nullable=True),
    )
    error_details: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB, nullable=True),
    )
    actor_id: str = Field(sa_column=Column(String(255), nullable=False))
    created_at: datetime = Field(default_factory=datetime.utcnow, sa_column=_created_at_column())
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)


class Dataset(BaseModel, table=True):
    __tablename__ = "datasets"
    __table_args__ = (
        CheckConstraint(
            "(kind = 'RAW' AND source_id IS NOT NULL) OR "
            "(kind = 'DERIVED' AND source_id IS NULL AND produced_by_run_id IS NOT NULL)",
            name="ck_etl_datasets_origin_matches_kind",
        ),
        UniqueConstraint("produced_by_run_id", name="uq_etl_dataset_producing_run"),
        {"schema": ETL_SCHEMA},
    )

    name: str = Field(sa_column=Column(String(160), nullable=False, index=True))
    label: Optional[str] = Field(
        default=None,
        sa_column=Column(String(160), nullable=True),
    )
    kind: DatasetKind = Field(sa_column=Column(String(20), nullable=False, index=True))
    dataset_schema: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column("schema", JSONB, nullable=False),
    )
    record_count: int = Field(default=0, ge=0)
    size_bytes: int = Field(
        default=0,
        ge=0,
        sa_column=Column(BigInteger, nullable=False, server_default="0"),
    )
    source_id: Optional[UUID] = Field(
        default=None,
        sa_column=Column(
            ForeignKey(f"{ETL_SCHEMA}.data_sources.id", ondelete="RESTRICT"),
            nullable=True,
            index=True,
        ),
    )
    produced_by_run_id: Optional[UUID] = Field(
        default=None,
        sa_column=Column(
            ForeignKey(f"{ETL_SCHEMA}.runs.id", ondelete="RESTRICT"),
            nullable=True,
            index=True,
        ),
    )
    created_by_actor_id: str = Field(sa_column=Column(String(255), nullable=False))
    created_at: datetime = Field(default_factory=datetime.utcnow, sa_column=_created_at_column())


class DatasetRecord(BaseModel, table=True):
    __tablename__ = "dataset_records"
    __table_args__ = (
        UniqueConstraint("dataset_id", "position", name="uq_etl_dataset_record_position"),
        {"schema": ETL_SCHEMA},
    )

    dataset_id: UUID = Field(
        sa_column=Column(
            ForeignKey(f"{ETL_SCHEMA}.datasets.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
    )
    position: int = Field(ge=0, sa_column=Column(Integer, nullable=False))
    payload: Dict[str, Any] = Field(sa_column=Column(JSONB, nullable=False))


class RecipeInput(BaseModel, table=True):
    __tablename__ = "recipe_inputs"
    __table_args__ = (
        UniqueConstraint("recipe_id", "position", name="uq_etl_recipe_input_position"),
        UniqueConstraint("recipe_id", "alias", name="uq_etl_recipe_input_alias"),
        CheckConstraint(
            "(selector_kind = 'SOURCE' AND source_id IS NOT NULL AND dataset_id IS NULL) OR "
            "(selector_kind = 'DATASET' AND source_id IS NULL AND dataset_id IS NOT NULL)",
            name="ck_etl_recipe_input_selector",
        ),
        {"schema": ETL_SCHEMA},
    )

    recipe_id: UUID = Field(
        sa_column=Column(
            ForeignKey(f"{ETL_SCHEMA}.data_recipes.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
    )
    position: int = Field(ge=0, sa_column=Column(Integer, nullable=False))
    alias: str = Field(sa_column=Column(String(64), nullable=False))
    selector_kind: SelectorKind = Field(sa_column=Column(String(20), nullable=False))
    source_id: Optional[UUID] = Field(
        default=None,
        sa_column=Column(
            ForeignKey(f"{ETL_SCHEMA}.data_sources.id", ondelete="RESTRICT"),
            nullable=True,
        ),
    )
    dataset_id: Optional[UUID] = Field(
        default=None,
        sa_column=Column(
            ForeignKey(f"{ETL_SCHEMA}.datasets.id", ondelete="RESTRICT"),
            nullable=True,
        ),
    )


class RunInput(BaseModel, table=True):
    __tablename__ = "run_inputs"
    __table_args__ = (
        UniqueConstraint("run_id", "position", name="uq_etl_run_input_position"),
        {"schema": ETL_SCHEMA},
    )

    run_id: UUID = Field(
        sa_column=Column(
            ForeignKey(f"{ETL_SCHEMA}.runs.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
    )
    dataset_id: UUID = Field(
        sa_column=Column(
            ForeignKey(f"{ETL_SCHEMA}.datasets.id", ondelete="RESTRICT"),
            nullable=False,
            index=True,
        )
    )
    position: int = Field(ge=0, sa_column=Column(Integer, nullable=False))
    alias: str = Field(sa_column=Column(String(64), nullable=False))

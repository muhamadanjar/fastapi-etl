from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generic, List, Literal, Optional, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator

from app.infrastructure.db.models.workspace import (
    CredentialType,
    DatasetKind,
    RecipeOperation,
    ResourceStatus,
    RunKind,
    RunStatus,
    SelectorKind,
    SourceKind,
)


class PageMeta(BaseModel):
    page: int
    page_size: int
    total_items: int
    total_pages: int


EnvelopeData = TypeVar("EnvelopeData")


class ListEnvelope(BaseModel, Generic[EnvelopeData]):
    data: List[EnvelopeData]
    metas: PageMeta


class ObjectEnvelope(BaseModel, Generic[EnvelopeData]):
    data: EnvelopeData


class ErrorBody(BaseModel):
    code: str
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)


class ErrorEnvelope(BaseModel):
    error: ErrorBody


class ApiPaginationConfig(BaseModel):
    mode: Literal["NONE", "PAGE"] = "NONE"
    page_param: str = "page"
    page_size_param: str = "page_size"
    start_page: int = Field(default=1, ge=0)
    page_size: int = Field(default=100, ge=1, le=1000)
    max_pages: int = Field(default=1000, ge=1, le=10000)


class ApiSourceConfig(BaseModel):
    url: HttpUrl
    headers: Dict[str, str] = Field(default_factory=dict)
    query: Dict[str, str] = Field(default_factory=dict)
    timeout_seconds: int = Field(default=30, ge=1, le=300)
    records_path: Optional[str] = None
    pagination: ApiPaginationConfig = Field(default_factory=ApiPaginationConfig)

    @model_validator(mode="after")
    def reject_secret_headers(self):
        secret_names = {
            "authorization",
            "proxy-authorization",
            "x-api-key",
            "x-auth-token",
            "cookie",
        }
        invalid = sorted(name for name in self.headers if name.lower() in secret_names)
        if invalid:
            raise ValueError(
                "Authentication headers must be supplied through Source Credential: "
                + ", ".join(invalid)
            )
        secret_query_names = {
            "access_token",
            "api_key",
            "apikey",
            "key",
            "password",
            "secret",
            "token",
        }
        invalid_query = sorted(
            name for name in self.query if name.lower() in secret_query_names
        )
        if invalid_query:
            raise ValueError(
                "Authentication query parameters are not allowed in public Source config: "
                + ", ".join(invalid_query)
            )
        return self


class FileSourceConfig(BaseModel):
    allowed_formats: List[Literal["CSV", "EXCEL", "JSON"]] = Field(
        default_factory=lambda: ["CSV", "EXCEL", "JSON"]
    )


class CredentialWrite(BaseModel):
    credential_type: CredentialType = CredentialType.NONE
    secret: Optional[str] = Field(default=None, min_length=1, max_length=10000)
    header_name: Optional[str] = Field(default=None, min_length=1, max_length=100)

    @model_validator(mode="after")
    def validate_secret(self):
        if self.credential_type == CredentialType.NONE:
            if self.secret is not None or self.header_name is not None:
                raise ValueError("NONE credential cannot contain a secret or header name")
            return self
        if not self.secret:
            raise ValueError("Authenticated API Sources require a secret")
        if self.credential_type == CredentialType.API_KEY and not self.header_name:
            raise ValueError("API_KEY credential requires header_name")
        if self.credential_type == CredentialType.BEARER and self.header_name:
            raise ValueError("BEARER credential always uses Authorization header")
        return self


class DataSourceCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    description: Optional[str] = Field(default=None, max_length=500)
    kind: SourceKind
    config: Dict[str, Any] = Field(default_factory=dict)
    credential: Optional[CredentialWrite] = None

    @model_validator(mode="after")
    def validate_kind_config(self):
        if self.kind == SourceKind.FILE:
            FileSourceConfig.model_validate(self.config)
            if self.credential and self.credential.credential_type != CredentialType.NONE:
                raise ValueError("File Sources cannot contain API credentials")
        else:
            ApiSourceConfig.model_validate(self.config)
        return self


class DataSourceUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=100)
    description: Optional[str] = Field(default=None, max_length=500)
    config: Optional[Dict[str, Any]] = None
    credential: Optional[CredentialWrite] = None


class DataSourceRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    description: Optional[str]
    kind: SourceKind
    status: ResourceStatus
    version: int
    config: Dict[str, Any]
    has_credential: bool = False
    credential_type: Optional[CredentialType] = None
    created_by_actor_id: str
    created_at: datetime
    updated_at: datetime


class FileIngestionCreate(BaseModel):
    artifact_id: str = Field(min_length=1, max_length=100)
    grant_id: Optional[str] = Field(default=None, max_length=100)


class ApiIngestionCreate(BaseModel):
    pass


class DatasetRead(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: UUID
    name: str
    label: Optional[str]
    kind: DatasetKind
    dataset_schema: Dict[str, Any] = Field(serialization_alias="schema")
    record_count: int
    size_bytes: int
    source_id: Optional[UUID]
    produced_by_run_id: Optional[UUID]
    created_by_actor_id: str
    created_at: datetime


class SelectorInput(BaseModel):
    alias: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z][A-Za-z0-9_]*$")
    selector_kind: SelectorKind
    source_id: Optional[UUID] = None
    dataset_id: Optional[UUID] = None

    @model_validator(mode="after")
    def validate_selector(self):
        if self.selector_kind == SelectorKind.SOURCE:
            if not self.source_id or self.dataset_id:
                raise ValueError("SOURCE selector requires only source_id")
        elif not self.dataset_id or self.source_id:
            raise ValueError("DATASET selector requires only dataset_id")
        return self


class CastType(str, Enum):
    STRING = "STRING"
    NUMBER = "NUMBER"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"


class ColumnMappingSpec(BaseModel):
    source_alias: str = Field(min_length=1, max_length=64)
    source_column: str = Field(min_length=1, max_length=255)
    target_column: str = Field(min_length=1, max_length=255)
    cast: Optional[CastType] = None


class FilterOperator(str, Enum):
    EQ = "EQ"
    NE = "NE"
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"
    CONTAINS = "CONTAINS"
    IS_EMPTY = "IS_EMPTY"
    IS_NOT_EMPTY = "IS_NOT_EMPTY"


class DatasetFilterSpec(BaseModel):
    column: str = Field(min_length=1, max_length=255)
    operator: FilterOperator
    value: Any = None

    @model_validator(mode="after")
    def validate_value(self):
        unary = {FilterOperator.IS_EMPTY, FilterOperator.IS_NOT_EMPTY}
        if self.operator in unary and self.value is not None:
            raise ValueError(f"{self.operator.value} does not accept value")
        if self.operator not in unary and self.value is None:
            raise ValueError(f"{self.operator.value} requires value")
        return self


class JoinDefinition(BaseModel):
    join_type: Literal["INNER", "LEFT"]
    left_keys: List[str] = Field(min_length=1)
    right_keys: List[str] = Field(min_length=1)
    mappings: List[ColumnMappingSpec] = Field(default_factory=list)
    filters: List[DatasetFilterSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_keys(self):
        if len(self.left_keys) != len(self.right_keys):
            raise ValueError("left_keys and right_keys must have equal length")
        return self


class UnionDefinition(BaseModel):
    mappings: List[ColumnMappingSpec] = Field(default_factory=list)
    filters: List[DatasetFilterSpec] = Field(default_factory=list)


class DataRecipeCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    description: Optional[str] = Field(default=None, max_length=500)
    operation: RecipeOperation
    inputs: List[SelectorInput]
    definition: Dict[str, Any]

    @model_validator(mode="after")
    def validate_operation(self):
        input_aliases = [item.alias for item in self.inputs]
        aliases = [alias.lower() for alias in input_aliases]
        if len(set(aliases)) != len(aliases):
            raise ValueError("Recipe input aliases must be unique")
        if self.operation == RecipeOperation.JOIN:
            if len(self.inputs) != 2:
                raise ValueError("JOIN requires exactly two inputs")
            definition = JoinDefinition.model_validate(self.definition)
            mapping_aliases = {mapping.source_alias for mapping in definition.mappings}
            unknown = sorted(mapping_aliases - set(input_aliases))
            if unknown:
                raise ValueError(f"Unknown mapping aliases: {', '.join(unknown)}")
            targets = [mapping.target_column for mapping in definition.mappings]
            if len(set(targets)) != len(targets):
                raise ValueError("JOIN mapping target columns must be unique")
        else:
            if not 2 <= len(self.inputs) <= 10:
                raise ValueError("UNION requires 2 to 10 inputs")
            definition = UnionDefinition.model_validate(self.definition)
            mapping_aliases = {mapping.source_alias for mapping in definition.mappings}
            unknown = sorted(mapping_aliases - set(input_aliases))
            if unknown:
                raise ValueError(f"Unknown mapping aliases: {', '.join(unknown)}")
            if definition.mappings:
                targets_by_alias = {
                    alias: [
                        mapping.target_column
                        for mapping in definition.mappings
                        if mapping.source_alias == alias
                    ]
                    for alias in input_aliases
                }
                expected = targets_by_alias[input_aliases[0]]
                if not expected or any(
                    targets != expected for targets in targets_by_alias.values()
                ):
                    raise ValueError(
                        "UNION mappings must define identical target columns for every input"
                    )
                if len(set(expected)) != len(expected):
                    raise ValueError("UNION mapping target columns must be unique")
        return self


class DataRecipeUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=100)
    description: Optional[str] = Field(default=None, max_length=500)
    inputs: Optional[List[SelectorInput]] = None
    definition: Optional[Dict[str, Any]] = None


class DataRecipeRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    description: Optional[str]
    status: ResourceStatus
    operation: RecipeOperation
    version: int
    definition: Dict[str, Any]
    inputs: List[SelectorInput] = Field(default_factory=list)
    created_by_actor_id: str
    created_at: datetime
    updated_at: datetime


class RecipeRunCreate(BaseModel):
    dataset_overrides: Dict[str, UUID] = Field(default_factory=dict)


class RunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    kind: RunKind
    status: RunStatus
    source_id: Optional[UUID]
    recipe_id: Optional[UUID]
    definition_version: int
    artifact_id: Optional[str]
    celery_task_id: Optional[str]
    records_processed: int
    output_size_bytes: int
    error_code: Optional[str]
    error_details: Optional[Dict[str, Any]]
    actor_id: str
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    output_dataset_id: Optional[UUID] = None


class RecipeValidationRead(BaseModel):
    valid: bool
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[Dict[str, Any]] = Field(default_factory=list)


class OverviewRead(BaseModel):
    active_sources: int
    raw_datasets: int
    derived_datasets: int
    active_recipes: int
    running_runs: int
    failed_runs_last_24h: int
    latest_runs: List[RunRead]

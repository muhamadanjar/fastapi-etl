from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from uuid import UUID

import requests
from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from app.application.services.credential_cipher import CredentialCipher
from app.application.services.dataset_engine import schema_columns
from app.core.workspace_errors import WorkspaceError, not_found
from app.infrastructure.db.models.workspace import (
    CredentialType,
    DataRecipe,
    DataSource,
    DataSourceCredential,
    Dataset,
    DatasetKind,
    DatasetRecord,
    RecipeInput,
    RecipeOperation,
    ResourceStatus,
    Run,
    RunInput,
    RunKind,
    RunStatus,
    SelectorKind,
    SourceKind,
)
from app.schemas.workspace import (
    ApiSourceConfig,
    CredentialWrite,
    DataRecipeCreate,
    DataRecipeRead,
    DataRecipeUpdate,
    DataSourceCreate,
    DataSourceRead,
    DataSourceUpdate,
    DatasetRead,
    FileSourceConfig,
    JoinDefinition,
    OverviewRead,
    PageMeta,
    RecipeRunCreate,
    RecipeValidationRead,
    RunRead,
    SelectorInput,
    UnionDefinition,
)


DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100


def enum_value(value: Any) -> Any:
    return getattr(value, "value", value)


def page_meta(page: int, page_size: int, total_items: int) -> PageMeta:
    return PageMeta(
        page=page,
        page_size=page_size,
        total_items=total_items,
        total_pages=math.ceil(total_items / page_size) if total_items else 0,
    )


def bounded_page(page: int, page_size: int) -> tuple[int, int, int]:
    if page < 1:
        raise WorkspaceError("INVALID_PAGE", "page must be at least 1", status_code=422)
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise WorkspaceError(
            "INVALID_PAGE_SIZE",
            f"page_size must be between 1 and {MAX_PAGE_SIZE}",
            status_code=422,
        )
    return page, page_size, (page - 1) * page_size


class WorkspaceService:
    def __init__(self, session: Session):
        self.session = session

    def _commit(self, conflict_message: str) -> None:
        try:
            self.session.commit()
        except IntegrityError as exc:
            self.session.rollback()
            raise WorkspaceError("CONFLICT", conflict_message, status_code=409) from exc

    def _source(self, source_id: UUID) -> DataSource:
        source = self.session.get(DataSource, source_id)
        if not source:
            raise not_found("Data Source")
        return source

    def _dataset(self, dataset_id: UUID) -> Dataset:
        dataset = self.session.get(Dataset, dataset_id)
        if not dataset:
            raise not_found("Dataset")
        return dataset

    def _recipe(self, recipe_id: UUID) -> DataRecipe:
        recipe = self.session.get(DataRecipe, recipe_id)
        if not recipe:
            raise not_found("Data Recipe")
        return recipe

    def _run(self, run_id: UUID) -> Run:
        run = self.session.get(Run, run_id)
        if not run:
            raise not_found("Run")
        return run

    def _credential(self, source: DataSource) -> Optional[DataSourceCredential]:
        if source.active_credential_version is None:
            return None
        return self.session.exec(
            select(DataSourceCredential).where(
                DataSourceCredential.source_id == source.id,
                DataSourceCredential.version == source.active_credential_version,
            )
        ).first()

    def _source_read(self, source: DataSource) -> DataSourceRead:
        credential = self._credential(source)
        data = DataSourceRead.model_validate(source)
        data.has_credential = bool(
            credential and credential.credential_type != CredentialType.NONE
        )
        data.credential_type = credential.credential_type if credential else None
        return data

    def create_source(self, body: DataSourceCreate, actor_id: str) -> DataSourceRead:
        config = self._validated_source_config(body.kind, body.config)
        source = DataSource(
            name=body.name.strip(),
            description=body.description,
            kind=body.kind,
            config=config,
            created_by_actor_id=actor_id,
        )
        self.session.add(source)
        self.session.flush()
        if body.credential:
            self._replace_credential(source, body.credential, actor_id)
        self._commit("A Data Source with this name already exists")
        self.session.refresh(source)
        return self._source_read(source)

    def list_sources(
        self,
        page: int,
        page_size: int,
        *,
        kind: Optional[SourceKind] = None,
        status: Optional[ResourceStatus] = None,
    ) -> tuple[List[DataSourceRead], PageMeta]:
        page, page_size, offset = bounded_page(page, page_size)
        filters = []
        if kind:
            filters.append(DataSource.kind == kind)
        if status:
            filters.append(DataSource.status == status)
        total = self.session.exec(
            select(func.count()).select_from(DataSource).where(*filters)
        ).one()
        rows = self.session.exec(
            select(DataSource)
            .where(*filters)
            .order_by(DataSource.created_at.desc())
            .offset(offset)
            .limit(page_size)
        ).all()
        return [self._source_read(row) for row in rows], page_meta(page, page_size, total)

    def get_source(self, source_id: UUID) -> DataSourceRead:
        return self._source_read(self._source(source_id))

    def update_source(
        self, source_id: UUID, body: DataSourceUpdate, actor_id: str
    ) -> DataSourceRead:
        source = self._source(source_id)
        values = body.model_dump(exclude_unset=True, exclude={"credential"})
        if "config" in values:
            values["config"] = self._validated_source_config(source.kind, values["config"])
        for name, value in values.items():
            setattr(source, name, value.strip() if name == "name" else value)
        source.version += 1
        source.updated_at = datetime.utcnow()
        self.session.add(source)
        if body.credential is not None:
            self._replace_credential(source, body.credential, actor_id)
        self._commit("A Data Source with this name already exists")
        self.session.refresh(source)
        return self._source_read(source)

    def set_source_status(
        self, source_id: UUID, status: ResourceStatus
    ) -> DataSourceRead:
        source = self._source(source_id)
        source.status = status
        source.version += 1
        source.updated_at = datetime.utcnow()
        self.session.add(source)
        self.session.commit()
        self.session.refresh(source)
        return self._source_read(source)

    def test_source(self, source_id: UUID) -> Dict[str, Any]:
        source = self._source(source_id)
        if source.kind == SourceKind.FILE:
            return {"reachable": True, "message": "File Source uses Upload API artifacts"}
        config = ApiSourceConfig.model_validate(source.config)
        headers = dict(config.headers)
        credential = self._decrypted_credential(source)
        if credential:
            if credential.credential_type == CredentialType.API_KEY:
                headers[str(credential.header_name)] = str(credential.secret)
            elif credential.credential_type == CredentialType.BEARER:
                headers["Authorization"] = f"Bearer {credential.secret}"
        try:
            response = requests.get(
                str(config.url),
                headers=headers,
                params=config.query,
                timeout=config.timeout_seconds,
            )
        except requests.RequestException as exc:
            return {"reachable": False, "message": str(exc)[:500]}
        return {
            "reachable": response.status_code < 400,
            "status_code": response.status_code,
        }

    def create_ingestion_run(
        self,
        source_id: UUID,
        actor_id: str,
        *,
        artifact_id: Optional[str] = None,
    ) -> RunRead:
        source = self._source(source_id)
        if source.status != ResourceStatus.ACTIVE:
            raise WorkspaceError("SOURCE_INACTIVE", "Data Source is inactive", status_code=409)
        if source.kind == SourceKind.FILE and not artifact_id:
            raise WorkspaceError(
                "ARTIFACT_REQUIRED", "File Source ingestion requires artifact_id", status_code=422
            )
        if source.kind == SourceKind.API and artifact_id:
            raise WorkspaceError(
                "ARTIFACT_NOT_ALLOWED", "API Source ingestion cannot include artifact_id", status_code=422
            )
        snapshot = {
            "source_id": str(source.id),
            "name": source.name,
            "kind": enum_value(source.kind),
            "config": source.config,
        }
        run = Run(
            kind=RunKind.INGESTION,
            source_id=source.id,
            definition_version=source.version,
            definition_snapshot=snapshot,
            credential_version=source.active_credential_version,
            artifact_id=artifact_id,
            actor_id=actor_id,
        )
        self.session.add(run)
        self.session.commit()
        self.session.refresh(run)
        return self.run_read(run)

    def create_recipe(self, body: DataRecipeCreate, actor_id: str) -> DataRecipeRead:
        self._validate_recipe_definition(body.operation, body.definition, body.inputs)
        self._validate_selector_targets(body.inputs)
        recipe = DataRecipe(
            name=body.name.strip(),
            description=body.description,
            operation=body.operation,
            definition=body.definition,
            created_by_actor_id=actor_id,
        )
        self.session.add(recipe)
        self.session.flush()
        self._replace_recipe_inputs(recipe.id, body.inputs)
        self._ensure_no_recipe_cycle(recipe.id)
        self._commit("A Data Recipe with this name already exists")
        self.session.refresh(recipe)
        return self.recipe_read(recipe)

    def list_recipes(
        self,
        page: int,
        page_size: int,
        status: Optional[ResourceStatus] = None,
    ) -> tuple[List[DataRecipeRead], PageMeta]:
        page, page_size, offset = bounded_page(page, page_size)
        filters = [DataRecipe.status == status] if status else []
        total = self.session.exec(
            select(func.count()).select_from(DataRecipe).where(*filters)
        ).one()
        rows = self.session.exec(
            select(DataRecipe)
            .where(*filters)
            .order_by(DataRecipe.created_at.desc())
            .offset(offset)
            .limit(page_size)
        ).all()
        return [self.recipe_read(row) for row in rows], page_meta(page, page_size, total)

    def recipe_read(self, recipe: DataRecipe) -> DataRecipeRead:
        result = DataRecipeRead.model_validate(recipe)
        rows = self.session.exec(
            select(RecipeInput)
            .where(RecipeInput.recipe_id == recipe.id)
            .order_by(RecipeInput.position)
        ).all()
        result.inputs = [
            SelectorInput(
                alias=row.alias,
                selector_kind=row.selector_kind,
                source_id=row.source_id,
                dataset_id=row.dataset_id,
            )
            for row in rows
        ]
        return result

    def get_recipe(self, recipe_id: UUID) -> DataRecipeRead:
        return self.recipe_read(self._recipe(recipe_id))

    def update_recipe(
        self, recipe_id: UUID, body: DataRecipeUpdate
    ) -> DataRecipeRead:
        recipe = self._recipe(recipe_id)
        values = body.model_dump(exclude_unset=True, exclude={"inputs"})
        definition = values.get("definition", recipe.definition)
        candidate_inputs = body.inputs if body.inputs is not None else self.recipe_read(recipe).inputs
        self._validate_recipe_definition(recipe.operation, definition, candidate_inputs)
        for name, value in values.items():
            setattr(recipe, name, value.strip() if name == "name" else value)
        recipe.version += 1
        recipe.updated_at = datetime.utcnow()
        self.session.add(recipe)
        if body.inputs is not None:
            self._validate_input_count(recipe.operation, body.inputs)
            self._validate_selector_targets(body.inputs)
            self._replace_recipe_inputs(recipe.id, body.inputs)
        self.session.flush()
        self._ensure_no_recipe_cycle(recipe.id)
        self._commit("A Data Recipe with this name already exists")
        self.session.refresh(recipe)
        return self.recipe_read(recipe)

    def set_recipe_status(
        self, recipe_id: UUID, status: ResourceStatus
    ) -> DataRecipeRead:
        recipe = self._recipe(recipe_id)
        recipe.status = status
        recipe.version += 1
        recipe.updated_at = datetime.utcnow()
        self.session.add(recipe)
        self.session.commit()
        self.session.refresh(recipe)
        return self.recipe_read(recipe)

    def delete_recipe(self, recipe_id: UUID) -> None:
        recipe = self._recipe(recipe_id)
        has_run = self.session.exec(
            select(func.count()).select_from(Run).where(Run.recipe_id == recipe.id)
        ).one()
        if has_run:
            raise WorkspaceError(
                "RECIPE_HAS_HISTORY",
                "A Recipe with historical Runs can only be deactivated",
                status_code=409,
            )
        self.session.delete(recipe)
        self.session.commit()

    def clone_recipe(self, recipe_id: UUID, actor_id: str) -> DataRecipeRead:
        original = self._recipe(recipe_id)
        inputs = self.recipe_read(original).inputs
        clone = DataRecipeCreate(
            name=f"{original.name} copy",
            description=original.description,
            operation=original.operation,
            inputs=inputs,
            definition=original.definition,
        )
        return self.create_recipe(clone, actor_id)

    def validate_recipe(self, recipe_id: UUID) -> RecipeValidationRead:
        recipe = self._recipe(recipe_id)
        errors: List[Dict[str, Any]] = []
        warnings: List[Dict[str, Any]] = []
        try:
            resolved = self._resolve_recipe_inputs(recipe, {})
            self._validate_resolved_schemas(recipe, resolved)
            if recipe.operation == RecipeOperation.JOIN:
                warnings.extend(self._join_duplicate_warnings(recipe, resolved))
        except WorkspaceError as exc:
            errors.append({"code": exc.code, "message": exc.message, "details": exc.details})
        return RecipeValidationRead(valid=not errors, errors=errors, warnings=warnings)

    def create_transformation_run(
        self, recipe_id: UUID, body: RecipeRunCreate, actor_id: str
    ) -> RunRead:
        recipe = self._recipe(recipe_id)
        if recipe.status != ResourceStatus.ACTIVE:
            raise WorkspaceError("RECIPE_INACTIVE", "Data Recipe is inactive", status_code=409)
        resolved = self._resolve_recipe_inputs(recipe, body.dataset_overrides)
        self._validate_resolved_schemas(recipe, resolved)
        recipe_read = self.recipe_read(recipe)
        snapshot = {
            "recipe_id": str(recipe.id),
            "name": recipe.name,
            "operation": enum_value(recipe.operation),
            "definition": recipe.definition,
            "inputs": [item.model_dump(mode="json") for item in recipe_read.inputs],
        }
        run = Run(
            kind=RunKind.TRANSFORMATION,
            recipe_id=recipe.id,
            definition_version=recipe.version,
            definition_snapshot=snapshot,
            actor_id=actor_id,
        )
        self.session.add(run)
        self.session.flush()
        for position, (alias, dataset) in enumerate(resolved):
            self.session.add(
                RunInput(
                    run_id=run.id,
                    dataset_id=dataset.id,
                    position=position,
                    alias=alias,
                )
            )
        self.session.commit()
        self.session.refresh(run)
        return self.run_read(run)

    def list_datasets(
        self,
        page: int,
        page_size: int,
        kind: Optional[DatasetKind] = None,
        source_id: Optional[UUID] = None,
    ) -> tuple[List[DatasetRead], PageMeta]:
        page, page_size, offset = bounded_page(page, page_size)
        filters = []
        if kind:
            filters.append(Dataset.kind == kind)
        if source_id:
            filters.append(Dataset.source_id == source_id)
        total = self.session.exec(
            select(func.count()).select_from(Dataset).where(*filters)
        ).one()
        rows = self.session.exec(
            select(Dataset)
            .where(*filters)
            .order_by(Dataset.created_at.desc())
            .offset(offset)
            .limit(page_size)
        ).all()
        return [DatasetRead.model_validate(row) for row in rows], page_meta(page, page_size, total)

    def get_dataset(self, dataset_id: UUID) -> DatasetRead:
        return DatasetRead.model_validate(self._dataset(dataset_id))

    def dataset_records(
        self, dataset_id: UUID, page: int, page_size: int
    ) -> tuple[List[Dict[str, Any]], PageMeta]:
        self._dataset(dataset_id)
        page, page_size, offset = bounded_page(page, page_size)
        total = self.session.exec(
            select(func.count()).select_from(DatasetRecord).where(
                DatasetRecord.dataset_id == dataset_id
            )
        ).one()
        rows = self.session.exec(
            select(DatasetRecord)
            .where(DatasetRecord.dataset_id == dataset_id)
            .order_by(DatasetRecord.position)
            .offset(offset)
            .limit(page_size)
        ).all()
        return [row.payload for row in rows], page_meta(page, page_size, total)

    def dataset_lineage(self, dataset_id: UUID) -> Dict[str, Any]:
        dataset = self._dataset(dataset_id)
        if dataset.kind == DatasetKind.RAW:
            return {
                "dataset_id": str(dataset.id),
                "kind": enum_value(dataset.kind),
                "source_id": str(dataset.source_id),
                "inputs": [],
            }
        run = self._run(dataset.produced_by_run_id)
        inputs = self.session.exec(
            select(RunInput).where(RunInput.run_id == run.id).order_by(RunInput.position)
        ).all()
        return {
            "dataset_id": str(dataset.id),
            "kind": enum_value(dataset.kind),
            "recipe_id": str(run.recipe_id),
            "recipe_version": run.definition_version,
            "run_id": str(run.id),
            "inputs": [
                {"alias": item.alias, "dataset_id": str(item.dataset_id)} for item in inputs
            ],
        }

    def delete_dataset(self, dataset_id: UUID) -> None:
        dataset = self._dataset(dataset_id)
        referenced = self.session.exec(
            select(func.count()).select_from(RunInput).where(
                RunInput.dataset_id == dataset.id
            )
        ).one()
        recipe_ref = self.session.exec(
            select(func.count()).select_from(RecipeInput).where(
                RecipeInput.dataset_id == dataset.id
            )
        ).one()
        if referenced or recipe_ref:
            raise WorkspaceError(
                "DATASET_REFERENCED",
                "Dataset is referenced by a Recipe or Transformation Run",
                status_code=409,
            )
        self.session.delete(dataset)
        self.session.commit()

    def list_runs(
        self,
        page: int,
        page_size: int,
        kind: Optional[RunKind] = None,
        status: Optional[RunStatus] = None,
    ) -> tuple[List[RunRead], PageMeta]:
        page, page_size, offset = bounded_page(page, page_size)
        filters = []
        if kind:
            filters.append(Run.kind == kind)
        if status:
            filters.append(Run.status == status)
        total = self.session.exec(
            select(func.count()).select_from(Run).where(*filters)
        ).one()
        rows = self.session.exec(
            select(Run)
            .where(*filters)
            .order_by(Run.created_at.desc())
            .offset(offset)
            .limit(page_size)
        ).all()
        return [self.run_read(row) for row in rows], page_meta(page, page_size, total)

    def run_read(self, run: Run) -> RunRead:
        result = RunRead.model_validate(run)
        output = self.session.exec(
            select(Dataset.id).where(Dataset.produced_by_run_id == run.id)
        ).first()
        result.output_dataset_id = output
        return result

    def get_run(self, run_id: UUID) -> RunRead:
        return self.run_read(self._run(run_id))

    def set_run_task_id(self, run_id: UUID, task_id: str) -> RunRead:
        run = self._run(run_id)
        run.celery_task_id = task_id
        self.session.add(run)
        self.session.commit()
        self.session.refresh(run)
        return self.run_read(run)

    def fail_pending_run(self, run_id: UUID, code: str, message: str) -> None:
        run = self._run(run_id)
        if run.status != RunStatus.PENDING:
            return
        run.status = RunStatus.FAILED
        run.completed_at = datetime.utcnow()
        run.error_code = code
        run.error_details = {"message": message[:1000]}
        self.session.add(run)
        self.session.commit()

    def overview(self) -> OverviewRead:
        def count(statement):
            return self.session.exec(statement).one()

        latest = self.session.exec(
            select(Run).order_by(Run.created_at.desc()).limit(10)
        ).all()
        return OverviewRead(
            active_sources=count(
                select(func.count()).select_from(DataSource).where(
                    DataSource.status == ResourceStatus.ACTIVE
                )
            ),
            raw_datasets=count(
                select(func.count()).select_from(Dataset).where(Dataset.kind == DatasetKind.RAW)
            ),
            derived_datasets=count(
                select(func.count()).select_from(Dataset).where(
                    Dataset.kind == DatasetKind.DERIVED
                )
            ),
            active_recipes=count(
                select(func.count()).select_from(DataRecipe).where(
                    DataRecipe.status == ResourceStatus.ACTIVE
                )
            ),
            running_runs=count(
                select(func.count()).select_from(Run).where(Run.status == RunStatus.RUNNING)
            ),
            failed_runs_last_24h=count(
                select(func.count()).select_from(Run).where(
                    Run.status == RunStatus.FAILED,
                    Run.completed_at >= datetime.utcnow() - timedelta(hours=24),
                )
            ),
            latest_runs=[self.run_read(run) for run in latest],
        )

    def _validated_source_config(
        self, kind: SourceKind, config: Mapping[str, Any]
    ) -> Dict[str, Any]:
        parsed = (
            FileSourceConfig.model_validate(config)
            if kind == SourceKind.FILE
            else ApiSourceConfig.model_validate(config)
        )
        return parsed.model_dump(mode="json")

    def _replace_credential(
        self, source: DataSource, body: CredentialWrite, actor_id: str
    ) -> None:
        version = (source.active_credential_version or 0) + 1
        encrypted = None
        if body.secret:
            encrypted = CredentialCipher().encrypt(body.secret)
        credential = DataSourceCredential(
            source_id=source.id,
            version=version,
            credential_type=body.credential_type,
            encrypted_secret=encrypted,
            header_name=body.header_name,
            created_by_actor_id=actor_id,
        )
        source.active_credential_version = version
        self.session.add(credential)
        self.session.add(source)

    def _decrypted_credential(self, source: DataSource) -> Optional[CredentialWrite]:
        row = self._credential(source)
        if not row or row.credential_type == CredentialType.NONE:
            return None
        if not row.encrypted_secret:
            raise WorkspaceError(
                "CREDENTIAL_INVALID", "Source Credential has no encrypted secret", status_code=500
            )
        return CredentialWrite(
            credential_type=row.credential_type,
            secret=CredentialCipher().decrypt(row.encrypted_secret),
            header_name=row.header_name,
        )

    def _validate_recipe_definition(
        self,
        operation: RecipeOperation,
        definition: Mapping[str, Any],
        inputs: Sequence[SelectorInput],
    ) -> None:
        try:
            DataRecipeCreate(
                name="recipe-validation",
                operation=operation,
                inputs=list(inputs),
                definition=dict(definition),
            )
        except ValidationError as exc:
            raise WorkspaceError(
                "INVALID_RECIPE",
                "Recipe definition is invalid",
                status_code=422,
                details={"errors": exc.errors(include_context=False, include_input=False)},
            ) from exc

    def _validate_input_count(
        self, operation: RecipeOperation, inputs: Sequence[SelectorInput]
    ) -> None:
        if operation == RecipeOperation.JOIN and len(inputs) != 2:
            raise WorkspaceError("INVALID_RECIPE", "JOIN requires exactly two inputs", status_code=422)
        if operation == RecipeOperation.UNION and not 2 <= len(inputs) <= 10:
            raise WorkspaceError("INVALID_RECIPE", "UNION requires 2 to 10 inputs", status_code=422)

    def _validate_selector_targets(self, inputs: Sequence[SelectorInput]) -> None:
        aliases = set()
        for item in inputs:
            normalized = item.alias.lower()
            if normalized in aliases:
                raise WorkspaceError("INVALID_RECIPE", "Input aliases must be unique", status_code=422)
            aliases.add(normalized)
            if item.selector_kind == SelectorKind.SOURCE:
                self._source(item.source_id)
            else:
                self._dataset(item.dataset_id)

    def _replace_recipe_inputs(
        self, recipe_id: UUID, inputs: Sequence[SelectorInput]
    ) -> None:
        existing = self.session.exec(
            select(RecipeInput).where(RecipeInput.recipe_id == recipe_id)
        ).all()
        for row in existing:
            self.session.delete(row)
        self.session.flush()
        for position, item in enumerate(inputs):
            self.session.add(
                RecipeInput(
                    recipe_id=recipe_id,
                    position=position,
                    alias=item.alias,
                    selector_kind=item.selector_kind,
                    source_id=item.source_id,
                    dataset_id=item.dataset_id,
                )
            )

    def _upstream_recipes(self, recipe_id: UUID) -> set[UUID]:
        result: set[UUID] = set()
        inputs = self.session.exec(
            select(RecipeInput).where(
                RecipeInput.recipe_id == recipe_id,
                RecipeInput.dataset_id.is_not(None),
            )
        ).all()
        for item in inputs:
            dataset = self.session.get(Dataset, item.dataset_id)
            if not dataset or not dataset.produced_by_run_id:
                continue
            run = self.session.get(Run, dataset.produced_by_run_id)
            if run and run.recipe_id:
                result.add(run.recipe_id)
        return result

    def _ensure_no_recipe_cycle(self, recipe_id: UUID) -> None:
        visiting: set[UUID] = set()
        visited: set[UUID] = set()

        def visit(current: UUID) -> None:
            if current in visiting:
                raise WorkspaceError(
                    "RECIPE_CYCLE", "Recipe Chain cannot contain a cycle", status_code=422
                )
            if current in visited:
                return
            visiting.add(current)
            for upstream in self._upstream_recipes(current):
                visit(upstream)
            visiting.remove(current)
            visited.add(current)

        visit(recipe_id)

    def _latest_raw_dataset(self, source_id: UUID) -> Dataset:
        dataset = self.session.exec(
            select(Dataset)
            .where(Dataset.source_id == source_id, Dataset.kind == DatasetKind.RAW)
            .order_by(Dataset.created_at.desc())
        ).first()
        if not dataset:
            raise WorkspaceError(
                "INPUT_DATASET_UNAVAILABLE",
                "Data Source does not have a successful Raw Dataset",
                status_code=409,
                details={"source_id": str(source_id)},
            )
        return dataset

    def _resolve_recipe_inputs(
        self, recipe: DataRecipe, overrides: Mapping[str, UUID]
    ) -> List[tuple[str, Dataset]]:
        rows = self.session.exec(
            select(RecipeInput)
            .where(RecipeInput.recipe_id == recipe.id)
            .order_by(RecipeInput.position)
        ).all()
        resolved = []
        known_aliases = {row.alias for row in rows}
        unknown = sorted(set(overrides) - known_aliases)
        if unknown:
            raise WorkspaceError(
                "INVALID_INPUT_OVERRIDE",
                "Unknown input override alias",
                status_code=422,
                details={"aliases": unknown},
            )
        for row in rows:
            if row.alias in overrides:
                dataset = self._dataset(overrides[row.alias])
                if row.selector_kind == SelectorKind.SOURCE and dataset.source_id != row.source_id:
                    raise WorkspaceError(
                        "INVALID_INPUT_OVERRIDE",
                        "Pinned Dataset does not belong to the selected Data Source",
                        status_code=422,
                        details={"alias": row.alias},
                    )
            elif row.selector_kind == SelectorKind.SOURCE:
                dataset = self._latest_raw_dataset(row.source_id)
            else:
                dataset = self._dataset(row.dataset_id)
            resolved.append((row.alias, dataset))
        return resolved

    def _validate_resolved_schemas(
        self, recipe: DataRecipe, inputs: Sequence[tuple[str, Dataset]]
    ) -> None:
        aliases = {alias: schema_columns(dataset.dataset_schema) for alias, dataset in inputs}
        if recipe.operation == RecipeOperation.JOIN:
            definition = JoinDefinition.model_validate(recipe.definition)
            left_alias, right_alias = inputs[0][0], inputs[1][0]
            missing_left = [key for key in definition.left_keys if key not in aliases[left_alias]]
            missing_right = [key for key in definition.right_keys if key not in aliases[right_alias]]
            if missing_left or missing_right:
                raise WorkspaceError(
                    "SCHEMA_DRIFT",
                    "Join key columns are missing",
                    status_code=409,
                    details={"left": missing_left, "right": missing_right},
                )
            for mapping in definition.mappings:
                if mapping.source_alias not in aliases or mapping.source_column not in aliases[mapping.source_alias]:
                    raise WorkspaceError(
                        "SCHEMA_DRIFT",
                        "Mapped input column is missing",
                        status_code=409,
                        details={
                            "alias": mapping.source_alias,
                            "column": mapping.source_column,
                        },
                    )
        else:
            definition = UnionDefinition.model_validate(recipe.definition)
            for mapping in definition.mappings:
                if mapping.source_alias not in aliases or mapping.source_column not in aliases[mapping.source_alias]:
                    raise WorkspaceError(
                        "SCHEMA_DRIFT",
                        "Mapped input column is missing",
                        status_code=409,
                        details={
                            "alias": mapping.source_alias,
                            "column": mapping.source_column,
                        },
                    )

    def _join_duplicate_warnings(
        self, recipe: DataRecipe, inputs: Sequence[tuple[str, Dataset]]
    ) -> List[Dict[str, Any]]:
        definition = JoinDefinition.model_validate(recipe.definition)
        warnings = []
        duplicate_sides = []
        for alias, dataset, keys in (
            (inputs[0][0], inputs[0][1], definition.left_keys),
            (inputs[1][0], inputs[1][1], definition.right_keys),
        ):
            rows = self.session.exec(
                select(DatasetRecord.payload).where(DatasetRecord.dataset_id == dataset.id)
            ).all()
            seen = set()
            duplicated = False
            for payload in rows:
                key = tuple(payload.get(name) for name in keys)
                if any(value is None for value in key):
                    continue
                if key in seen:
                    duplicated = True
                    break
                seen.add(key)
            if duplicated:
                duplicate_sides.append(alias)
        if len(duplicate_sides) == 2:
            warnings.append(
                {
                    "code": "MANY_TO_MANY_JOIN",
                    "message": "Both Join inputs contain duplicate keys; output rows may expand",
                }
            )
        return warnings

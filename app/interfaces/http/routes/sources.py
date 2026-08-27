from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query, status

from app.application.services.workspace_service import WorkspaceService
from app.core.workspace_errors import WorkspaceError
from app.infrastructure.db.models.workspace import ResourceStatus, SourceKind
from app.interfaces.workspace_dependencies import get_actor_id, get_workspace_service
from app.schemas.workspace import (
    ApiIngestionCreate,
    DataSourceCreate,
    DataSourceUpdate,
    FileIngestionCreate,
    DataSourceRead,
    ListEnvelope,
    ObjectEnvelope,
    RunRead,
)


router = APIRouter(prefix="/sources", tags=["Data Sources"])


def _enqueue_ingestion(run_id: UUID) -> str:
    from app.tasks.run_tasks import execute_ingestion_run

    return execute_ingestion_run.delay(str(run_id)).id


@router.post("", status_code=status.HTTP_201_CREATED, response_model=ObjectEnvelope[DataSourceRead])
def create_source(
    body: DataSourceCreate,
    actor_id: str = Depends(get_actor_id),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {"data": service.create_source(body, actor_id).model_dump(mode="json")}


@router.get("", response_model=ListEnvelope[DataSourceRead])
def list_sources(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    kind: Optional[SourceKind] = None,
    resource_status: Optional[ResourceStatus] = Query(None, alias="status"),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    rows, meta = service.list_sources(page, page_size, kind=kind, status=resource_status)
    return {
        "data": [row.model_dump(mode="json") for row in rows],
        "metas": meta.model_dump(),
    }


@router.get("/{source_id}", response_model=ObjectEnvelope[DataSourceRead])
def get_source(
    source_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {"data": service.get_source(source_id).model_dump(mode="json")}


@router.patch("/{source_id}", response_model=ObjectEnvelope[DataSourceRead])
def update_source(
    source_id: UUID,
    body: DataSourceUpdate,
    actor_id: str = Depends(get_actor_id),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {"data": service.update_source(source_id, body, actor_id).model_dump(mode="json")}


@router.post("/{source_id}/activate", response_model=ObjectEnvelope[DataSourceRead])
def activate_source(
    source_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {
        "data": service.set_source_status(source_id, ResourceStatus.ACTIVE).model_dump(mode="json")
    }


@router.post("/{source_id}/deactivate", response_model=ObjectEnvelope[DataSourceRead])
def deactivate_source(
    source_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {
        "data": service.set_source_status(source_id, ResourceStatus.INACTIVE).model_dump(mode="json")
    }


@router.post("/{source_id}/test", response_model=ObjectEnvelope[Dict[str, Any]])
def test_source(
    source_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {"data": service.test_source(source_id)}


@router.post(
    "/{source_id}/ingestions/file",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ObjectEnvelope[RunRead],
)
def ingest_file(
    source_id: UUID,
    body: FileIngestionCreate,
    actor_id: str = Depends(get_actor_id),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    run = service.create_ingestion_run(source_id, actor_id, artifact_id=body.artifact_id)
    try:
        run = service.set_run_task_id(run.id, _enqueue_ingestion(run.id))
    except Exception as exc:
        service.fail_pending_run(run.id, "ENQUEUE_FAILED", "Worker queue is unavailable")
        raise WorkspaceError(
            "WORKER_UNAVAILABLE", "Worker queue is unavailable", status_code=503
        ) from exc
    return {"data": run.model_dump(mode="json")}


@router.post(
    "/{source_id}/ingestions/api",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ObjectEnvelope[RunRead],
)
def ingest_api(
    source_id: UUID,
    body: ApiIngestionCreate,
    actor_id: str = Depends(get_actor_id),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    run = service.create_ingestion_run(source_id, actor_id)
    try:
        run = service.set_run_task_id(run.id, _enqueue_ingestion(run.id))
    except Exception as exc:
        service.fail_pending_run(run.id, "ENQUEUE_FAILED", "Worker queue is unavailable")
        raise WorkspaceError(
            "WORKER_UNAVAILABLE", "Worker queue is unavailable", status_code=503
        ) from exc
    return {"data": run.model_dump(mode="json")}

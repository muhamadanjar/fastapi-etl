from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from app.application.services.workspace_service import WorkspaceService
from app.infrastructure.db.models.workspace import RunKind, RunStatus
from app.interfaces.workspace_dependencies import get_workspace_service
from app.schemas.workspace import ListEnvelope, ObjectEnvelope, RunRead


router = APIRouter(prefix="/runs", tags=["Runs"])


@router.get("", response_model=ListEnvelope[RunRead])
def list_runs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    kind: Optional[RunKind] = None,
    run_status: Optional[RunStatus] = Query(None, alias="status"),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    rows, meta = service.list_runs(page, page_size, kind, run_status)
    return {
        "data": [row.model_dump(mode="json") for row in rows],
        "metas": meta.model_dump(),
    }


@router.get("/{run_id}", response_model=ObjectEnvelope[RunRead])
def get_run(
    run_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {"data": service.get_run(run_id).model_dump(mode="json")}

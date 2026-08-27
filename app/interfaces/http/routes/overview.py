from fastapi import APIRouter, Depends

from app.application.services.workspace_service import WorkspaceService
from app.interfaces.workspace_dependencies import get_workspace_service
from app.schemas.workspace import ObjectEnvelope, OverviewRead


router = APIRouter(prefix="/overview", tags=["Overview"])


@router.get("", response_model=ObjectEnvelope[OverviewRead])
def overview(service: WorkspaceService = Depends(get_workspace_service)) -> dict:
    return {"data": service.overview().model_dump(mode="json")}

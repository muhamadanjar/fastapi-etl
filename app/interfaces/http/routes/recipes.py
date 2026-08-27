from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status

from app.application.services.workspace_service import WorkspaceService
from app.core.workspace_errors import WorkspaceError
from app.infrastructure.db.models.workspace import ResourceStatus
from app.interfaces.workspace_dependencies import get_actor_id, get_workspace_service
from app.schemas.workspace import (
    DataRecipeCreate,
    DataRecipeRead,
    DataRecipeUpdate,
    ListEnvelope,
    ObjectEnvelope,
    RecipeRunCreate,
    RecipeValidationRead,
    RunRead,
)


router = APIRouter(prefix="/recipes", tags=["Data Recipes"])


def _enqueue_transformation(run_id: UUID) -> str:
    from app.tasks.run_tasks import execute_transformation_run

    return execute_transformation_run.delay(str(run_id)).id


@router.post("", status_code=status.HTTP_201_CREATED, response_model=ObjectEnvelope[DataRecipeRead])
def create_recipe(
    body: DataRecipeCreate,
    actor_id: str = Depends(get_actor_id),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {"data": service.create_recipe(body, actor_id).model_dump(mode="json")}


@router.get("", response_model=ListEnvelope[DataRecipeRead])
def list_recipes(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    resource_status: Optional[ResourceStatus] = Query(None, alias="status"),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    rows, meta = service.list_recipes(page, page_size, resource_status)
    return {
        "data": [row.model_dump(mode="json") for row in rows],
        "metas": meta.model_dump(),
    }


@router.get("/{recipe_id}", response_model=ObjectEnvelope[DataRecipeRead])
def get_recipe(
    recipe_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {"data": service.get_recipe(recipe_id).model_dump(mode="json")}


@router.patch("/{recipe_id}", response_model=ObjectEnvelope[DataRecipeRead])
def update_recipe(
    recipe_id: UUID,
    body: DataRecipeUpdate,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {"data": service.update_recipe(recipe_id, body).model_dump(mode="json")}


@router.delete("/{recipe_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_recipe(
    recipe_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> Response:
    service.delete_recipe(recipe_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/{recipe_id}/activate", response_model=ObjectEnvelope[DataRecipeRead])
def activate_recipe(
    recipe_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {
        "data": service.set_recipe_status(recipe_id, ResourceStatus.ACTIVE).model_dump(mode="json")
    }


@router.post("/{recipe_id}/deactivate", response_model=ObjectEnvelope[DataRecipeRead])
def deactivate_recipe(
    recipe_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {
        "data": service.set_recipe_status(recipe_id, ResourceStatus.INACTIVE).model_dump(mode="json")
    }


@router.post(
    "/{recipe_id}/clone",
    status_code=status.HTTP_201_CREATED,
    response_model=ObjectEnvelope[DataRecipeRead],
)
def clone_recipe(
    recipe_id: UUID,
    actor_id: str = Depends(get_actor_id),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {"data": service.clone_recipe(recipe_id, actor_id).model_dump(mode="json")}


@router.post("/{recipe_id}/validate", response_model=ObjectEnvelope[RecipeValidationRead])
def validate_recipe(
    recipe_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {"data": service.validate_recipe(recipe_id).model_dump(mode="json")}


@router.post(
    "/{recipe_id}/runs",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ObjectEnvelope[RunRead],
)
def run_recipe(
    recipe_id: UUID,
    body: RecipeRunCreate,
    actor_id: str = Depends(get_actor_id),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    run = service.create_transformation_run(recipe_id, body, actor_id)
    try:
        run = service.set_run_task_id(run.id, _enqueue_transformation(run.id))
    except Exception as exc:
        service.fail_pending_run(run.id, "ENQUEUE_FAILED", "Worker queue is unavailable")
        raise WorkspaceError(
            "WORKER_UNAVAILABLE", "Worker queue is unavailable", status_code=503
        ) from exc
    return {"data": run.model_dump(mode="json")}

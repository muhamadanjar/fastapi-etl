import csv
import io
import json
import math
from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status
from fastapi.responses import StreamingResponse

from app.application.services.workspace_service import WorkspaceService
from app.infrastructure.db.models.workspace import DatasetKind
from app.interfaces.workspace_dependencies import get_workspace_service
from app.schemas.workspace import DatasetRead, ListEnvelope, ObjectEnvelope


router = APIRouter(prefix="/datasets", tags=["Datasets"])


@router.get("", response_model=ListEnvelope[DatasetRead])
def list_datasets(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    kind: Optional[DatasetKind] = None,
    source_id: Optional[UUID] = None,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    rows, meta = service.list_datasets(page, page_size, kind, source_id)
    return {
        "data": [row.model_dump(mode="json", by_alias=True) for row in rows],
        "metas": meta.model_dump(),
    }


@router.get("/{dataset_id}", response_model=ObjectEnvelope[DatasetRead])
def get_dataset(
    dataset_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {"data": service.get_dataset(dataset_id).model_dump(mode="json", by_alias=True)}


@router.get("/{dataset_id}/records", response_model=ListEnvelope[Dict[str, Any]])
def get_dataset_records(
    dataset_id: UUID,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    rows, meta = service.dataset_records(dataset_id, page, page_size)
    return {"data": rows, "metas": meta.model_dump()}


@router.get("/{dataset_id}/lineage", response_model=ObjectEnvelope[Dict[str, Any]])
def get_dataset_lineage(
    dataset_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict:
    return {"data": service.dataset_lineage(dataset_id)}


@router.get("/{dataset_id}/download")
def download_dataset(
    dataset_id: UUID,
    output_format: str = Query("json", alias="format", pattern="^(json|csv)$"),
    service: WorkspaceService = Depends(get_workspace_service),
) -> StreamingResponse:
    dataset = service.get_dataset(dataset_id)
    total_pages = math.ceil(dataset.record_count / 100)
    if output_format == "json":
        def content():
            yield "["
            first = True
            for page in range(1, total_pages + 1):
                batch, _ = service.dataset_records(dataset_id, page, 100)
                for record in batch:
                    if not first:
                        yield ","
                    yield json.dumps(record, ensure_ascii=False, default=str)
                    first = False
            yield "]"

        media_type = "application/json"
    else:
        columns = [column["name"] for column in dataset.dataset_schema.get("columns", [])]

        def content():
            buffer = io.StringIO()
            writer = csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            yield buffer.getvalue()
            for page in range(1, total_pages + 1):
                batch, _ = service.dataset_records(dataset_id, page, 100)
                for record in batch:
                    buffer.seek(0)
                    buffer.truncate(0)
                    writer.writerow(record)
                    yield buffer.getvalue()

        media_type = "text/csv"
    filename = f"dataset-{dataset_id}.{output_format}"
    return StreamingResponse(
        content(),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_dataset(
    dataset_id: UUID,
    service: WorkspaceService = Depends(get_workspace_service),
) -> Response:
    service.delete_dataset(dataset_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

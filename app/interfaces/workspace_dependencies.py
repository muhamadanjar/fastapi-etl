from typing import Generator

from fastapi import Depends, Request
from sqlmodel import Session

from app.application.services.workspace_service import WorkspaceService
from app.core.workspace_errors import WorkspaceError
from app.infrastructure.db.manager import get_session_dependency


def get_actor_id(request: Request) -> str:
    principal = getattr(request.state, "current_user", None)
    actor_id = getattr(principal, "id", None)
    if not actor_id:
        raise WorkspaceError(
            "AUTH_CONTEXT_MISSING",
            "Authorized principal is unavailable",
            status_code=401,
        )
    return str(actor_id)


def get_workspace_service(
    session: Session = Depends(get_session_dependency),
) -> Generator[WorkspaceService, None, None]:
    yield WorkspaceService(session)

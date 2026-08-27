from __future__ import annotations

from typing import Any, Dict


class WorkspaceError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 400,
        details: Dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}


def not_found(resource: str) -> WorkspaceError:
    return WorkspaceError(
        "NOT_FOUND",
        f"{resource} not found",
        status_code=404,
    )

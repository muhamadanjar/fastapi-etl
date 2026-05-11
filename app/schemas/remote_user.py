from __future__ import annotations
from typing import Any, List, Optional
from pydantic import BaseModel


class RemoteUserInfo(BaseModel):
    id: str
    username: str
    email: str
    is_active: bool
    is_superuser: bool
    name: str
    roles: List[Any] = []
    privileges: List[Any] = []
    full_name: Optional[str] = None

    model_config = {"extra": "allow"}

from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from app.domain.value_objects.email import Email


@dataclass
class User:
    id: str
    name: str
    email: Email
    password: str
    is_active: bool = True
    is_superuser: bool = False
    is_verified: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None

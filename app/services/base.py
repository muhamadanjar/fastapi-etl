from typing import Any
from sqlmodel import Session

class BaseService:
    def __init__(self, session: Session):
        self.session = session
        
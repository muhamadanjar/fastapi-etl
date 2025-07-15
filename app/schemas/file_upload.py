from pydantic import BaseModel
from typing import Optional

class FileRegistryCreate(BaseModel):
    filename: str
    status: str

class FileRegistryRead(FileRegistryCreate):
    id: int
    upload_date: str

    class Config:
        from_attributes = True
        
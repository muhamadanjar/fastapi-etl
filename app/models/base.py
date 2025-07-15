
from sqlmodel import SQLModel

class BaseModel(SQLModel):
    class Config:
        arbitrary_types_allowed = True
        orm_mode = True
from pydantic import BaseModel as PydanticBaseModel

class BaseSchema(PydanticBaseModel):
    class Config:
        orm_mode = True
        
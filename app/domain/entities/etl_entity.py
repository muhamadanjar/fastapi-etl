from pydantic import BaseModel

class ETLInput(BaseModel):
    source: str
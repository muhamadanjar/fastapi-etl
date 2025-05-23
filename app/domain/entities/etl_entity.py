from pydantic import BaseModel
from dataclasses import dataclass

class ETLInput(BaseModel):
    source: str


@dataclass
class ETLEntity:
    id: str
    name: str
    description: str
    input: ETLInput
    output: str
    status: str
    created_at: str
    updated_at: str

    def __init__(self, id: str, name: str, description: str, input: ETLInput, output: str, status: str, created_at: str, updated_at: str):
        self.id = id
        self.name = name
        self.description = description
        self.input = input
        self.output = output
        self.status = status
        self.created_at = created_at
        self.updated_at = updated_at
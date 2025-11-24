from pydantic import BaseModel, Field
from typing import Generic, Optional, TypeVar, Any

# Use Any instead of SQLModel for more flexibility
DataType = TypeVar("DataType")

class APIResponse(BaseModel, Generic[DataType]):
    """Standard API response wrapper"""
    
    message: str = Field(..., description="Response message")
    data: Optional[DataType] = Field(default=None, description="Response data")
    success: bool = Field(default=True, description="Success status")
    
    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def success(message: str = "Success", data: Optional[DataType] = None) -> "APIResponse[DataType]":
        """Create a success response"""
        return APIResponse(message=message, data=data, success=True)

    @staticmethod
    def error(message: str = "Error", data: Optional[DataType] = None) -> "APIResponse[DataType]":
        """Create an error response"""
        return APIResponse(message=message, data=data, success=False)

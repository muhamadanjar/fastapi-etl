from typing import Generic, TypeVar, Optional, List, Any, Dict
from datetime import datetime
from pydantic import BaseModel, Field

T = TypeVar('T')


class BaseResponse(BaseModel):
    """Base response schema for all API responses."""
    success: bool = True
    message: str = "Operation completed successfully"
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class PaginatedResponse(BaseModel, Generic[T]):
    """Paginated response schema."""
    items: List[T]
    total: int
    page: int = Field(ge=1, description="Current page number")
    size: int = Field(ge=1, le=100, description="Number of items per page")
    pages: int = Field(ge=1, description="Total number of pages")
    has_next: bool = Field(description="Whether there are more pages")
    has_prev: bool = Field(description="Whether there are previous pages")


class ErrorResponse(BaseResponse):
    """Error response schema."""
    success: bool = False
    error_code: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    

class MetadataSchema(BaseModel):
    """Schema for metadata fields."""
    key: str
    value: Any
    type: str = Field(description="Type of the metadata value")


class FilterSchema(BaseModel):
    """Schema for filtering options."""
    field: str
    operator: str = Field(description="Comparison operator: eq, ne, gt, lt, gte, lte, like, in")
    value: Any
    

class SortSchema(BaseModel):
    """Schema for sorting options."""
    field: str
    direction: str = Field(default="asc", regex="^(asc|desc)$")


class SearchSchema(BaseModel):
    """Schema for search requests."""
    query: Optional[str] = None
    filters: Optional[List[FilterSchema]] = None
    sort: Optional[List[SortSchema]] = None
    page: int = Field(default=1, ge=1)
    size: int = Field(default=10, ge=1, le=100)


class BulkOperationSchema(BaseModel):
    """Schema for bulk operations."""
    operation: str = Field(description="Type of bulk operation: create, update, delete")
    items: List[Dict[str, Any]]
    

class BulkOperationResponse(BaseResponse):
    """Response schema for bulk operations."""
    total_processed: int
    successful: int
    failed: int
    errors: Optional[List[Dict[str, Any]]] = None


class HealthCheckSchema(BaseModel):
    """Schema for health check response."""
    status: str = Field(description="Health status: healthy, unhealthy, degraded")
    version: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    services: Dict[str, str] = Field(description="Status of dependent services")
    uptime: float = Field(description="Uptime in seconds")


class ValidationErrorDetail(BaseModel):
    """Schema for validation error details."""
    field: str
    message: str
    value: Any = None
    

class ApiKeySchema(BaseModel):
    """Schema for API key information."""
    key_id: str
    name: str
    created_at: datetime
    expires_at: Optional[datetime] = None
    is_active: bool = True
    permissions: List[str] = []
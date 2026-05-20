"""
API Response Usage Examples
===========================

Standardized response format for all endpoints:
{
    "status": "success" | "error",
    "data": <list or dict>,
    "metas": <dict with metadata>,
    "message": <string>
}

Metas can contain: page, page_size, total, total_pages, time_elapsed, etc
"""

from app.core.response import APIResponse, ResponseBuilder

# ============= SIMPLE RESPONSES =============

# Success without data
def example_simple_success():
    return APIResponse.success(message="User created successfully")
    # {"status": "success", "message": "User created successfully", "data": null, "metas": {}}

# Success with data (dict)
def example_with_dict_data():
    user_data = {"id": 1, "name": "John", "email": "john@example.com"}
    return APIResponse.success(
        message="User retrieved",
        data=user_data
    )
    # {"status": "success", "message": "User retrieved", "data": {...}, "metas": {}}

# Success with data (list)
def example_with_list_data():
    users = [
        {"id": 1, "name": "John"},
        {"id": 2, "name": "Jane"}
    ]
    return APIResponse.success(
        message="Users retrieved",
        data=users
    )
    # {"status": "success", "message": "Users retrieved", "data": [...], "metas": {}}

# Error response
def example_error():
    return APIResponse.error(message="User not found")
    # {"status": "error", "message": "User not found", "data": null, "metas": {}}

# ============= WITH METAS (PAGINATION) =============

def example_with_pagination():
    users = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]

    return APIResponse.success(
        message="Users list",
        data=users,
        metas={
            "page": 1,
            "page_size": 10,
            "total": 50,
            "total_pages": 5
        }
    )
    # {
    #     "status": "success",
    #     "message": "Users list",
    #     "data": [...],
    #     "metas": {"page": 1, "page_size": 10, "total": 50, "total_pages": 5}
    # }

# ============= WITH METAS (CUSTOM) =============

def example_with_custom_metas():
    job_data = {"id": "job_123", "status": "processing"}

    return APIResponse.success(
        message="ETL job started",
        data=job_data,
        metas={
            "queue": "etl_high_priority",
            "estimated_duration_seconds": 300,
            "worker_id": "worker_02"
        }
    )

# ============= USING RESPONSE BUILDER (RECOMMENDED FOR COMPLEX LOGIC) =============

def example_builder_simple():
    return ResponseBuilder()\
        .with_message("Data processed")\
        .with_data({"processed_count": 150})\
        .build()
    # Auto-includes time_elapsed in metas

def example_builder_with_pagination():
    users = [{"id": 1, "name": "John"}]
    page, page_size, total = 1, 10, 100

    return ResponseBuilder()\
        .with_message("Users retrieved")\
        .with_data(users)\
        .with_pagination(page=page, page_size=page_size, total=total)\
        .build()
    # Auto-includes: page, page_size, total, total_pages, time_elapsed

def example_builder_with_custom_metas():
    return ResponseBuilder()\
        .with_message("ETL job created")\
        .with_data({"job_id": "job_123"})\
        .add_meta("queue_name", "etl_jobs")\
        .add_meta("priority", "high")\
        .add_meta("scheduled_at", "2026-05-20T10:30:00Z")\
        .build()

def example_builder_error():
    return ResponseBuilder()\
        .with_status("error")\
        .with_message("Invalid job configuration")\
        .add_meta("invalid_fields", ["job_name", "source_type"])\
        .build()

# ============= IN FASTAPI ENDPOINTS =============

from fastapi import APIRouter, HTTPException
from typing import List, Optional

router = APIRouter(prefix="/api/v1", tags=["users"])

@router.get("/users/{user_id}")
async def get_user(user_id: int):
    """Get single user"""
    user = {"id": user_id, "name": "John Doe", "email": "john@example.com"}
    return APIResponse.success(
        message="User retrieved successfully",
        data=user
    )

@router.get("/users")
async def list_users(page: int = 1, page_size: int = 10):
    """List users with pagination"""
    users = [{"id": i, "name": f"User {i}"} for i in range(1, 11)]
    total = 100

    return ResponseBuilder()\
        .with_message("Users list retrieved")\
        .with_data(users)\
        .with_pagination(page=page, page_size=page_size, total=total)\
        .build()

@router.post("/users")
async def create_user(name: str, email: str):
    """Create new user"""
    new_user = {"id": 1, "name": name, "email": email}

    return ResponseBuilder()\
        .with_message("User created successfully")\
        .with_data(new_user)\
        .add_meta("created_at", "2026-05-20T10:30:00Z")\
        .build()

@router.delete("/users/{user_id}")
async def delete_user(user_id: int):
    """Delete user"""
    return ResponseBuilder()\
        .with_message("User deleted successfully")\
        .add_meta("deleted_id", user_id)\
        .build()

@router.post("/jobs")
async def create_job(job_name: str):
    """Create ETL job"""
    if not job_name:
        return ResponseBuilder()\
            .with_status("error")\
            .with_message("job_name is required")\
            .add_meta("required_fields", ["job_name"])\
            .build()

    job = {"id": "job_123", "name": job_name, "status": "pending"}
    return ResponseBuilder()\
        .with_message("ETL job created")\
        .with_data(job)\
        .add_meta("queue", "etl_jobs")\
        .build()

# ============= COPY THIS RESPONSE.PY TO OTHER SERVICES =============

"""
To use this standardized format in other services:

1. Copy app/core/response.py to:
   - payment_api/app/core/response.py
   - tileserver_api/app/core/response.py
   - usermanagement_api/app/core/response.py

2. Import in endpoints:
   from app.core.response import APIResponse, ResponseBuilder

3. Use in FastAPI route handlers:
   @app.post("/endpoint")
   def my_endpoint():
       return APIResponse.success(message="...", data={...})
       # or
       return ResponseBuilder()\
           .with_message("...")\
           .with_data({...})\
           .with_pagination(page=1, page_size=10, total=100)\
           .build()

4. Response format is guaranteed:
   {
       "status": "success|error",
       "data": null|dict|list,
       "metas": {...},
       "message": "..."
   }
"""

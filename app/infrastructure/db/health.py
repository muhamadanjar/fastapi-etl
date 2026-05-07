from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from app.infrastructure.db.manager import database_manager

router = APIRouter(prefix="/health", tags=["Health"])


@router.get("/db")
async def check_database_health():
    """Check database connection health."""
    try:
        is_healthy = await database_manager.health_check()
        if is_healthy:
            return JSONResponse(
                content={"status": "healthy", "database": "default"},
                status_code=status.HTTP_200_OK,
            )
        return JSONResponse(
            content={"status": "unhealthy", "database": "default"},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    except Exception as e:
        return JSONResponse(
            content={"status": "unhealthy", "error": str(e), "database": "default"},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

from fastapi import APIRouter

from .auth import router as auth_router
from .files import router as files_router
from .transformation import router as transform_router
from .jobs import router as jobs_router
from .monitoring import router as monitoring_router
from .data_quality import router as data_quality_router
from .entities import router as entities_router
# from .reports import router as reports_router

api_router = APIRouter()

# Include all routers with prefixes
api_router.include_router(auth_router, prefix="/auth", tags=["Authentication"])
api_router.include_router(files_router, prefix="/files", tags=["File Management"])
api_router.include_router(transform_router, prefix="/transform", tags=["Transformation"])
api_router.include_router(jobs_router, prefix="/jobs", tags=["ETL Jobs"])
api_router.include_router(monitoring_router, prefix="/monitoring", tags=["Monitoring"])
api_router.include_router(data_quality_router, prefix="/data-quality", tags=["Data Quality"])
api_router.include_router(entities_router, prefix="/entities", tags=["Entity Management"])
# api_router.include_router(reports_router, prefix="/reports", tags=["Reports"])
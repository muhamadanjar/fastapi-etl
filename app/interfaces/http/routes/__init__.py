from fastapi import APIRouter

from .auth import router as auth_router
from .files import router as files_router
from .transformation import router as transform_router
from .jobs import router as jobs_router
from .monitoring import router as monitoring_router
from .data_quality import router as data_quality_router
from .dependencies import router as dependencies_router
from .entities import router as entities_router
from .errors import router as errors_router
from .rejected_records import router as rejected_records_router
from .metrics import router as metrics_router
# from .reports import router as reports_router

api_router = APIRouter()

# Include all routers with prefixes
api_router.include_router(auth_router, prefix="/auth", tags=["Authentication"])
api_router.include_router(files_router, prefix="/files", tags=["File Management"])
# api_router.include_router(transform_router, prefix="/transform", tags=["Transformation"])
api_router.include_router(jobs_router, prefix="/jobs", tags=["ETL Jobs"])
api_router.include_router(dependencies_router, tags=["Job Dependencies"])  # No prefix, uses /jobs from router
api_router.include_router(errors_router, tags=["Error Management"])
api_router.include_router(monitoring_router, prefix="/monitoring", tags=["Monitoring"])
api_router.include_router(data_quality_router, prefix="/data-quality", tags=["Data Quality"])
api_router.include_router(entities_router, prefix="/entities", tags=["Entity Management"])
api_router.include_router(rejected_records_router, prefix="/rejected-records", tags=["Rejected Records"])
api_router.include_router(metrics_router, tags=["Performance Metrics"])  # No prefix, uses /metrics from router
# api_router.include_router(reports_router, prefix="/reports", tags=["Reports"])
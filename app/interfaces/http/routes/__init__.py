from fastapi import APIRouter

from .datasets import router as datasets_router
from .overview import router as overview_router
from .recipes import router as recipes_router
from .runs import router as runs_router
from .sources import router as sources_router


api_router = APIRouter(prefix="/api/v1")
api_router.include_router(overview_router)
api_router.include_router(sources_router)
api_router.include_router(datasets_router)
api_router.include_router(recipes_router)
api_router.include_router(runs_router)

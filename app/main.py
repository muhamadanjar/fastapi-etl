from contextlib import asynccontextmanager
from typing import AsyncGenerator

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.core.exceptions import AppException
from app.core.logging import setup_logging
from app.core.workspace_errors import WorkspaceError
from app.infrastructure.db.manager import database_manager
from app.interfaces.http.middleware.auth import AuthMiddleware
from app.interfaces.http.middleware.logging import LoggingMiddleware
from app.interfaces.http.routes import api_router


settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    setup_logging()
    await database_manager.connect()
    try:
        yield
    finally:
        await database_manager.disconnect()


def create_application() -> FastAPI:
    application = FastAPI(
        title="ETL API",
        description="MVP service for raw ingestion and declarative dataset composition",
        version=settings.VERSION,
        debug=settings.DEBUG,
        docs_url="/docs" if settings.DEBUG else None,
        redoc_url="/redoc" if settings.DEBUG else None,
        openapi_url="/openapi.json" if settings.DEBUG else None,
        lifespan=lifespan,
    )

    application.add_middleware(LoggingMiddleware)
    application.add_middleware(AuthMiddleware)
    if settings.cors_settings.allowed_origins:
        application.add_middleware(
            CORSMiddleware,
            allow_origins=[str(origin) for origin in settings.cors_settings.allowed_origins],
            allow_credentials=settings.cors_settings.allow_credentials,
            allow_methods=settings.cors_settings.allowed_methods,
            allow_headers=settings.cors_settings.allowed_headers,
        )

    @application.exception_handler(WorkspaceError)
    async def workspace_exception_handler(
        request: Request, exc: WorkspaceError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    @application.exception_handler(AppException)
    async def app_exception_handler(request: Request, exc: AppException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.error_code,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    @application.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "Request validation failed",
                    "details": {"errors": jsonable_encoder(exc.errors())},
                }
            },
        )

    @application.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": "HTTP_ERROR",
                    "message": str(exc.detail),
                    "details": {},
                }
            },
        )

    @application.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "INTERNAL_SERVER_ERROR",
                    "message": "Internal server error",
                    "details": {},
                }
            },
        )

    @application.get("/health")
    async def health_check() -> dict:
        return {"status": "healthy", "version": settings.VERSION}

    application.include_router(api_router)
    return application


app = create_application()


def main() -> None:
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
    )


if __name__ == "__main__":
    main()

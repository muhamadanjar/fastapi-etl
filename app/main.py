from contextlib import asynccontextmanager
from typing import AsyncGenerator

from aio_pika import ExchangeType
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse

from app.core.logging import setup_logging
from app.core.config import get_settings
from app.core.exceptions import AppException
from app.infrastructure.db.manager import database_manager
from app.infrastructure.cache import redis_manager
from app.infrastructure.cache import cache_manager
from app.infrastructure.messaging import messaging_manager, Message
from app.infrastructure.messaging.decorators import message_handler, publish_event
from app.infrastructure.messaging.manager import MessagingType
from app.interfaces.http.middleware.logging import LoggingMiddleware
from app.interfaces.http.middleware.rate_limit import RateLimitMiddleware, RateLimitConfig
from app.interfaces.http.routes import api_router as app_routes
import uvicorn


settings = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Startup
    print("🚀 Starting application...")

    messaging_initialized = False
    cache_mode = None

    try:
        setup_logging()

        # Initialize database
        await database_manager.connect()
        print("✅ Database connection established")

        # Initialize Redis
        try:
            await redis_manager.connect()
            redis_cache = redis_manager.get_cache()
            cache_mode = "redis"

            # Initialize cache manager with Redis + Memory fallback
            await cache_manager.initialize(
                primary_cache=redis_cache,
                enable_fallback=True,
                fallback_config={
                    'max_size': 1000,
                    'max_memory_mb': 50,
                    'default_ttl': 300,
                    'eviction_policy': 'lru'
                },
                recovery_interval=60
            )
            print("✅ Cache system initialized (Redis + Memory fallback)")
        except Exception as e:
            print(f"⚠️  Redis unavailable: {e}")
            cache_mode = "memory"

            # Initialize with memory cache only
            await cache_manager.initialize(
                primary_cache=None,
                enable_fallback=True,
                fallback_config={
                    'max_size': 500,
                    'max_memory_mb': 25,
                    'default_ttl': 300
                }
            )
            print("✅ Memory cache initialized (fallback mode)")

        # Initialize messaging (environment-based config)
        try:
            rabbitmq_enabled = settings.rabbitmq_settings.host != "disabled"

            await messaging_manager.initialize(
                enable_redis=cache_mode == "redis",
                enable_websocket=True,
                enable_sse=True,
                enable_rabbitmq=rabbitmq_enabled,
                rabbitmq_config={
                    'host': settings.rabbitmq_settings.host,
                    'port': settings.rabbitmq_settings.port,
                    'username': settings.rabbitmq_settings.user,
                    'password': settings.rabbitmq_settings.password,
                    'vhost': settings.rabbitmq_settings.vhost,
                    'exchange_name': 'app_messages',
                    'exchange_type': ExchangeType.TOPIC
                } if rabbitmq_enabled else None,
                redis_config={
                    'host': settings.redis_settings.host,
                    'port': settings.redis_settings.port,
                    'db': settings.redis_settings.db
                } if cache_mode == "redis" else None
            )
            messaging_initialized = True
            print("✅ Messaging system initialized")
        except Exception as e:
            print(f"⚠️  Messaging initialization failed: {e}")
            # Messaging failure non-blocking - app can still run with degraded functionality
            if cache_mode == "redis":
                print("   Falling back to Redis messaging only")
            else:
                print("   Running with in-memory messaging only")

        yield

    finally:
        # Shutdown
        print("⏳ Shutting down...")

        # Shutdown messaging
        if messaging_initialized:
            try:
                await messaging_manager.shutdown()
                print("✅ Messaging system shut down")
            except Exception as e:
                print(f"⚠️  Messaging shutdown error: {e}")

        # Shutdown cache manager
        try:
            await cache_manager.shutdown()
            print("✅ Cache manager shut down")
        except Exception as e:
            print(f"⚠️  Cache shutdown error: {e}")

        # Shutdown Redis
        try:
            await redis_manager.disconnect()
            print("✅ Redis disconnected")
        except Exception as e:
            print(f"⚠️  Redis disconnect error: {e}")

        # Shutdown database
        try:
            await database_manager.disconnect()
            print("✅ Database connection closed")
        except Exception as e:
            print(f"⚠️  Database disconnect error: {e}")

def create_application() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        description="FastAPI Clean Architecture Starter",
        version=settings.VERSION,
        debug=settings.DEBUG,
        docs_url="/docs" if settings.DEBUG else None,
        redoc_url="/redoc" if settings.DEBUG else None,
        openapi_url="/openapi.json" if settings.DEBUG else None,
        lifespan=lifespan,
    )


    # Middleware is applied in LIFO order — add innermost (last to run) first,
    # outermost (first to run) last. CORS must be outermost so it handles
    # preflight OPTIONS requests before any other middleware rejects them.
    app.add_middleware(LoggingMiddleware)
    app.add_middleware(RateLimitMiddleware, config=RateLimitConfig())

    if settings.cors_settings.allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[str(origin) for origin in settings.cors_settings.allowed_origins],
            allow_credentials=settings.cors_settings.allow_credentials,
            allow_methods=settings.cors_settings.allowed_methods,
            allow_headers=settings.cors_settings.allowed_headers,
        )

    @app.exception_handler(AppException)
    async def app_exception_handler(request: Request, exc: AppException) -> JSONResponse:
        """Handle application-specific exceptions."""
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "type": exc.error_code,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        """Handle general exceptions."""
        if settings.DEBUG:
            import traceback
            return JSONResponse(
                status_code=500,
                content={
                    "error": {
                        "type": "internal_server_error",
                        "message": str(exc),
                        "traceback": traceback.format_exc(),
                    }
                },
            )
        else:
            return JSONResponse(
                status_code=500,
                content={
                    "error": {
                        "type": "internal_server_error",
                        "message": "Internal server error occurred",
                    }
                },
            )
    
    # Health check endpoint
    @app.get("/health")
    async def health_check() -> dict:
        """Health check endpoint."""
        return {
            "status": "healthy",
            "version": settings.VERSION,
            "environment": settings.ENVIRONMENT,
        }
    
    @app.get("/health/detailed")
    async def detailed_health_check() -> dict:
        """Detailed health check with dependencies status."""
        health_status = {
            "status": "healthy",
            "version": settings.VERSION,
            "environment": settings.ENVIRONMENT,
            "checks": {}
        }
        
        # Check database
        try:
            await database_manager.health_check()
            health_status["checks"]["database"] = "healthy"
        except Exception as e:
            health_status["checks"]["database"] = f"unhealthy: {str(e)}"
            health_status["status"] = "unhealthy"
        
        # Check Redis
        if settings.redis_settings.url or settings.redis_settings.host:
            try:
                await redis_manager.health_check()
                health_status["checks"]["cache"] = "healthy"
            except Exception as e:
                health_status["checks"]["cache"] = f"unhealthy: {str(e)}"
                health_status["status"] = "unhealthy"

        # Check Messaging
        try:
            messaging_health = await messaging_manager.health_check()
            health_status["checks"]["messaging"] = messaging_health.get("status", "healthy")
        except Exception as e:
            health_status["checks"]["messaging"] = f"unhealthy: {str(e)}"
            # Messaging health is non-critical for overall health

        return health_status


    # Include routers
    app.include_router(
        app_routes,
    )
    

    # @app.websocket("/ws/{client_id}")
    # async def websocket_endpoint(websocket, client_id: str):
    #     """WebSocket connection endpoint."""
    #     await websocket_manager.connect(websocket, client_id)
    
    # Root endpoint
    @app.get("/")
    async def root() -> dict:
        """Root endpoint with API information."""
        return {
            "message": "FastAPI Clean Architecture Starter",
            "version": settings.VERSION,
            "docs_url": "/docs" if settings.DEBUG else "disabled",
            "health_check": "/health",
        }
    
    return app

app = create_application()


# # WebSocket endpoint
# @app.websocket("/ws/{client_id}")
# async def websocket_endpoint(websocket: WebSocket, client_id: str):
#     ws_messaging = messaging_manager.get_messaging(MessagingType.WEBSOCKET)
#     if not ws_messaging:
#         await websocket.close(code=1011)
#         return
    
#     if await ws_messaging.connect_client(websocket, client_id):
#         try:
#             while True:
#                 message = await websocket.receive_text()
#                 await ws_messaging.handle_client_message(client_id, message)
#         except WebSocketDisconnect:
#             await ws_messaging.disconnect_client(client_id)

# # SSE endpoint
# @app.get("/events/{client_id}")
# async def sse_endpoint(request: Request, client_id: str):
#     sse_messaging = messaging_manager.get_messaging(MessagingType.SSE)
#     if not sse_messaging:
#         raise HTTPException(status_code=503, detail="SSE not available")
    
#     return await sse_messaging.create_connection(request, client_id)

# # Message handlers
# @message_handler("user.created", MessagingType.REDIS)
# async def handle_user_created(message: Message):
#     user_data = message.payload
#     print(f"New user created: {user_data}")
    
#     # Notify WebSocket clients
#     await messaging_manager.publish(
#         topic="notifications",
#         payload={"type": "user_created", "user": user_data},
#         messaging_type=MessagingType.WEBSOCKET
#     )

# # Publishing events
# @publish_event("user.updated", MessagingType.ALL)
# async def update_user(user_id: str, data: dict):
#     # Update user logic
#     updated_user = await user_service.update(user_id, data)
#     return updated_user  # This gets published automatically

# # Health check
# @app.get("/health/messaging")
# async def messaging_health():
#     health = await messaging_manager.health_check()
#     return health

# # Messaging stats
# @app.get("/admin/messaging/stats")
# async def messaging_stats():
#     stats = messaging_manager.get_statistics()
#     return stats

def main():
    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level="info" if not settings.DEBUG else "debug",
        access_log=True,
        server_header=False,
        date_header=False,
    )

if __name__ == "__main__":
    main()
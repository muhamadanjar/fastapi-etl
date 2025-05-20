from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1 import etl

# settings = get_settings()


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(etl.router, prefix="/api/v1/etl")
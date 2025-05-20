from fastapi import FastAPI
from app.api.v1 import etl


app = FastAPI()

app.include_router(etl.router, prefix="/api/v1/etl")
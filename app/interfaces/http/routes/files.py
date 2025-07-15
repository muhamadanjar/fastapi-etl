from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session
from app.core.database import SessionLocal
from app.services.file_service import FileService
from app.schemas.file_upload import FileRegistryCreate, FileRegistryRead

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/upload/", response_model=FileRegistryRead)
def upload_file(file_data: FileRegistryCreate, db: Session = Depends(get_db)):
    service = FileService(db)
    return service.register_file(file_data)
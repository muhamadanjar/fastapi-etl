from fastapi import APIRouter, UploadFile, File, BackgroundTasks, HTTPException
from app.infrastructure.message_broker.publisher import publish_etl_job_with_file
from app.infrastructure.db.etl_repo_impl import ETLRepositoryImpl
import shutil
import uuid
import os

UPLOAD_DIR = "/tmp/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

router = APIRouter()
repo = ETLRepositoryImpl()

@router.post("/run")
def run_etl(source: str):
    publish_etl_job_with_file(source)(source)
    return {"message": "ETL job submitted"}

@router.post("/upload")
def upload_and_run_etl(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    # Generate a unique filename to avoid collisions
    file_id = str(uuid.uuid4())
    file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")

    # Save the uploaded file to the temporary directory
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Queue a background task to process the file asynchronously
    background_tasks.add_task(publish_etl_job_with_file, file_path)
    return {"message": "ETL job with file submitted", "file": file.filename}

@router.get("/status/{job_id}")
def get_etl_status(job_id: str):
    # Check job status and result from DB (or could be Redis)
    result = repo.get_result(job_id)
    if not result:
        raise HTTPException(status_code=404, detail="Job not found")
    return result
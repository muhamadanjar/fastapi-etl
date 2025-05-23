from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.status import HTTP_400_BAD_REQUEST

class AppException(Exception):
    def __init__(self, message: str, status_code: int = HTTP_400_BAD_REQUEST):
        self.message = message
        self.status_code = status_code

def app_exception_handler(request: Request, exc: AppException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.message}
    )

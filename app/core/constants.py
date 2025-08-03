# App info
APP_NAME = "My FastAPI App"
VERSION = "1.0.0"

# JWT settings
JWT_SECRET_KEY = "your_jwt_secret_key"
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE = 60 # minutes
REFRESH_TOKEN_EXPIRE = 15 # days

# Roles
ROLE_ADMIN = "admin"
ROLE_USER = "user"

# Default pagination
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100

UPLOAD_DIRECTORY = "uploads"
ALLOWED_FILE_TYPES = [
    "image/jpeg",
    "image/png",
    "application/pdf",
    "text/csv",
    "application/json"
]
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
LOG_EXCLUDE_PATHS = [
    "/health",
    "/metrics",
    "/static",
]


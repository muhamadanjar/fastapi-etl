from fastapi import APIRouter, Depends
from app.interfaces.dependencies import get_current_user
from app.schemas.remote_user import RemoteUserInfo

router = APIRouter()


@router.get("/me", response_model=RemoteUserInfo)
async def get_current_user_info(
    current_user: RemoteUserInfo = Depends(get_current_user),
) -> RemoteUserInfo:
    """Return current authenticated user info (proxied from usermanagement_api)."""
    return current_user

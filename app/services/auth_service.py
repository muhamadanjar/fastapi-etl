from app.services.base import BaseService

class AuthService(BaseService):
    def authenticate_user(self, username: str, password: str) -> bool:
        # Dummy auth logic
        return username == "admin" and password == "secret"

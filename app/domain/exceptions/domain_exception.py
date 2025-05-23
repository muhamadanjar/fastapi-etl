class DomainException(Exception):
    """Base exception for domain errors."""
    pass


class UserAlreadyExists(DomainException):
    def __init__(self, email: str):
        super().__init__(f"User with email '{email}' already exists.")


class InvalidUserData(DomainException):
    def __init__(self, reason: str):
        super().__init__(f"Invalid user data: {reason}")

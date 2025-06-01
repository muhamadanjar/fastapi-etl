class DomainException(Exception):
    """Base exception for domain errors."""
    pass


class UserAlreadyExists(DomainException):
    def __init__(self, email: str):
        super().__init__(f"User with email '{email}' already exists.")


class InvalidUserData(DomainException):
    def __init__(self, reason: str):
        super().__init__(f"Invalid user data: {reason}")


class UserNotFoundError(DomainException):
    """Raised when user is not found."""
    
    def __init__(self, message: str = "User not found"):
        self.message = message
        super().__init__(self.message)


class InvalidCredentialsError(DomainException):
    """Raised when user credentials are invalid."""
    
    def __init__(self, message: str = "Invalid credentials"):
        self.message = message
        super().__init__(self.message)


class UserInactiveError(DomainException):
    """Raised when trying to perform actions on inactive user."""
    
    def __init__(self, message: str = "User account is inactive"):
        self.message = message
        super().__init__(self.message)


class UserNotVerifiedError(DomainException):
    """Raised when trying to perform actions requiring verified user."""
    
    def __init__(self, message: str = "User email is not verified"):
        self.message = message
        super().__init__(self.message)


class UserPermissionError(DomainException):
    """Raised when user doesn't have required permissions."""
    
    def __init__(self, message: str = "Insufficient permissions"):
        self.message = message
        super().__init__(self.message)
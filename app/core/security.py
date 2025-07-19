import re
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union

from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

from .config import get_settings
from .exceptions import UnauthorizedError, ValidationException

settings = get_settings()


# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class TokenData(BaseModel):
    """Token data model."""
    
    user_id: Optional[str] = None
    username: Optional[str] = None
    email: Optional[str] = None
    scopes: list[str] = []
    token_type: str = "access"
    exp: Optional[datetime] = None
    iat: Optional[datetime] = None
    sub: Optional[str] = None

class PasswordPolicy:
    """Password policy validator."""
    
    @staticmethod
    def validate_password(password: str) -> bool:
        """
        Validate password against policy requirements.
        
        Args:
            password: Password to validate
            
        Returns:
            True if password meets all requirements
            
        Raises:
            ValidationException: If password doesn't meet requirements
        """
        errors = []
        
        # Check minimum length
        if len(password) < settings.security.password_min_length:
            errors.append(f"Password must be at least {settings.security.password_min_length} characters long")
        
        # Check for uppercase letters
        if settings.security.password_require_uppercase and not re.search(r'[A-Z]', password):
            errors.append("Password must contain at least one uppercase letter")
        
        # Check for lowercase letters
        if settings.security.password_require_lowercase and not re.search(r'[a-z]', password):
            errors.append("Password must contain at least one lowercase letter")
        
        # Check for numbers
        if settings.security.password_require_numbers and not re.search(r'\d', password):
            errors.append("Password must contain at least one number")
        
        # Check for special characters
        if settings.security.password_require_special and not re.search(r'[!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>\/?]', password):
            errors.append("Password must contain at least one special character")
        
        if errors:
            raise ValidationException(
                message="Password does not meet policy requirements",
                details={"requirements": errors}
            )
        
        return True
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """
        Verify a password against its hash.
        
        Args:
            plain_password: Plain text password
            hashed_password: Hashed password to verify against
            
        Returns:
            True if password matches hash
        """
        return pwd_context.verify(plain_password, hashed_password)
    
    def get_password_hash(self, password: str) -> str:
        """
        Hash a password using bcrypt.
        
        Args:
            password: Plain text password to hash
            
        Returns:
            Hashed password
        """
        self.validate_password(password)
        return pwd_context.hash(password)
    

def create_access_token(
    data: Dict[str, Any],
    expires_delta: Optional[timedelta] = None,
    scopes: Optional[list[str]] = None,
) -> str:
    """
    Create a JWT access token.
    
    Args:
        data: Data to encode in the token
        expires_delta: Custom expiration time
        scopes: List of scopes for the token
        
    Returns:
        Encoded JWT token
    """
    to_encode = data.copy()
    
    # Set expiration time
    if expires_delta:
        expire = datetime.now() + expires_delta
    else:
        expire = datetime.now() + timedelta(
            minutes=settings.security.access_token_expire
        )
    
    # Add standard claims
    to_encode.update({
        "exp": expire,
        "iat": datetime.now(),
        "token_type": "access",
        "scopes": scopes or [],
    })
    
    # Ensure 'sub' claim is present
    if "sub" not in to_encode and "user_id" in to_encode:
        to_encode["sub"] = str(to_encode["user_id"])
    
    encoded_jwt = jwt.encode(
        to_encode,
        settings.security.secret_key,
        algorithm=settings.security.algorithm
    )
    
    return encoded_jwt


def create_refresh_token(
    data: Dict[str, Any],
    expires_delta: Optional[timedelta] = None,
) -> str:
    """
    Create a JWT refresh token.
    
    Args:
        data: Data to encode in the token
        expires_delta: Custom expiration time
        
    Returns:
        Encoded JWT refresh token
    """
    to_encode = data.copy()
    
    # Set expiration time
    if expires_delta:
        expire = datetime.now() + expires_delta
    else:
        expire = datetime.now() + timedelta(
            days=settings.security.refresh_token_expire_minutes
        )
    
    # Add standard claims
    to_encode.update({
        "exp": expire,
        "iat": datetime.now(),
        "token_type": "refresh",
    })
    
    # Ensure 'sub' claim is present
    if "sub" not in to_encode and "user_id" in to_encode:
        to_encode["sub"] = str(to_encode["user_id"])
    
    encoded_jwt = jwt.encode(
        to_encode,
        settings.security.secret_key,
        algorithm=settings.security.algorithm
    )
    
    return encoded_jwt


def verify_token(token: str, token_type: str = "access") -> TokenData:
    """
    Verify and decode a JWT token.
    
    Args:
        token: JWT token to verify
        token_type: Expected token type ('access' or 'refresh')
        
    Returns:
        Decoded token data
        
    Raises:
        UnauthorizedError: If token is invalid or expired
    """
    try:
        payload = jwt.decode(
            token,
            settings.security.secret_key,
            algorithms=[settings.security.algorithm]
        )
        
        # Check token type
        if payload.get("token_type") != token_type:
            raise UnauthorizedError(
                message=f"Invalid token type. Expected {token_type}",
                details={"expected_type": token_type, "actual_type": payload.get("token_type")}
            )
        
        # Extract user identifier
        user_id: str = payload.get("sub")
        if user_id is None:
            raise UnauthorizedError(
                message="Token missing user identifier",
                details={"missing_claim": "sub"}
            )
        
        token_data = TokenData(
            user_id=user_id,
            username=payload.get("username"),
            email=payload.get("email"),
            scopes=payload.get("scopes", []),
            token_type=payload.get("token_type", "access"),
            exp=datetime.fromtimestamp(payload.get("exp", 0)) if payload.get("exp") else None,
            iat=datetime.fromtimestamp(payload.get("iat", 0)) if payload.get("iat") else None,
            sub=user_id,
        )
        
        return token_data
    except JWTError as e:
        raise UnauthorizedError(
            message="Could not validate credentials",
            details={"jwt_error": str(e)}
        )
def decode_token_without_verification(token: str) -> Dict[str, Any]:
    """
    Decode JWT token without verification (for debugging purposes).
    
    Args:
        token: JWT token to decode
        
    Returns:
        Decoded token payload
        
    Warning:
        This function should only be used for debugging purposes
        as it doesn't verify the token signature.
    """
    try:
        return jwt.get_unverified_claims(token)
    except JWTError as e:
        raise UnauthorizedError(
            message="Could not decode token",
            details={"jwt_error": str(e)}
        )


def check_user_permissions(
    user_scopes: list[str],
    required_scopes: list[str],
    require_all: bool = False
) -> bool:
    """
    Check if user has required permissions.
    
    Args:
        user_scopes: List of scopes that the user has
        required_scopes: List of scopes required for the operation
        require_all: If True, user must have all required scopes.
                    If False, user must have at least one required scope.
    
    Returns:
        True if user has required permissions
    """
    if not required_scopes:
        return True
    
    if require_all:
        return all(scope in user_scopes for scope in required_scopes)
    else:
        return any(scope in user_scopes for scope in required_scopes)


def generate_api_key(length: int = 32) -> str:
    """
    Generate a secure API key.
    
    Args:
        length: Length of the API key
        
    Returns:
        Generated API key
    """
    import secrets
    import string
    
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def mask_sensitive_data(data: str, visible_chars: int = 4, mask_char: str = "*") -> str:
    """
    Mask sensitive data for logging or display purposes.
    
    Args:
        data: Sensitive data to mask
        visible_chars: Number of characters to keep visible at the end
        mask_char: Character to use for masking
        
    Returns:
        Masked data
    """
    if len(data) <= visible_chars:
        return mask_char * len(data)
    
    return mask_char * (len(data) - visible_chars) + data[-visible_chars:]


class SecurityHeaders:
    """Security headers for HTTP responses."""
    
    @staticmethod
    def get_security_headers() -> Dict[str, str]:
        """
        Get standard security headers.
        
        Returns:
            Dictionary of security headers
        """
        return {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Content-Security-Policy": "default-src 'self'",
        }    

def validate_email(email: str) -> bool:
    """
    Validate email format.
    
    Args:
        email: Email address to validate
        
    Returns:
        True if email is valid
        
    Raises:
        ValidationException: If email format is invalid
    """
    email_pattern = re.compile(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    )
    
    if not email_pattern.match(email):
        raise ValidationException(
            message="Invalid email format",
            field="email",
            value=email
        )
    
    return True

def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename to prevent directory traversal and other attacks.
    
    Args:
        filename: Filename to sanitize
        
    Returns:
        Sanitized filename
    """
    import os
    
    # Remove directory components
    filename = os.path.basename(filename)
    
    # Remove or replace dangerous characters
    dangerous_chars = ['<', '>', ':', '"', '|', '?', '*', '..', '/', '\\']
    for char in dangerous_chars:
        filename = filename.replace(char, '_')
    
    # Remove leading/trailing whitespace and dots
    filename = filename.strip(' .')
    
    # Ensure filename is not empty
    if not filename:
        filename = "unnamed_file"
    
    return filename


def rate_limit_key(identifier: str, endpoint: str) -> str:
    """
    Generate a rate limit key for Redis.
    
    Args:
        identifier: User identifier (IP, user ID, etc.)
        endpoint: API endpoint
        
    Returns:
        Rate limit key
    """
    return f"rate_limit:{identifier}:{endpoint}"
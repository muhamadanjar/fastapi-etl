"""
Security utilities for the ETL system.
Provides functions for password hashing, token generation, and security validation.
"""

import secrets
import string
import hashlib
import hmac
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Union
from passlib.context import CryptContext
from jose import JWTError, jwt
import bcrypt
import base64
import os

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT settings
JWT_SECRET_KEY = settings.security.secret_key
JWT_ALGORITHM = settings.security.algorithm
ACCESS_TOKEN_EXPIRE_MINUTES = settings.security.access_token_expire
REFRESH_TOKEN_EXPIRE_DAYS = settings.security.refresh_token_expire


def hash_password(password: str) -> str:
    """
    Hash a password using bcrypt.
    
    Args:
        password: Plain text password
        
    Returns:
        Hashed password string
    """
    try:
        return pwd_context.hash(password)
    except Exception as e:
        logger.log_error("hash_password", e)
        raise


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify a password against its hash.
    
    Args:
        plain_password: Plain text password to verify
        hashed_password: Stored hash to verify against
        
    Returns:
        True if password matches, False otherwise
    """
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception as e:
        logger.log_error("verify_password", e)
        return False


def generate_random_password(
    length: int = 12,
    include_uppercase: bool = True,
    include_lowercase: bool = True,
    include_digits: bool = True,
    include_symbols: bool = True,
    exclude_ambiguous: bool = True
) -> str:
    """
    Generate a random secure password.
    
    Args:
        length: Length of the password
        include_uppercase: Include uppercase letters
        include_lowercase: Include lowercase letters
        include_digits: Include digits
        include_symbols: Include symbols
        exclude_ambiguous: Exclude ambiguous characters (0, O, l, 1, etc.)
        
    Returns:
        Generated password string
    """
    try:
        characters = ""
        
        if include_lowercase:
            chars = string.ascii_lowercase
            if exclude_ambiguous:
                chars = chars.replace('l', '').replace('o', '')
            characters += chars
            
        if include_uppercase:
            chars = string.ascii_uppercase
            if exclude_ambiguous:
                chars = chars.replace('I', '').replace('O', '')
            characters += chars
            
        if include_digits:
            chars = string.digits
            if exclude_ambiguous:
                chars = chars.replace('0', '').replace('1', '')
            characters += chars
            
        if include_symbols:
            chars = "!@#$%^&*()-_=+[]{}|;:,.<>?"
            characters += chars
        
        if not characters:
            raise ValueError("At least one character type must be included")
        
        # Ensure at least one character from each selected type
        password = []
        
        if include_lowercase:
            password.append(secrets.choice(string.ascii_lowercase))
        if include_uppercase:
            password.append(secrets.choice(string.ascii_uppercase))
        if include_digits:
            password.append(secrets.choice(string.digits))
        if include_symbols:
            password.append(secrets.choice("!@#$%^&*()-_=+[]{}|;:,.<>?"))
        
        # Fill the rest randomly
        for _ in range(length - len(password)):
            password.append(secrets.choice(characters))
        
        # Shuffle the password
        secrets.SystemRandom().shuffle(password)
        
        return ''.join(password)
        
    except Exception as e:
        logger.log_error("generate_random_password", e)
        raise


def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Create a JWT access token.
    
    Args:
        data: Data to encode in the token
        expires_delta: Optional expiration time delta
        
    Returns:
        Encoded JWT token string
    """
    try:
        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        
        to_encode.update({"exp": expire, "type": "access"})
        
        encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        
        logger.log_operation("create_access_token", {
            "subject": data.get("sub"),
            "expires_at": expire.isoformat()
        })
        
        return encoded_jwt
        
    except Exception as e:
        logger.log_error("create_access_token", e)
        raise


def create_refresh_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Create a JWT refresh token.
    
    Args:
        data: Data to encode in the token
        expires_delta: Optional expiration time delta
        
    Returns:
        Encoded JWT refresh token string
    """
    try:
        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        
        to_encode.update({"exp": expire, "type": "refresh"})
        
        encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        
        logger.log_operation("create_refresh_token", {
            "subject": data.get("sub"),
            "expires_at": expire.isoformat()
        })
        
        return encoded_jwt
        
    except Exception as e:
        logger.log_error("create_refresh_token", e)
        raise


def decode_access_token(token: str) -> Dict[str, Any]:
    """
    Decode and validate a JWT access token.
    
    Args:
        token: JWT token string to decode
        
    Returns:
        Decoded token payload
        
    Raises:
        JWTError: If token is invalid or expired
    """
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        
        # Verify token type
        if payload.get("type") != "access":
            raise JWTError("Invalid token type")
        
        logger.log_operation("decode_access_token", {
            "subject": payload.get("sub"),
            "expires_at": payload.get("exp")
        })
        
        return payload
        
    except JWTError as e:
        logger.log_error("decode_access_token", e)
        raise
    except Exception as e:
        logger.log_error("decode_access_token", e)
        raise JWTError("Token decode error")


def decode_refresh_token(token: str) -> Dict[str, Any]:
    """
    Decode and validate a JWT refresh token.
    
    Args:
        token: JWT refresh token string to decode
        
    Returns:
        Decoded token payload
        
    Raises:
        JWTError: If token is invalid or expired
    """
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        
        # Verify token type
        if payload.get("type") != "refresh":
            raise JWTError("Invalid token type")
        
        logger.log_operation("decode_refresh_token", {
            "subject": payload.get("sub"),
            "expires_at": payload.get("exp")
        })
        
        return payload
        
    except JWTError as e:
        logger.log_error("decode_refresh_token", e)
        raise
    except Exception as e:
        logger.log_error("decode_refresh_token", e)
        raise JWTError("Token decode error")


def generate_random_token(length: int = 32, url_safe: bool = True) -> str:
    """
    Generate a cryptographically secure random token.
    
    Args:
        length: Length of the token in bytes
        url_safe: Whether to make the token URL-safe
        
    Returns:
        Random token string
    """
    try:
        random_bytes = secrets.token_bytes(length)
        
        if url_safe:
            return base64.urlsafe_b64encode(random_bytes).decode('utf-8').rstrip('=')
        else:
            return base64.b64encode(random_bytes).decode('utf-8')
            
    except Exception as e:
        logger.log_error("generate_random_token", e)
        raise


def generate_api_key(prefix: str = "etl", length: int = 32) -> str:
    """
    Generate an API key with a specific prefix.
    
    Args:
        prefix: Prefix for the API key
        length: Length of the random part
        
    Returns:
        API key string
    """
    try:
        random_part = generate_random_token(length, url_safe=True)
        api_key = f"{prefix}_{random_part}"
        
        logger.log_operation("generate_api_key", {"prefix": prefix})
        
        return api_key
        
    except Exception as e:
        logger.log_error("generate_api_key", e)
        raise


def hash_api_key(api_key: str) -> str:
    """
    Hash an API key for secure storage.
    
    Args:
        api_key: API key to hash
        
    Returns:
        Hashed API key
    """
    try:
        return hashlib.sha256(api_key.encode('utf-8')).hexdigest()
    except Exception as e:
        logger.log_error("hash_api_key", e)
        raise


def verify_api_key(api_key: str, hashed_key: str) -> bool:
    """
    Verify an API key against its hash.
    
    Args:
        api_key: API key to verify
        hashed_key: Stored hash to verify against
        
    Returns:
        True if API key matches, False otherwise
    """
    try:
        computed_hash = hash_api_key(api_key)
        return hmac.compare_digest(computed_hash, hashed_key)
    except Exception as e:
        logger.log_error("verify_api_key", e)
        return False


def create_session_token(user_id: int, session_data: Optional[Dict[str, Any]] = None) -> str:
    """
    Create a session token for user authentication.
    
    Args:
        user_id: User ID
        session_data: Optional additional session data
        
    Returns:
        Session token string
    """
    try:
        token_data = {
            "user_id": user_id,
            "session_id": generate_random_token(16),
            "created_at": datetime.utcnow().isoformat(),
            **(session_data or {})
        }
        
        return create_access_token(token_data)
        
    except Exception as e:
        logger.log_error("create_session_token", e)
        raise


def encrypt_sensitive_data(data: str, key: Optional[str] = None) -> str:
    """
    Encrypt sensitive data using AES encryption.
    
    Args:
        data: Data to encrypt
        key: Optional encryption key (uses default if not provided)
        
    Returns:
        Encrypted data as base64 string
    """
    try:
        from cryptography.fernet import Fernet
        
        if key is None:
            # Use a key from settings or generate one
            key = getattr(settings, 'ENCRYPTION_KEY', None)
            if key is None:
                # Generate a key (in production, this should be stored securely)
                key = Fernet.generate_key()
        
        if isinstance(key, str):
            key = key.encode('utf-8')
        
        f = Fernet(key)
        encrypted_data = f.encrypt(data.encode('utf-8'))
        
        return base64.b64encode(encrypted_data).decode('utf-8')
        
    except Exception as e:
        logger.log_error("encrypt_sensitive_data", e)
        raise


def decrypt_sensitive_data(encrypted_data: str, key: Optional[str] = None) -> str:
    """
    Decrypt sensitive data using AES encryption.
    
    Args:
        encrypted_data: Encrypted data as base64 string
        key: Optional decryption key (uses default if not provided)
        
    Returns:
        Decrypted data string
    """
    try:
        from cryptography.fernet import Fernet
        
        if key is None:
            key = getattr(settings, 'ENCRYPTION_KEY', None)
            if key is None:
                raise ValueError("No encryption key available")
        
        if isinstance(key, str):
            key = key.encode('utf-8')
        
        f = Fernet(key)
        encrypted_bytes = base64.b64decode(encrypted_data.encode('utf-8'))
        decrypted_data = f.decrypt(encrypted_bytes)
        
        return decrypted_data.decode('utf-8')
        
    except Exception as e:
        logger.log_error("decrypt_sensitive_data", e)
        raise


def validate_password_strength(password: str) -> Dict[str, Any]:
    """
    Validate password strength and return feedback.
    
    Args:
        password: Password to validate
        
    Returns:
        Dictionary with validation results and feedback
    """
    try:
        result = {
            "is_valid": False,
            "score": 0,
            "feedback": [],
            "requirements_met": {}
        }
        
        # Check length
        min_length = 8
        if len(password) >= min_length:
            result["requirements_met"]["min_length"] = True
            result["score"] += 20
        else:
            result["requirements_met"]["min_length"] = False
            result["feedback"].append(f"Password must be at least {min_length} characters long")
        
        # Check for uppercase
        if any(c.isupper() for c in password):
            result["requirements_met"]["uppercase"] = True
            result["score"] += 20
        else:
            result["requirements_met"]["uppercase"] = False
            result["feedback"].append("Password must contain at least one uppercase letter")
        
        # Check for lowercase
        if any(c.islower() for c in password):
            result["requirements_met"]["lowercase"] = True
            result["score"] += 20
        else:
            result["requirements_met"]["lowercase"] = False
            result["feedback"].append("Password must contain at least one lowercase letter")
        
        # Check for digits
        if any(c.isdigit() for c in password):
            result["requirements_met"]["digit"] = True
            result["score"] += 20
        else:
            result["requirements_met"]["digit"] = False
            result["feedback"].append("Password must contain at least one digit")
        
        # Check for special characters
        special_chars = "!@#$%^&*()-_=+[]{}|;:,.<>?"
        if any(c in special_chars for c in password):
            result["requirements_met"]["special_char"] = True
            result["score"] += 20
        else:
            result["requirements_met"]["special_char"] = False
            result["feedback"].append("Password must contain at least one special character")
        
        # Check for common patterns
        common_patterns = ['123', 'abc', 'password', 'admin', '000']
        if any(pattern in password.lower() for pattern in common_patterns):
            result["score"] -= 30
            result["feedback"].append("Password contains common patterns")
        
        # Determine if valid
        result["is_valid"] = all(result["requirements_met"].values()) and result["score"] >= 80
        
        return result
        
    except Exception as e:
        logger.log_error("validate_password_strength", e)
        raise


def generate_csrf_token() -> str:
    """
    Generate a CSRF token for form protection.
    
    Returns:
        CSRF token string
    """
    try:
        return generate_random_token(32, url_safe=True)
    except Exception as e:
        logger.log_error("generate_csrf_token", e)
        raise


def verify_csrf_token(token: str, stored_token: str) -> bool:
    """
    Verify a CSRF token.
    
    Args:
        token: Token to verify
        stored_token: Stored token to verify against
        
    Returns:
        True if token is valid, False otherwise
    """
    try:
        return hmac.compare_digest(token, stored_token)
    except Exception as e:
        logger.log_error("verify_csrf_token", e)
        return False


def sanitize_filename_for_security(filename: str) -> str:
    """
    Sanitize filename to prevent directory traversal attacks.
    
    Args:
        filename: Original filename
        
    Returns:
        Sanitized filename
    """
    try:
        # Remove directory separators and special characters
        import os
        import re
        
        # Get just the filename part
        filename = os.path.basename(filename)
        
        # Remove any non-alphanumeric characters except dots, dashes, and underscores
        filename = re.sub(r'[^a-zA-Z0-9._-]', '_', filename)
        
        # Remove leading dots to prevent hidden files
        filename = filename.lstrip('.')
        
        # Ensure filename is not empty
        if not filename:
            filename = "unnamed_file"
        
        # Limit length
        if len(filename) > 255:
            name, ext = os.path.splitext(filename)
            filename = name[:250] + ext
        
        return filename
        
    except Exception as e:
        logger.log_error("sanitize_filename_for_security", e)
        return "sanitized_file"


def check_rate_limit(identifier: str, max_requests: int = 100, window_seconds: int = 3600) -> Dict[str, Any]:
    """
    Simple in-memory rate limiting check.
    In production, this should use Redis or similar.
    
    Args:
        identifier: Unique identifier (IP, user ID, etc.)
        max_requests: Maximum requests allowed
        window_seconds: Time window in seconds
        
    Returns:
        Dictionary with rate limit information
    """
    try:
        import time
        
        # This is a simplified in-memory implementation
        # In production, use Redis or similar persistent storage
        if not hasattr(check_rate_limit, '_requests'):
            check_rate_limit._requests = {}
        
        current_time = time.time()
        window_start = current_time - window_seconds
        
        # Clean old entries
        if identifier in check_rate_limit._requests:
            check_rate_limit._requests[identifier] = [
                req_time for req_time in check_rate_limit._requests[identifier]
                if req_time > window_start
            ]
        else:
            check_rate_limit._requests[identifier] = []
        
        # Check current count
        current_count = len(check_rate_limit._requests[identifier])
        
        if current_count >= max_requests:
            return {
                "allowed": False,
                "current_count": current_count,
                "max_requests": max_requests,
                "reset_time": window_start + window_seconds,
                "retry_after": window_seconds
            }
        else:
            # Add current request
            check_rate_limit._requests[identifier].append(current_time)
            
            return {
                "allowed": True,
                "current_count": current_count + 1,
                "max_requests": max_requests,
                "reset_time": window_start + window_seconds,
                "remaining": max_requests - current_count - 1
            }
            
    except Exception as e:
        logger.log_error("check_rate_limit", e)
        # Allow request on error
        return {"allowed": True, "error": str(e)}
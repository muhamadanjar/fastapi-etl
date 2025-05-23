"""
Password value object representing a hashed password in the domain.

This module contains the Password value object which encapsulates
password handling logic and ensures passwords are always properly hashed.
"""

import re
from typing import Any


class Password:
    """
    Password value object representing a hashed password.
    
    This value object handles password validation and ensures that passwords
    are always stored in a hashed format, never as plain text.
    """
    
    def __init__(self, hashed_value: str):
        """
        Initialize password value object with a hashed password.
        
        Args:
            hashed_value: Already hashed password string
            
        Raises:
            ValueError: If hashed value is empty or invalid
        """
        if not hashed_value:
            raise ValueError("Password hash cannot be empty")
        
        if not self._is_valid_hash(hashed_value):
            raise ValueError("Invalid password hash format")
        
        self._hashed_value = hashed_value
    
    @property
    def value(self) -> str:
        """Get hashed password value."""
        return self._hashed_value
    
    @classmethod
    def create_from_plain_text(cls, plain_text: str) -> 'Password':
        """
        Create password from plain text (for domain testing purposes only).
        
        Note: In real implementation, hashing should be done in infrastructure layer.
        This method is provided for domain testing and validation purposes.
        
        Args:
            plain_text: Plain text password
            
        Returns:
            Password value object with validated plain text
            
        Raises:
            ValueError: If password doesn't meet requirements
        """
        cls._validate_password_strength(plain_text)
        
        # In a real implementation, this would be hashed by infrastructure
        # For domain layer, we just validate and store as-is for testing
        return cls(f"hashed_{plain_text}")
    
    @classmethod
    def _validate_password_strength(cls, password: str) -> None:
        """
        Validate password strength requirements.
        
        Args:
            password: Plain text password to validate
            
        Raises:
            ValueError: If password doesn't meet requirements
        """
        if not password:
            raise ValueError("Password cannot be empty")
        
        if len(password) < 8:
            raise ValueError("Password must be at least 8 characters long")
        
        if len(password) > 128:
            raise ValueError("Password cannot exceed 128 characters")
        
        # Check for at least one uppercase letter
        if not re.search(r'[A-Z]', password):
            raise ValueError("Password must contain at least one uppercase letter")
        
        # Check for at least one lowercase letter
        if not re.search(r'[a-z]', password):
            raise ValueError("Password must contain at least one lowercase letter")
        
        # Check for at least one digit
        if not re.search(r'\d', password):
            raise ValueError("Password must contain at least one number")
        
        # Check for at least one special character
        if not re.search(r'[!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>\/?]', password):
            raise ValueError("Password must contain at least one special character")
        
        # Check for common weak passwords
        weak_patterns = [
            r'123456',
            r'password',
            r'qwerty',
            r'abc123',
            r'letmein',
            r'welcome',
            r'monkey',
            r'dragon',
        ]
        
        password_lower = password.lower()
        for pattern in weak_patterns:
            if re.search(pattern, password_lower):
                raise ValueError("Password contains common weak patterns")
        
        # Check for repeated characters (more than 3 consecutive same characters)
        if re.search(r'(.)\1{3,}', password):
            raise ValueError("Password cannot have more than 3 consecutive identical characters")
        
        # Check for simple sequences
        sequences = ['0123', '1234', '2345', '3456', '4567', '5678', '6789', 'abcd', 'bcde', 'cdef']
        for seq in sequences:
            if seq in password_lower or seq[::-1] in password_lower:
                raise ValueError("Password cannot contain simple sequences")
    
    @staticmethod
    def _is_valid_hash(hashed_value: str) -> bool:
        """
        Validate if the string looks like a valid password hash.
        
        Args:
            hashed_value: String to validate as hash
            
        Returns:
            True if string appears to be a valid hash
        """
        # Basic validation - in real implementation this would check
        # for specific hash formats (bcrypt, scrypt, etc.)
        if len(hashed_value) < 10:
            return False
        
        # Check if it's a bcrypt hash (starts with $2a$, $2b$, $2x$, $2y$)
        bcrypt_pattern = r'^\$2[abxy]\$\d{2}\$[./A-Za-z0-9]{53}$'
        if re.match(bcrypt_pattern, hashed_value):
            return True
        
        # For testing purposes, allow hashed_ prefix
        if hashed_value.startswith('hashed_'):
            return True
        
        # Add other hash format validations as needed
        return len(hashed_value) >= 20  # Minimum reasonable hash length
    
    def is_bcrypt_hash(self) -> bool:
        """
        Check if this is a bcrypt hash.
        
        Returns:
            True if hash is in bcrypt format
        """
        bcrypt_pattern = r'^\$2[abxy]\$\d{2}\$[./A-Za-z0-9]{53}$'
        return bool(re.match(bcrypt_pattern, self._hashed_value))
    
    def get_hash_algorithm(self) -> str:
        """
        Identify the hashing algorithm used.
        
        Returns:
            Name of the hashing algorithm
        """
        if self.is_bcrypt_hash():
            return "bcrypt"
        elif self._hashed_value.startswith('hashed_'):
            return "test"  # For testing purposes
        else:
            return "unknown"
    
    def __eq__(self, other: Any) -> bool:
        """
        Compare passwords for equality.
        
        Args:
            other: Object to compare with
            
        Returns:
            True if password hashes are equal
        """
        if not isinstance(other, Password):
            return False
        return self._hashed_value == other._hashed_value
    
    def __hash__(self):
        """
        Hash password by its hashed value.
        
        Returns:
            Hash value based on hashed password
        """
        return hash(self._hashed_value)
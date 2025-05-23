import re
from typing import Any

class Email:

    # RFC 5322 compliant email regex (simplified version)
    EMAIL_REGEX = re.compile(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )

    def __init__(self, address: str):
        if not address:
            raise ValueError("Email cannot be empty")
        
        if not self._is_valid_email(address):
            raise ValueError(f"Invalid email address: {address}")
        self.address = address

    def _is_valid_email(self, email: str) -> bool:
        return re.match(r"[^@]+@[^@]+\.[^@]+", email) is not None

    def __str__(self):
        return self.address

    def __eq__(self, other):
        return isinstance(other, Email) and self.address == other.address

    def __hash__(self):
        return hash(self.address)
    
    @property
    def value(self) -> str:
        """Get email address value."""
        return self.address

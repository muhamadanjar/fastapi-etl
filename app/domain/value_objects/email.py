import re

class Email:
    def __init__(self, address: str):
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

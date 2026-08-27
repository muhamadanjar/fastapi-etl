from __future__ import annotations

import os

from cryptography.fernet import Fernet, InvalidToken


class CredentialCipherError(RuntimeError):
    pass


class CredentialCipher:
    """Encrypts API Source secrets using a dedicated environment key."""

    def __init__(self, key: str | None = None):
        value = key or os.getenv("DATA_SOURCE_SECRET_KEY")
        if not value:
            raise CredentialCipherError("DATA_SOURCE_SECRET_KEY is not configured")
        try:
            self._fernet = Fernet(value.encode("ascii"))
        except (ValueError, UnicodeError) as exc:
            raise CredentialCipherError(
                "DATA_SOURCE_SECRET_KEY must be a valid Fernet key"
            ) from exc

    def encrypt(self, secret: str) -> str:
        return self._fernet.encrypt(secret.encode("utf-8")).decode("ascii")

    def decrypt(self, encrypted: str) -> str:
        try:
            return self._fernet.decrypt(encrypted.encode("ascii")).decode("utf-8")
        except (InvalidToken, UnicodeError) as exc:
            raise CredentialCipherError("Stored Source Credential cannot be decrypted") from exc

"""
Transparent encryption for sensitive DB columns (Azure connection strings).

Uses Fernet (AES-128-CBC + HMAC-SHA256) from the cryptography package.
The key lives in .env as STORAGE_ENCRYPTION_KEY.

SQLAlchemy TypeDecorator — encrypts on write, decrypts on read.
All existing call sites use config.connection_string normally — no changes needed.

Migration safety: if STORAGE_ENCRYPTION_KEY is not set, values pass through unencrypted
(so the app still works during local dev without the key).
If a row contains legacy plain text (pre-encryption), it is returned as-is — so existing
containers keep working after the key is first added.
"""
from __future__ import annotations

from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import Text
from sqlalchemy.types import TypeDecorator


@lru_cache(maxsize=1)
def _fernet() -> Fernet | None:
    from app.core.config import get_settings
    key = get_settings().STORAGE_ENCRYPTION_KEY
    if not key:
        return None
    return Fernet(key.encode())


class EncryptedText(TypeDecorator):
    """
    SQLAlchemy column type that transparently encrypts/decrypts text.
    Drop-in replacement for Text — no call-site changes needed.
    """
    impl = Text
    cache_ok = True

    def process_bind_param(self, value: str | None, dialect) -> str | None:
        """Encrypt before writing to DB."""
        if value is None:
            return None
        f = _fernet()
        if f is None:
            return value  # no key configured — store as plain text
        return f.encrypt(value.encode()).decode()

    def process_result_value(self, value: str | None, dialect) -> str | None:
        """Decrypt after reading from DB."""
        if value is None:
            return None
        f = _fernet()
        if f is None:
            return value  # no key configured — return as-is
        try:
            return f.decrypt(value.encode()).decode()
        except (InvalidToken, Exception):
            # Legacy plain-text row (written before encryption was enabled).
            # Return as-is so existing containers keep working.
            return value

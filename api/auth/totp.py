"""TOTP helpers — secret generation, code verification, QR rendering.

Storage of the secret is pluggable: today we base64-wrap it via
:mod:`core.security.encryption` (item 7 of the data-governance
sprint). The wrapper module is a thin Fernet stand-in until a KMS-
backed implementation lands.
"""

from __future__ import annotations

import base64
import io

import pyotp
import qrcode

from core.security.encryption import decrypt, encrypt

_ISSUER = "FraudNet"


def generate_secret() -> str:
    """Mint a fresh base32 TOTP secret. 160 bits of entropy."""

    return pyotp.random_base32()


def store_secret(plain: str) -> str:
    """Encrypt the secret for storage. Use this every time you write to
    ``User.totp_secret``."""

    return encrypt(plain)


def reveal_secret(stored: str) -> str:
    """Reverse of :func:`store_secret`. Used at verify time."""

    return decrypt(stored)


def provisioning_uri(*, secret: str, account: str) -> str:
    """The otpauth:// URI that authenticator apps consume."""

    return pyotp.totp.TOTP(secret).provisioning_uri(name=account, issuer_name=_ISSUER)


def qr_png_base64(uri: str) -> str:
    """Render the provisioning URI as a base64-encoded PNG. Frontend
    can drop straight into ``<img src="data:image/png;base64,...">``."""

    img = qrcode.make(uri)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("ascii")


def verify(stored_secret: str, code: str) -> bool:
    """Validate a 6-digit code against the encrypted-stored secret.

    Accepts a one-step skew window in either direction (RFC 6238 5.2)
    so a clock drift of <= 30s doesn't lock the user out.
    """

    if not code or not code.isdigit():
        return False
    try:
        secret = reveal_secret(stored_secret)
    except Exception:  # noqa: BLE001 — corrupted secret should fail closed
        return False
    return bool(pyotp.totp.TOTP(secret).verify(code, valid_window=1))


__all__ = [
    "generate_secret",
    "store_secret",
    "reveal_secret",
    "provisioning_uri",
    "qr_png_base64",
    "verify",
]

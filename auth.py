"""
Firebase ID token verification for SyncAura's authenticated endpoints.

Reuses the firebase_admin app already initialized in main.py for FCM —
no separate initialize_app() here. The dependency reads the
`Authorization: Bearer <id_token>` header, verifies the token via
firebase_admin.auth.verify_id_token, and returns a small dict the
endpoint handlers can rely on:

    {
        "uid": str,           # Firebase UID — primary key in `users` table
        "email": str | None,
        "name": str | None,   # Google display name
        "picture": str | None # Profile photo URL
    }

Token failures (missing/expired/revoked/forged) raise HTTPException(401).
"""

from __future__ import annotations

from typing import TypedDict

from fastapi import Header, HTTPException, status
from firebase_admin import auth as fb_auth


class AuthedUser(TypedDict):
    uid: str
    email: str | None
    name: str | None
    picture: str | None


def _extract_bearer(authorization: str | None) -> str:
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
        )
    parts = authorization.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header must be 'Bearer <token>'",
        )
    return parts[1].strip()


async def get_current_user(
    authorization: str | None = Header(default=None),
) -> AuthedUser:
    """FastAPI dependency: verify the Firebase ID token and return the user.

    Use as: `user: AuthedUser = Depends(get_current_user)`.
    """
    token = _extract_bearer(authorization)
    try:
        # check_revoked=True is intentionally off — verifying revocation hits
        # Firebase on every request and we don't have a revocation flow yet.
        decoded = fb_auth.verify_id_token(token)
    except fb_auth.ExpiredIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="ID token expired — client should refresh",
        )
    except fb_auth.RevokedIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="ID token revoked",
        )
    except fb_auth.InvalidIdTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid ID token: {e}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token verification failed: {e}",
        )

    uid = decoded.get("uid") or decoded.get("user_id")
    if not uid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing uid claim",
        )

    return AuthedUser(
        uid=uid,
        email=decoded.get("email"),
        name=decoded.get("name"),
        picture=decoded.get("picture"),
    )

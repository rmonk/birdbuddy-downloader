"""
Authentication, authorization and runtime settings for the embedded web dashboard.

- Guests may read the dashboard, status, sightings and media.
- Logged-in users ("user" role) may delete/restore media, trigger syncs and
  manage their own password, passkeys and API keys.
- Admins ("admin" role) may additionally manage users and edit runtime settings.

Authentication sources (checked in order):
  1. Session cookie (password or passkey login)
  2. Per-user API key via `X-API-Key` header or `Authorization: Bearer <key>`
"""

import functools
import hashlib
import hmac
import json
import logging
import re
import secrets
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from aiohttp import web
from webauthn import (
    generate_authentication_options,
    generate_registration_options,
    options_to_json,
    verify_authentication_response,
    verify_registration_response,
)
from webauthn.helpers import base64url_to_bytes, bytes_to_base64url
from webauthn.helpers.structs import (
    AuthenticatorSelectionCriteria,
    AuthenticatorTransport,
    PublicKeyCredentialDescriptor,
    ResidentKeyRequirement,
    UserVerificationRequirement,
)

logger = logging.getLogger("birdbuddy-downloader")

ROLES = ("user", "admin")
SESSION_COOKIE = "bbd_session"
CHALLENGE_COOKIE = "bbd_webauthn"
SESSION_TTL = timedelta(days=14)
CHALLENGE_TTL_SECONDS = 300
MIN_PASSWORD_LENGTH = 8
USERNAME_RE = re.compile(r"^[A-Za-z0-9_.@-]{1,64}$")

SCRYPT_N = 2**14
SCRYPT_R = 8
SCRYPT_P = 1

AUTH_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE COLLATE NOCASE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin')),
    webauthn_user_id BLOB NOT NULL,
    must_change_password INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    password_changed_at TEXT
);
CREATE TABLE IF NOT EXISTS sessions (
    token_hash TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    last_seen_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
CREATE TABLE IF NOT EXISTS passkeys (
    credential_id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    public_key BLOB NOT NULL,
    sign_count INTEGER NOT NULL DEFAULT 0,
    name TEXT,
    transports TEXT,
    created_at TEXT NOT NULL,
    last_used_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_passkeys_user ON passkeys(user_id);
CREATE TABLE IF NOT EXISTS api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT,
    prefix TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    last_used_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    updated_by TEXT
);
"""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _hash_token(token: str) -> str:
    """Hash a server-generated random secret (session token or API key) for storage.

    These are 256-bit values from `secrets`, so a fast digest is appropriate: there is
    nothing to brute-force, and a slow KDF would add latency to every request. User
    passwords never come through here; they use scrypt in `hash_password()`.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _error(message: str, status: int) -> web.Response:
    return web.json_response({"status": "error", "message": message}, status=status)


async def _read_json(request: web.Request) -> dict | None:
    try:
        data = await request.json()
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def init_auth_db(conn: sqlite3.Connection):
    """Create authentication and settings tables."""
    conn.execute("PRAGMA foreign_keys=ON;")
    with conn:
        conn.executescript(AUTH_SCHEMA)
        user_cols = {row[1] for row in conn.execute("PRAGMA table_info(users)")}
        if "must_change_password" not in user_cols:
            conn.execute(
                "ALTER TABLE users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0"
            )
    purge_expired_sessions(conn)


# ------------------------------------------------------------------------------
# Passwords
# ------------------------------------------------------------------------------


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(
        password.encode("utf-8"), salt=salt, n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P
    )
    return f"scrypt${SCRYPT_N}${SCRYPT_R}${SCRYPT_P}${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, n, r, p, salt_hex, digest_hex = stored.split("$")
        if scheme != "scrypt":
            return False
        digest = hashlib.scrypt(
            password.encode("utf-8"),
            salt=bytes.fromhex(salt_hex),
            n=int(n),
            r=int(r),
            p=int(p),
        )
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(digest.hex(), digest_hex)


# Verified against when a username does not exist, so lookups take similar time.
_DUMMY_HASH = hash_password(secrets.token_urlsafe(16))


def generate_password() -> str:
    return secrets.token_urlsafe(18)


def validate_new_password(password) -> str | None:
    if not isinstance(password, str) or len(password) < MIN_PASSWORD_LENGTH:
        return f"Password must be at least {MIN_PASSWORD_LENGTH} characters"
    return None


# ------------------------------------------------------------------------------
# Users
# ------------------------------------------------------------------------------


# Queries feeding this select: id, username, role, must_change_password, ...
def _user_row_to_dict(row) -> dict:
    return {
        "id": row[0],
        "username": row[1],
        "role": row[2],
        "must_change_password": bool(row[3]),
    }


def get_user_by_id(conn: sqlite3.Connection, user_id: int) -> dict | None:
    row = conn.execute(
        "SELECT id, username, role, must_change_password FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    return _user_row_to_dict(row) if row else None


def get_user_by_username(conn: sqlite3.Connection, username: str) -> dict | None:
    row = conn.execute(
        "SELECT id, username, role, must_change_password FROM users WHERE username = ?",
        (username,),
    ).fetchone()
    return _user_row_to_dict(row) if row else None


def create_user(
    conn: sqlite3.Connection,
    username: str,
    password: str,
    role: str = "user",
    must_change_password: bool = False,
) -> int:
    with conn:
        cur = conn.execute(
            "INSERT INTO users (username, password_hash, role, webauthn_user_id, must_change_password, "
            "created_at, password_changed_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                username,
                hash_password(password),
                role,
                secrets.token_bytes(32),
                int(must_change_password),
                _now_iso(),
                _now_iso(),
            ),
        )
    return cur.lastrowid


def set_user_password(
    conn: sqlite3.Connection,
    user_id: int,
    password: str,
    must_change_password: bool = False,
):
    """Set a password. Pass must_change_password=True for any password the user did
    not choose themselves (generated, logged, or set by an admin) so it works only
    until their first login."""
    with conn:
        conn.execute(
            "UPDATE users SET password_hash = ?, password_changed_at = ?, must_change_password = ? "
            "WHERE id = ?",
            (hash_password(password), _now_iso(), int(must_change_password), user_id),
        )


def count_admins(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM users WHERE role = 'admin'").fetchone()[0]


def ensure_initial_admin(conn: sqlite3.Connection) -> str | None:
    """Create the 'admin' account with a random password if no users exist."""
    if conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] > 0:
        return None
    password = generate_password()
    create_user(conn, "admin", password, role="admin", must_change_password=True)
    banner = "=" * 72
    logger.warning(banner)
    logger.warning("No users found - created initial web dashboard administrator:")
    logger.warning("    username: admin")
    logger.warning(f"    password: {password}")
    logger.warning(
        "This password is single-use: you must choose a new one when you first log in."
    )
    logger.warning(banner)
    return password


def reset_user_password(
    conn: sqlite3.Connection, username: str, password: str | None = None
) -> str | None:
    """Set a user's password and revoke their sessions.

    With no password, a random one is generated and returned; it must be changed at
    next login. A password chosen by the operator is not flagged.
    """
    user = get_user_by_username(conn, username)
    if not user:
        return None
    generated = password is None
    if generated:
        password = generate_password()
    set_user_password(conn, user["id"], password, must_change_password=generated)
    delete_user_sessions(conn, user["id"])
    return password


# ------------------------------------------------------------------------------
# Sessions
# ------------------------------------------------------------------------------


def create_session(conn: sqlite3.Connection, user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    now = _now()
    with conn:
        conn.execute(
            "INSERT INTO sessions (token_hash, user_id, created_at, expires_at, last_seen_at) VALUES (?, ?, ?, ?, ?)",
            (
                _hash_token(token),
                user_id,
                now.isoformat(),
                (now + SESSION_TTL).isoformat(),
                now.isoformat(),
            ),
        )
    return token


def get_session_user(conn: sqlite3.Connection, token: str) -> dict | None:
    token_hash = _hash_token(token)
    row = conn.execute(
        "SELECT u.id, u.username, u.role, u.must_change_password, s.expires_at, s.last_seen_at "
        "FROM sessions s "
        "JOIN users u ON u.id = s.user_id WHERE s.token_hash = ?",
        (token_hash,),
    ).fetchone()
    if not row:
        return None
    now = _now()
    if datetime.fromisoformat(row[4]) <= now:
        delete_session(conn, token)
        return None
    # Sliding expiry, written at most once a minute
    last_seen = datetime.fromisoformat(row[5]) if row[5] else None
    if not last_seen or (now - last_seen).total_seconds() > 60:
        with conn:
            conn.execute(
                "UPDATE sessions SET last_seen_at = ?, expires_at = ? WHERE token_hash = ?",
                (now.isoformat(), (now + SESSION_TTL).isoformat(), token_hash),
            )
    return _user_row_to_dict(row)


def delete_session(conn: sqlite3.Connection, token: str):
    with conn:
        conn.execute("DELETE FROM sessions WHERE token_hash = ?", (_hash_token(token),))


def delete_user_sessions(
    conn: sqlite3.Connection, user_id: int, keep_token: str | None = None
):
    with conn:
        if keep_token:
            conn.execute(
                "DELETE FROM sessions WHERE user_id = ? AND token_hash != ?",
                (user_id, _hash_token(keep_token)),
            )
        else:
            conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))


def purge_expired_sessions(conn: sqlite3.Connection):
    with conn:
        conn.execute("DELETE FROM sessions WHERE expires_at <= ?", (_now_iso(),))


# ------------------------------------------------------------------------------
# API keys
# ------------------------------------------------------------------------------


def create_api_key(conn: sqlite3.Connection, user_id: int, name: str) -> dict:
    prefix = secrets.token_hex(4)
    key = f"bbd_{prefix}_{secrets.token_urlsafe(32)}"
    with conn:
        cur = conn.execute(
            "INSERT INTO api_keys (user_id, name, prefix, key_hash, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, name, prefix, _hash_token(key), _now_iso()),
        )
    return {"id": cur.lastrowid, "name": name, "prefix": prefix, "key": key}


def resolve_api_key(conn: sqlite3.Connection, key: str) -> dict | None:
    row = conn.execute(
        "SELECT u.id, u.username, u.role, u.must_change_password, k.id "
        "FROM api_keys k JOIN users u ON u.id = k.user_id "
        "WHERE k.key_hash = ?",
        (_hash_token(key),),
    ).fetchone()
    if not row:
        return None
    with conn:
        conn.execute(
            "UPDATE api_keys SET last_used_at = ? WHERE id = ?", (_now_iso(), row[4])
        )
    return _user_row_to_dict(row)


def _extract_api_key(request: web.Request) -> str | None:
    header_key = request.headers.get("X-API-Key")
    if header_key:
        return header_key.strip()
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:].strip() or None
    return None


# ------------------------------------------------------------------------------
# Login throttling
# ------------------------------------------------------------------------------


class LoginThrottle:
    """In-memory sliding-window limiter for failed logins, keyed by IP and username."""

    def __init__(self, max_failures: int = 5, window_seconds: int = 300):
        self.max_failures = max_failures
        self.window = window_seconds
        self._failures: dict[str, list[float]] = {}

    def _recent(self, key: str) -> list[float]:
        cutoff = time.monotonic() - self.window
        attempts = [t for t in self._failures.get(key, []) if t > cutoff]
        if attempts:
            self._failures[key] = attempts
        else:
            self._failures.pop(key, None)
        return attempts

    def is_blocked(self, *keys: str) -> bool:
        return any(len(self._recent(k)) >= self.max_failures for k in keys)

    def record_failure(self, *keys: str):
        now = time.monotonic()
        for k in keys:
            self._failures.setdefault(k, []).append(now)

    def reset(self, *keys: str):
        for k in keys:
            self._failures.pop(k, None)


# ------------------------------------------------------------------------------
# Configuration, middleware and guards
# ------------------------------------------------------------------------------


@dataclass
class AuthConfig:
    public_url: str | None = None
    rp_name: str = "Bird Buddy Downloader"

    @property
    def origin(self) -> str | None:
        if not self.public_url:
            return None
        parsed = urlparse(self.public_url)
        return f"{parsed.scheme}://{parsed.netloc}"

    @property
    def rp_id(self) -> str | None:
        if not self.public_url:
            return None
        return urlparse(self.public_url).hostname

    @property
    def passkeys_enabled(self) -> bool:
        return bool(self.rp_id)

    @property
    def secure_cookies(self) -> bool:
        return bool(self.public_url and self.public_url.lower().startswith("https://"))


def _origin_allowed(request: web.Request, config: AuthConfig) -> bool:
    """Reject cross-site state-changing requests from browsers."""
    origin = request.headers.get("Origin")
    if origin:
        if config.origin and origin == config.origin:
            return True
        return urlparse(origin).netloc == request.host
    fetch_site = request.headers.get("Sec-Fetch-Site")
    if fetch_site:
        return fetch_site in ("same-origin", "none")
    # Non-browser clients (curl, scripts) send neither header and are not CSRF vectors
    return True


@web.middleware
async def auth_middleware(request: web.Request, handler):
    conn: sqlite3.Connection = request.app["auth_conn"]
    config: AuthConfig = request.app["auth_config"]
    user = None
    via = None

    token = request.cookies.get(SESSION_COOKIE)
    if token:
        user = get_session_user(conn, token)
        if user:
            via = "session"
    if not user:
        api_key = _extract_api_key(request)
        if api_key:
            user = resolve_api_key(conn, api_key)
            if user:
                via = "api_key"

    request["user"] = user
    request["auth_via"] = via

    if (
        request.method not in ("GET", "HEAD", "OPTIONS")
        and via != "api_key"
        and not _origin_allowed(request, config)
    ):
        return _error("Cross-origin request rejected", 403)

    return await handler(request)


def require_role(role: str, allow_pending_password_change: bool = False):
    """Decorator for aiohttp handlers: 401 when anonymous, 403 when role is insufficient.

    Sessions whose password must be changed (generated or admin-set password) are
    refused with code "password_change_required" unless the handler opts out.
    API keys are independent credentials and are not affected.
    """

    def decorator(handler):
        @functools.wraps(handler)
        async def wrapper(request: web.Request):
            user = request.get("user")
            if not user:
                return _error("Login required", 401)
            if (
                user.get("must_change_password")
                and request.get("auth_via") == "session"
                and not allow_pending_password_change
            ):
                return web.json_response(
                    {
                        "status": "error",
                        "code": "password_change_required",
                        "message": "You must change your password before continuing.",
                    },
                    status=403,
                )
            if role == "admin" and user["role"] != "admin":
                return _error("Admin role required", 403)
            return await handler(request)

        return wrapper

    return decorator


def _set_session_cookie(response: web.Response, token: str, config: AuthConfig):
    response.set_cookie(
        SESSION_COOKIE,
        token,
        max_age=int(SESSION_TTL.total_seconds()),
        httponly=True,
        secure=config.secure_cookies,
        samesite="Strict",
        path="/",
    )


def _login_response(
    request: web.Request, conn: sqlite3.Connection, user: dict
) -> web.Response:
    purge_expired_sessions(conn)
    token = create_session(conn, user["id"])
    response = web.json_response({"status": "ok", "user": user})
    _set_session_cookie(response, token, request.app["auth_config"])
    return response


# ------------------------------------------------------------------------------
# Login / logout / identity
# ------------------------------------------------------------------------------


async def handle_me(request: web.Request) -> web.Response:
    config: AuthConfig = request.app["auth_config"]
    return web.json_response(
        {
            "user": request.get("user"),
            "auth_via": request.get("auth_via"),
            "passkeys_enabled": config.passkeys_enabled,
        }
    )


async def handle_login(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    throttle: LoginThrottle = request.app["login_throttle"]
    data = await _read_json(request)
    if data is None:
        return _error("Invalid JSON", 400)
    username = str(data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not isinstance(password, str) or not password:
        return _error("Username and password required", 400)

    keys = (f"ip:{request.remote}", f"user:{username.lower()}")
    if throttle.is_blocked(*keys):
        return _error("Too many failed attempts. Try again in a few minutes.", 429)

    row = conn.execute(
        "SELECT id, username, role, must_change_password, password_hash FROM users WHERE username = ?",
        (username,),
    ).fetchone()
    if not verify_password(password, row[4] if row else _DUMMY_HASH) or not row:
        throttle.record_failure(*keys)
        logger.warning(
            f"Failed web login for '{username}' from {request.remote or 'unknown'}"
        )
        return _error("Invalid username or password", 401)

    throttle.reset(*keys)
    logger.info(f"User '{row[1]}' logged in to web dashboard")
    return _login_response(request, conn, _user_row_to_dict(row))


async def handle_logout(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    token = request.cookies.get(SESSION_COOKIE)
    if token:
        delete_session(conn, token)
    response = web.json_response({"status": "ok"})
    response.del_cookie(SESSION_COOKIE, path="/")
    return response


# ------------------------------------------------------------------------------
# Passkeys (WebAuthn)
# ------------------------------------------------------------------------------


def _store_challenge(
    request: web.Request, response: web.Response, purpose: str, challenge: bytes
):
    store: dict = request.app["webauthn_challenges"]
    now = time.monotonic()
    for k in [k for k, v in store.items() if v["expires"] <= now]:
        store.pop(k, None)
    challenge_id = secrets.token_urlsafe(24)
    user = request.get("user")
    store[challenge_id] = {
        "challenge": challenge,
        "purpose": purpose,
        "user_id": user["id"] if user else None,
        "expires": now + CHALLENGE_TTL_SECONDS,
    }
    response.set_cookie(
        CHALLENGE_COOKIE,
        challenge_id,
        max_age=CHALLENGE_TTL_SECONDS,
        httponly=True,
        secure=request.app["auth_config"].secure_cookies,
        samesite="Strict",
        path="/api/",
    )


def _pop_challenge(request: web.Request, purpose: str) -> bytes | None:
    store: dict = request.app["webauthn_challenges"]
    challenge_id = request.cookies.get(CHALLENGE_COOKIE)
    entry = store.pop(challenge_id, None) if challenge_id else None
    if not entry or entry["expires"] <= time.monotonic():
        return None
    if entry["purpose"] != purpose:
        return None
    user = request.get("user")
    if purpose == "register" and (not user or entry["user_id"] != user["id"]):
        return None
    return entry["challenge"]


def _passkeys_disabled_response() -> web.Response:
    return _error(
        "Passkeys are disabled. Set WEB_PUBLIC_URL to the dashboard's HTTPS address to enable them.",
        503,
    )


async def handle_passkey_login_options(request: web.Request) -> web.Response:
    config: AuthConfig = request.app["auth_config"]
    if not config.passkeys_enabled:
        return _passkeys_disabled_response()
    options = generate_authentication_options(
        rp_id=config.rp_id,
        user_verification=UserVerificationRequirement.PREFERRED,
    )
    response = web.Response(
        text=options_to_json(options), content_type="application/json"
    )
    _store_challenge(request, response, "login", options.challenge)
    return response


async def handle_passkey_login_verify(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    config: AuthConfig = request.app["auth_config"]
    throttle: LoginThrottle = request.app["login_throttle"]
    if not config.passkeys_enabled:
        return _passkeys_disabled_response()
    ip_key = f"ip:{request.remote}"
    if throttle.is_blocked(ip_key):
        return _error("Too many failed attempts. Try again in a few minutes.", 429)

    credential = await _read_json(request)
    challenge = _pop_challenge(request, "login")
    if credential is None or challenge is None:
        return _error("Passkey challenge expired or invalid. Please try again.", 400)

    row = conn.execute(
        "SELECT p.credential_id, p.public_key, p.sign_count, u.id, u.username, u.role, u.webauthn_user_id "
        "FROM passkeys p JOIN users u ON u.id = p.user_id WHERE p.credential_id = ?",
        (str(credential.get("id", "")),),
    ).fetchone()
    if not row:
        throttle.record_failure(ip_key)
        return _error("This passkey is not registered", 401)

    user_handle = (credential.get("response") or {}).get("userHandle")
    if user_handle and user_handle != bytes_to_base64url(row[6]):
        throttle.record_failure(ip_key)
        return _error("Passkey does not match its user", 401)

    try:
        verified = verify_authentication_response(
            credential=credential,
            expected_challenge=challenge,
            expected_rp_id=config.rp_id,
            expected_origin=config.origin,
            credential_public_key=row[1],
            credential_current_sign_count=row[2],
        )
    except Exception as e:
        throttle.record_failure(ip_key)
        logger.warning(f"Passkey login verification failed: {e}")
        return _error("Passkey verification failed", 401)

    with conn:
        conn.execute(
            "UPDATE passkeys SET sign_count = ?, last_used_at = ? WHERE credential_id = ?",
            (verified.new_sign_count, _now_iso(), row[0]),
        )
    throttle.reset(ip_key)
    user = get_user_by_id(conn, row[3])
    logger.info(f"User '{user['username']}' logged in to web dashboard with a passkey")
    return _login_response(request, conn, user)


@require_role("user")
async def handle_passkey_register_options(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    config: AuthConfig = request.app["auth_config"]
    if not config.passkeys_enabled:
        return _passkeys_disabled_response()
    user = request["user"]
    webauthn_user_id = conn.execute(
        "SELECT webauthn_user_id FROM users WHERE id = ?", (user["id"],)
    ).fetchone()[0]
    existing = conn.execute(
        "SELECT credential_id FROM passkeys WHERE user_id = ?", (user["id"],)
    ).fetchall()
    options = generate_registration_options(
        rp_id=config.rp_id,
        rp_name=config.rp_name,
        user_name=user["username"],
        user_id=webauthn_user_id,
        user_display_name=user["username"],
        authenticator_selection=AuthenticatorSelectionCriteria(
            resident_key=ResidentKeyRequirement.REQUIRED,
            user_verification=UserVerificationRequirement.PREFERRED,
        ),
        exclude_credentials=[
            PublicKeyCredentialDescriptor(id=base64url_to_bytes(r[0])) for r in existing
        ],
    )
    response = web.Response(
        text=options_to_json(options), content_type="application/json"
    )
    _store_challenge(request, response, "register", options.challenge)
    return response


@require_role("user")
async def handle_passkey_register_verify(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    config: AuthConfig = request.app["auth_config"]
    if not config.passkeys_enabled:
        return _passkeys_disabled_response()
    data = await _read_json(request)
    challenge = _pop_challenge(request, "register")
    if (
        data is None
        or challenge is None
        or not isinstance(data.get("credential"), dict)
    ):
        return _error("Passkey challenge expired or invalid. Please try again.", 400)

    credential = data["credential"]
    name = str(data.get("name") or "").strip()[:64] or "Passkey"
    try:
        verified = verify_registration_response(
            credential=credential,
            expected_challenge=challenge,
            expected_rp_id=config.rp_id,
            expected_origin=config.origin,
        )
    except Exception as e:
        logger.warning(f"Passkey registration verification failed: {e}")
        return _error("Passkey registration failed", 400)

    transports = (credential.get("response") or {}).get("transports") or []
    valid_transports = {t.value for t in AuthenticatorTransport}
    transports = [t for t in transports if t in valid_transports]
    credential_id = bytes_to_base64url(verified.credential_id)
    try:
        with conn:
            conn.execute(
                "INSERT INTO passkeys (credential_id, user_id, public_key, sign_count, name, transports, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    credential_id,
                    request["user"]["id"],
                    verified.credential_public_key,
                    verified.sign_count,
                    name,
                    json.dumps(transports),
                    _now_iso(),
                ),
            )
    except sqlite3.IntegrityError:
        return _error("This passkey is already registered", 409)
    logger.info(f"User '{request['user']['username']}' registered passkey '{name}'")
    return web.json_response(
        {"status": "ok", "passkey": {"id": credential_id, "name": name}}
    )


@require_role("user")
async def handle_passkeys_list(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    rows = conn.execute(
        "SELECT credential_id, name, created_at, last_used_at FROM passkeys WHERE user_id = ? ORDER BY created_at",
        (request["user"]["id"],),
    ).fetchall()
    return web.json_response(
        {
            "passkeys": [
                {"id": r[0], "name": r[1], "created_at": r[2], "last_used_at": r[3]}
                for r in rows
            ]
        }
    )


@require_role("user")
async def handle_passkey_rename(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    data = await _read_json(request)
    name = str((data or {}).get("name") or "").strip()[:64]
    if not name:
        return _error("name required", 400)
    with conn:
        cur = conn.execute(
            "UPDATE passkeys SET name = ? WHERE credential_id = ? AND user_id = ?",
            (name, request.match_info["credential_id"], request["user"]["id"]),
        )
    if cur.rowcount == 0:
        return _error("Passkey not found", 404)
    return web.json_response({"status": "ok"})


@require_role("user")
async def handle_passkey_delete(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    with conn:
        cur = conn.execute(
            "DELETE FROM passkeys WHERE credential_id = ? AND user_id = ?",
            (request.match_info["credential_id"], request["user"]["id"]),
        )
    if cur.rowcount == 0:
        return _error("Passkey not found", 404)
    return web.json_response({"status": "deleted"})


# ------------------------------------------------------------------------------
# Account: password & API keys
# ------------------------------------------------------------------------------


@require_role("user", allow_pending_password_change=True)
async def handle_change_password(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    data = await _read_json(request)
    if data is None:
        return _error("Invalid JSON", 400)
    user = request["user"]
    current = data.get("current_password") or ""
    new = data.get("new_password")
    stored = conn.execute(
        "SELECT password_hash FROM users WHERE id = ?", (user["id"],)
    ).fetchone()[0]
    if not isinstance(current, str) or not verify_password(current, stored):
        return _error("Current password is incorrect", 403)
    problem = validate_new_password(new)
    if problem:
        return _error(problem, 400)
    if new == current:
        return _error("New password must be different from the current one", 400)
    set_user_password(conn, user["id"], new)
    delete_user_sessions(
        conn, user["id"], keep_token=request.cookies.get(SESSION_COOKIE)
    )
    logger.info(f"User '{user['username']}' changed their password")
    return web.json_response({"status": "ok"})


@require_role("user")
async def handle_api_keys_list(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    rows = conn.execute(
        "SELECT id, name, prefix, created_at, last_used_at FROM api_keys WHERE user_id = ? ORDER BY created_at",
        (request["user"]["id"],),
    ).fetchall()
    return web.json_response(
        {
            "api_keys": [
                {
                    "id": r[0],
                    "name": r[1],
                    "prefix": f"bbd_{r[2]}_",
                    "created_at": r[3],
                    "last_used_at": r[4],
                }
                for r in rows
            ]
        }
    )


@require_role("user")
async def handle_api_key_create(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    data = await _read_json(request) or {}
    name = str(data.get("name") or "").strip()[:64] or "API key"
    created = create_api_key(conn, request["user"]["id"], name)
    logger.info(f"User '{request['user']['username']}' created API key '{name}'")
    return web.json_response({"status": "ok", "api_key": created})


@require_role("user")
async def handle_api_key_delete(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    try:
        key_id = int(request.match_info["key_id"])
    except ValueError:
        return _error("Invalid key id", 400)
    with conn:
        cur = conn.execute(
            "DELETE FROM api_keys WHERE id = ? AND user_id = ?",
            (key_id, request["user"]["id"]),
        )
    if cur.rowcount == 0:
        return _error("API key not found", 404)
    return web.json_response({"status": "deleted"})


# ------------------------------------------------------------------------------
# Admin: users
# ------------------------------------------------------------------------------


@require_role("admin")
async def handle_users_list(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    rows = conn.execute(
        "SELECT u.id, u.username, u.role, u.created_at, "
        "(SELECT COUNT(*) FROM passkeys p WHERE p.user_id = u.id), "
        "(SELECT COUNT(*) FROM api_keys k WHERE k.user_id = u.id), "
        "u.must_change_password "
        "FROM users u ORDER BY u.username"
    ).fetchall()
    return web.json_response(
        {
            "users": [
                {
                    "id": r[0],
                    "username": r[1],
                    "role": r[2],
                    "created_at": r[3],
                    "passkeys": r[4],
                    "api_keys": r[5],
                    "must_change_password": bool(r[6]),
                }
                for r in rows
            ]
        }
    )


@require_role("admin")
async def handle_user_create(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    data = await _read_json(request)
    if data is None:
        return _error("Invalid JSON", 400)
    username = str(data.get("username") or "").strip()
    role = data.get("role", "user")
    password = data.get("password") or None
    if not USERNAME_RE.match(username):
        return _error(
            "Username must be 1-64 characters of letters, digits, '.', '_', '-' or '@'",
            400,
        )
    if role not in ROLES:
        return _error("Role must be 'user' or 'admin'", 400)
    generated = password is None
    if generated:
        password = generate_password()
    else:
        problem = validate_new_password(password)
        if problem:
            return _error(problem, 400)
    try:
        user_id = create_user(conn, username, password, role, must_change_password=True)
    except sqlite3.IntegrityError:
        return _error("A user with that name already exists", 409)
    logger.info(
        f"Admin '{request['user']['username']}' created {role} account '{username}'"
    )
    result = {"status": "ok", "user": get_user_by_id(conn, user_id)}
    if generated:
        result["password"] = password
    return web.json_response(result)


@require_role("admin")
async def handle_user_update(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    try:
        user_id = int(request.match_info["user_id"])
    except ValueError:
        return _error("Invalid user id", 400)
    target = get_user_by_id(conn, user_id)
    if not target:
        return _error("User not found", 404)
    data = await _read_json(request)
    if data is None:
        return _error("Invalid JSON", 400)

    result = {"status": "ok"}
    role = data.get("role")
    if role is not None:
        if role not in ROLES:
            return _error("Role must be 'user' or 'admin'", 400)
        if target["role"] == "admin" and role != "admin" and count_admins(conn) <= 1:
            return _error("Cannot demote the last admin", 400)
    password = data.get("password")
    if password is not None:
        problem = validate_new_password(password)
        if problem:
            return _error(problem, 400)

    if role is not None and role != target["role"]:
        with conn:
            conn.execute("UPDATE users SET role = ? WHERE id = ?", (role, user_id))
        logger.info(
            f"Admin '{request['user']['username']}' set role of '{target['username']}' to {role}"
        )
    if data.get("reset_password") or password is not None:
        new_password = password if password is not None else generate_password()
        set_user_password(conn, user_id, new_password, must_change_password=True)
        delete_user_sessions(conn, user_id)
        if password is None:
            result["password"] = new_password
        logger.info(
            f"Admin '{request['user']['username']}' reset the password of '{target['username']}'"
        )
    result["user"] = get_user_by_id(conn, user_id)
    return web.json_response(result)


@require_role("admin")
async def handle_user_delete(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    try:
        user_id = int(request.match_info["user_id"])
    except ValueError:
        return _error("Invalid user id", 400)
    if user_id == request["user"]["id"]:
        return _error("You cannot delete your own account", 400)
    target = get_user_by_id(conn, user_id)
    if not target:
        return _error("User not found", 404)
    if target["role"] == "admin" and count_admins(conn) <= 1:
        return _error("Cannot delete the last admin", 400)
    with conn:
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    logger.info(
        f"Admin '{request['user']['username']}' deleted account '{target['username']}'"
    )
    return web.json_response({"status": "deleted"})


# ------------------------------------------------------------------------------
# Admin: runtime settings
# ------------------------------------------------------------------------------


class _BlankFormat(dict):
    def __missing__(self, key):
        return "x"


def _parse_int(value, minimum=0):
    if isinstance(value, bool):
        raise ValueError("must be a whole number")
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError("must be a whole number")
    if isinstance(value, float) and value != parsed:
        raise ValueError("must be a whole number")
    if parsed < minimum:
        raise ValueError(f"must be at least {minimum}")
    return parsed


def _parse_float(value, minimum=0.0, maximum=None):
    if isinstance(value, bool):
        raise ValueError("must be a number")
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        raise ValueError("must be a number")
    if parsed != parsed or parsed < minimum:
        raise ValueError(f"must be at least {minimum}")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"must be at most {maximum}")
    return parsed


def _parse_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    raise ValueError("must be true or false")


def _parse_template(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("must not be empty")
    try:
        value.format_map(_BlankFormat())
    except Exception as e:
        raise ValueError(f"is not a valid template ({e})")
    return value


def _parse_optional_str(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("must be text")
    return value.strip() or None


SETTINGS_SPEC = {
    "interval": {
        "label": "Sync interval (seconds)",
        "type": "int",
        "help": "0 runs once at startup and then only on Sync Now.",
        "parse": lambda v: _parse_int(v, 0),
    },
    "min_bird_confidence": {
        "label": "Minimum bird confidence",
        "type": "float",
        "help": "Detection threshold between 0 and 1.",
        "parse": lambda v: _parse_float(v, 0.0, 1.0),
    },
    "buffer_hours": {
        "label": "Incremental sync buffer (hours)",
        "type": "float",
        "help": "Hours before the latest download to start fetching feed items.",
        "parse": lambda v: _parse_float(v, 0.0),
    },
    "db_retention_days": {
        "label": "Database retention (days)",
        "type": "int",
        "help": "0 disables cleanup.",
        "parse": lambda v: _parse_int(v, 0),
    },
    "max_pages": {
        "label": "Max feed pages",
        "type": "int",
        "help": "0 is unlimited.",
        "parse": lambda v: _parse_int(v, 0),
    },
    "dir_template": {
        "label": "Directory template",
        "type": "str",
        "help": "e.g. {feeder_name}/{species_name}",
        "parse": _parse_template,
    },
    "filename_template": {
        "label": "Filename template",
        "type": "str",
        "help": "e.g. {year}{month}{day}_{hour}{minute}{second}_{media_id_short}.{ext}",
        "parse": _parse_template,
    },
    "feeder_filter": {
        "label": "Feeder name filter",
        "type": "str",
        "help": "Case-insensitive substring. Leave empty for all feeders.",
        "parse": _parse_optional_str,
    },
    "no_images": {
        "label": "Skip images",
        "type": "bool",
        "help": "Do not download .jpg files.",
        "parse": _parse_bool,
    },
    "no_videos": {
        "label": "Skip videos",
        "type": "bool",
        "help": "Do not download .mp4 files.",
        "parse": _parse_bool,
    },
    "no_detect": {
        "label": "Disable bird detection",
        "type": "bool",
        "help": "Skip bird likelihood scoring on downloaded images.",
        "parse": _parse_bool,
    },
}

_DEFAULTS_ATTR = "_settings_defaults"


def _apply_setting(target, key: str, value):
    """Set a parsed setting on args and on any downloader attributes derived from it."""
    args = getattr(target, "args", target)
    setattr(args, key, value)
    if key in ("dir_template", "filename_template") and hasattr(target, key):
        setattr(target, key, value)


def apply_settings_overrides(args, conn: sqlite3.Connection):
    """Remember CLI/env values as defaults, then apply stored overrides onto args."""
    setattr(args, _DEFAULTS_ATTR, {k: getattr(args, k, None) for k in SETTINGS_SPEC})
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    for key, raw in rows:
        spec = SETTINGS_SPEC.get(key)
        if not spec:
            continue
        try:
            value = spec["parse"](json.loads(raw))
        except (ValueError, json.JSONDecodeError) as e:
            logger.warning(f"Ignoring invalid stored setting {key}: {e}")
            continue
        setattr(args, key, value)
        logger.info(f"Using web-configured setting {key} = {value!r}")


def _settings_payload(args, conn: sqlite3.Connection) -> list[dict]:
    defaults = getattr(args, _DEFAULTS_ATTR, {})
    overridden = {
        r[0]: (r[1], r[2])
        for r in conn.execute("SELECT key, updated_at, updated_by FROM settings")
    }
    items = []
    for key, spec in SETTINGS_SPEC.items():
        items.append(
            {
                "key": key,
                "label": spec["label"],
                "type": spec["type"],
                "help": spec["help"],
                "value": getattr(args, key, None),
                "default": defaults.get(key),
                "overridden": key in overridden,
                "updated_at": overridden.get(key, (None, None))[0],
                "updated_by": overridden.get(key, (None, None))[1],
            }
        )
    return items


@require_role("admin")
async def handle_settings_get(request: web.Request) -> web.Response:
    downloader = request.app["downloader"]
    return web.json_response(
        {"settings": _settings_payload(downloader.args, request.app["auth_conn"])}
    )


@require_role("admin")
async def handle_settings_update(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    downloader = request.app["downloader"]
    data = await _read_json(request)
    if not data:
        return _error("Expected a JSON object of settings", 400)

    parsed = {}
    errors = {}
    for key, value in data.items():
        spec = SETTINGS_SPEC.get(key)
        if not spec:
            errors[key] = "is not an editable setting"
            continue
        try:
            parsed[key] = spec["parse"](value)
        except ValueError as e:
            errors[key] = str(e)
    if errors:
        message = "; ".join(f"{k} {v}" for k, v in errors.items())
        return web.json_response(
            {"status": "error", "message": message, "errors": errors}, status=400
        )

    username = request["user"]["username"]
    with conn:
        for key, value in parsed.items():
            conn.execute(
                "INSERT INTO settings (key, value, updated_at, updated_by) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
                "updated_at = excluded.updated_at, updated_by = excluded.updated_by",
                (key, json.dumps(value), _now_iso(), username),
            )
    for key, value in parsed.items():
        _apply_setting(downloader, key, value)
        logger.info(f"Admin '{username}' set {key} = {value!r}")
    return web.json_response(
        {"status": "ok", "settings": _settings_payload(downloader.args, conn)}
    )


@require_role("admin")
async def handle_settings_reset(request: web.Request) -> web.Response:
    conn: sqlite3.Connection = request.app["auth_conn"]
    downloader = request.app["downloader"]
    key = request.match_info["key"]
    if key not in SETTINGS_SPEC:
        return _error("Unknown setting", 404)
    with conn:
        conn.execute("DELETE FROM settings WHERE key = ?", (key,))
    default = getattr(downloader.args, _DEFAULTS_ATTR, {}).get(key)
    _apply_setting(downloader, key, default)
    logger.info(
        f"Admin '{request['user']['username']}' reset {key} to default {default!r}"
    )
    return web.json_response(
        {"status": "ok", "settings": _settings_payload(downloader.args, conn)}
    )


# ------------------------------------------------------------------------------
# Registration
# ------------------------------------------------------------------------------


def setup_auth(app: web.Application, conn: sqlite3.Connection, config: AuthConfig):
    """Attach auth state and routes to the web application.

    The auth middleware must also be passed to `web.Application(middlewares=[...])`.
    """
    app["auth_conn"] = conn
    app["auth_config"] = config
    app["login_throttle"] = LoginThrottle()
    app["webauthn_challenges"] = {}

    r = app.router
    r.add_get("/api/auth/me", handle_me)
    r.add_post("/api/auth/login", handle_login)
    r.add_post("/api/auth/logout", handle_logout)
    r.add_post("/api/auth/passkey/login/options", handle_passkey_login_options)
    r.add_post("/api/auth/passkey/login/verify", handle_passkey_login_verify)

    r.add_post("/api/account/password", handle_change_password)
    r.add_get("/api/account/passkeys", handle_passkeys_list)
    r.add_post(
        "/api/account/passkeys/register/options", handle_passkey_register_options
    )
    r.add_post("/api/account/passkeys/register/verify", handle_passkey_register_verify)
    r.add_patch("/api/account/passkeys/{credential_id}", handle_passkey_rename)
    r.add_delete("/api/account/passkeys/{credential_id}", handle_passkey_delete)
    r.add_get("/api/account/api-keys", handle_api_keys_list)
    r.add_post("/api/account/api-keys", handle_api_key_create)
    r.add_delete("/api/account/api-keys/{key_id}", handle_api_key_delete)

    r.add_get("/api/admin/users", handle_users_list)
    r.add_post("/api/admin/users", handle_user_create)
    r.add_patch("/api/admin/users/{user_id}", handle_user_update)
    r.add_delete("/api/admin/users/{user_id}", handle_user_delete)
    r.add_get("/api/admin/settings", handle_settings_get)
    r.add_put("/api/admin/settings", handle_settings_update)
    r.add_delete("/api/admin/settings/{key}", handle_settings_reset)

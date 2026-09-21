"""OAuth 2.1 authorization server for the MCP server (stateless tokens).

The "login" is pasting your MGIMO API key (from the Superset cabinet). The key
is validated against the API, and the issued access/refresh token IS that key.

Why stateless: the API stores only sha256(token), never the raw key. So we must
not persist raw keys anywhere either. Making the token equal to the key means:
  - nothing secret is stored on the server (same as header-based clients, which
    already hold the raw key);
  - sessions survive a container restart automatically (the token is validated
    against the API on every call);
  - revocation stays where it belongs: the user revokes the key in the cabinet.

Only two things need persistence, and neither is secret:
  - dynamic client registrations (client_id, redirect_uris, ...) -> Postgres
    (`api.mcp_oauth_clients` in the same `tradeapi` DB), so token refresh also
    survives a restart. Falls back to in-memory if no DSN is configured.
  - short-lived authorization codes -> in-memory (transient; a restart mid-login
    just means the user logs in again).
"""
from __future__ import annotations

import json
import secrets
import time

import httpx
from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    RefreshToken,
    TokenError,
    construct_redirect_uri,
)
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken

TOKEN_TTL = 90 * 24 * 3600          # advertised token lifetime, seconds
CODE_TTL = 600                      # authorization code lifetime, seconds
_VALIDATE_CACHE_TTL = 300           # cache "key is valid" for 5 min


class KeyAccessToken(AccessToken):
    """AccessToken that also carries our per-user API key (not exposed externally)."""

    api_key: str = ""


# --- client registration stores -------------------------------------------
class InMemoryClientStore:
    def __init__(self) -> None:
        self._clients: dict[str, OAuthClientInformationFull] = {}

    async def get(self, client_id: str) -> OAuthClientInformationFull | None:
        return self._clients.get(client_id)

    async def put(self, client: OAuthClientInformationFull) -> None:
        self._clients[client.client_id] = client


class PostgresClientStore:
    """Persists client registrations in Postgres (api.mcp_oauth_clients).

    Client metadata is not secret. Table is created on first use.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool = None

    async def _ensure(self):
        if self._pool is None:
            import asyncpg

            self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=3)
            async with self._pool.acquire() as con:
                await con.execute("CREATE SCHEMA IF NOT EXISTS api")
                await con.execute(
                    "CREATE TABLE IF NOT EXISTS api.mcp_oauth_clients ("
                    "  client_id text PRIMARY KEY,"
                    "  client_info text NOT NULL,"
                    "  created_at timestamptz NOT NULL DEFAULT now())"
                )
        return self._pool

    async def get(self, client_id: str) -> OAuthClientInformationFull | None:
        pool = await self._ensure()
        async with pool.acquire() as con:
            row = await con.fetchrow(
                "SELECT client_info FROM api.mcp_oauth_clients WHERE client_id = $1", client_id
            )
        if row is None:
            return None
        return OAuthClientInformationFull.model_validate(json.loads(row["client_info"]))

    async def put(self, client: OAuthClientInformationFull) -> None:
        pool = await self._ensure()
        info = json.dumps(client.model_dump(mode="json"))
        async with pool.acquire() as con:
            await con.execute(
                "INSERT INTO api.mcp_oauth_clients (client_id, client_info) VALUES ($1, $2) "
                "ON CONFLICT (client_id) DO UPDATE SET client_info = EXCLUDED.client_info",
                client.client_id, info,
            )


class ApiKeyOAuthProvider:
    """OAuth AS where the credential is the user's MGIMO API key (stateless tokens)."""

    def __init__(self, api_base: str, client_store=None) -> None:
        self.api_base = api_base.rstrip("/")
        self.clients = client_store or InMemoryClientStore()
        self._codes: dict[str, tuple[AuthorizationCode, str]] = {}   # code -> (obj, api_key)
        self._pending: dict[str, tuple[OAuthClientInformationFull, AuthorizationParams]] = {}
        self._valid_cache: dict[str, float] = {}                     # api_key -> expiry

    # --- dynamic client registration ---------------------------------------
    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        return await self.clients.get(client_id)

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        await self.clients.put(client_info)

    # --- authorization -----------------------------------------------------
    async def authorize(self, client: OAuthClientInformationFull, params: AuthorizationParams) -> str:
        login_id = secrets.token_urlsafe(24)
        self._pending[login_id] = (client, params)
        return f"/oauth/login?login_id={login_id}"

    async def complete_login(self, login_id: str, api_key: str) -> str | None:
        """Called by the login form POST. Returns a redirect URL, "INVALID", or None."""
        entry = self._pending.get(login_id)
        if entry is None:
            return None
        client, params = entry
        if not await self.validate_key(api_key):
            return "INVALID"  # keep the pending entry so the user can retry
        self._pending.pop(login_id, None)
        code = secrets.token_urlsafe(32)
        self._codes[code] = (
            AuthorizationCode(
                code=code,
                scopes=params.scopes or [],
                expires_at=time.time() + CODE_TTL,
                client_id=client.client_id,
                code_challenge=params.code_challenge,
                redirect_uri=params.redirect_uri,
                redirect_uri_provided_explicitly=params.redirect_uri_provided_explicitly,
                resource=params.resource,
            ),
            api_key,
        )
        return construct_redirect_uri(str(params.redirect_uri), code=code, state=params.state)

    async def load_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: str
    ) -> AuthorizationCode | None:
        entry = self._codes.get(authorization_code)
        if entry is None or entry[0].expires_at < time.time():
            return None
        return entry[0]

    async def exchange_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: AuthorizationCode
    ) -> OAuthToken:
        entry = self._codes.pop(authorization_code.code, None)
        if entry is None:
            raise TokenError("invalid_grant", "Unknown or used authorization code")
        _, api_key = entry
        return self._issue(api_key, authorization_code.scopes)

    # --- refresh (stateless: the refresh token is the key) -----------------
    async def load_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        if await self.validate_key(refresh_token):
            return RefreshToken(token=refresh_token, client_id=client.client_id, scopes=[])
        return None

    async def exchange_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: RefreshToken, scopes: list[str]
    ) -> OAuthToken:
        return self._issue(refresh_token.token, scopes or refresh_token.scopes)

    # --- access tokens (stateless: the access token is the key) ------------
    async def load_access_token(self, token: str) -> KeyAccessToken | None:
        if await self.validate_key(token):
            return KeyAccessToken(token=token, client_id="mcp", scopes=[], expires_at=None, api_key=token)
        return None

    async def revoke_token(self, token) -> None:  # AccessToken | RefreshToken
        # The token is the API key; it is revoked by the user in the Superset cabinet.
        return None

    # --- helpers -----------------------------------------------------------
    def _issue(self, api_key: str, scopes: list[str]) -> OAuthToken:
        # Stateless: both tokens are the key itself, so sessions survive restarts.
        return OAuthToken(
            access_token=api_key,
            token_type="Bearer",
            expires_in=TOKEN_TTL,
            scope=" ".join(scopes) or None,
            refresh_token=api_key,
        )

    async def validate_key(self, api_key: str) -> bool:
        if not api_key:
            return False
        exp = self._valid_cache.get(api_key)
        if exp and exp > time.time():
            return True
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    f"{self.api_base}/v1/meta",
                    headers={"Authorization": f"Bearer {api_key}"},
                )
        except Exception:
            return False
        if resp.status_code == 200:
            self._valid_cache[api_key] = time.time() + _VALIDATE_CACHE_TTL
            return True
        return False

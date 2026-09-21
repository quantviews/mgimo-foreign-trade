"""Minimal OAuth 2.1 authorization server for the MCP server.

The "login" is pasting your MGIMO API key (from the Superset cabinet). The key
is validated against the API, then an opaque access token is issued that maps
back to that key. This lets OAuth-only clients (Claude web/mobile, ChatGPT)
connect, while clients that send the raw API key as a Bearer token keep working
- load_access_token also accepts a raw key (validated against the API).

Stores are in-memory (pilot): clients/codes/tokens are lost when the container
restarts, so users re-authorize after a redeploy. Fine for the pilot; move to
a shared store (SQLite/Redis) if it needs to survive restarts or scale out.
"""
from __future__ import annotations

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

TOKEN_TTL = 30 * 24 * 3600          # access token lifetime, seconds
CODE_TTL = 600                      # authorization code lifetime, seconds
_VALIDATE_CACHE_TTL = 300           # cache "key is valid" for 5 min


class KeyAccessToken(AccessToken):
    """AccessToken that also carries our per-user API key (not exposed externally)."""

    api_key: str = ""


class ApiKeyOAuthProvider:
    """OAuth AS where the credential is the user's MGIMO API key."""

    def __init__(self, api_base: str) -> None:
        self.api_base = api_base.rstrip("/")
        self._clients: dict[str, OAuthClientInformationFull] = {}
        self._codes: dict[str, tuple[AuthorizationCode, str]] = {}      # code -> (obj, api_key)
        self._tokens: dict[str, KeyAccessToken] = {}                    # access token -> obj
        self._refresh: dict[str, tuple[RefreshToken, str]] = {}         # refresh -> (obj, api_key)
        self._pending: dict[str, tuple[OAuthClientInformationFull, AuthorizationParams]] = {}
        self._valid_cache: dict[str, float] = {}                        # api_key -> expiry

    # --- dynamic client registration ---------------------------------------
    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        return self._clients.get(client_id)

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        self._clients[client_info.client_id] = client_info

    # --- authorization -----------------------------------------------------
    async def authorize(self, client: OAuthClientInformationFull, params: AuthorizationParams) -> str:
        """Stash the request and send the user to our key-entry page."""
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
        return self._issue(client.client_id, api_key, authorization_code.scopes, authorization_code.resource)

    # --- refresh -----------------------------------------------------------
    async def load_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        entry = self._refresh.get(refresh_token)
        return entry[0] if entry else None

    async def exchange_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: RefreshToken, scopes: list[str]
    ) -> OAuthToken:
        entry = self._refresh.pop(refresh_token.token, None)
        if entry is None:
            raise TokenError("invalid_grant", "Unknown refresh token")
        _, api_key = entry
        return self._issue(client.client_id, api_key, scopes or refresh_token.scopes, refresh_token.resource)

    # --- access tokens -----------------------------------------------------
    async def load_access_token(self, token: str) -> KeyAccessToken | None:
        obj = self._tokens.get(token)
        if obj is not None:
            if obj.expires_at is None or obj.expires_at > time.time():
                return obj
            self._tokens.pop(token, None)
            return None
        # Backwards compatibility: a raw API key sent directly as the Bearer.
        if await self.validate_key(token):
            return KeyAccessToken(token=token, client_id="raw", scopes=[], expires_at=None, api_key=token)
        return None

    async def revoke_token(self, token) -> None:  # AccessToken | RefreshToken
        tok = getattr(token, "token", None)
        if tok:
            self._tokens.pop(tok, None)
            self._refresh.pop(tok, None)

    # --- helpers -----------------------------------------------------------
    def _issue(self, client_id: str, api_key: str, scopes: list[str], resource) -> OAuthToken:
        access = secrets.token_urlsafe(32)
        refresh = secrets.token_urlsafe(32)
        self._tokens[access] = KeyAccessToken(
            token=access, client_id=client_id, scopes=scopes,
            expires_at=int(time.time() + TOKEN_TTL), resource=resource, api_key=api_key,
        )
        self._refresh[refresh] = (
            RefreshToken(token=refresh, client_id=client_id, scopes=scopes, resource=resource),
            api_key,
        )
        return OAuthToken(
            access_token=access, token_type="Bearer", expires_in=TOKEN_TTL,
            scope=" ".join(scopes) or None, refresh_token=refresh,
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

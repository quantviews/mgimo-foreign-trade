"""Superset cabinet page: existing, role-gated users mint their own API token.

No self-registration — admins create users and assign the required role in Superset.
A logged-in user with that role opens "Мой API-ключ" and generates/rotates a token,
which is written to the `tradeapi` Postgres DB the API reads.

Registered from superset_config.py via FLASK_APP_MUTATOR (see README). Synchronous
(psycopg2) because Superset is a sync Flask app; token hashing matches the API's
app/store.py (sha256 hex).
"""

from __future__ import annotations

import hashlib
import os
import secrets

import psycopg2
from flask import redirect, request
from flask_appbuilder import BaseView, expose
from flask_login import current_user

# tradeapi DSN (same DB the API uses). Inside the Superset container the Postgres
# service resolves as `postgres` on the shared Docker network.
TRADEAPI_DSN = os.environ.get(
    "TRADEAPI_DSN", "postgresql://superset:superset@postgres:5432/tradeapi"
)
# Superset role that grants API access. Admin assigns it to users.
REQUIRED_ROLE = os.environ.get("TRADEAPI_ROLE", "API")
DEFAULT_PLAN = os.environ.get("TRADEAPI_PLAN", "pilot")
LOGIN_URL = "/login/"


# --- token helpers (must match api/app/store.py) ----------------------------

def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _prefix(token: str) -> str:
    return token[:10]


def _generate() -> str:
    return "mgt_" + secrets.token_urlsafe(24)


def _connect():
    return psycopg2.connect(TRADEAPI_DSN, options="-c search_path=api")


def _has_role(user, role_name: str) -> bool:
    try:
        return any(getattr(r, "name", None) == role_name for r in user.roles)
    except Exception:
        return False


# --- store operations (sync) ------------------------------------------------

def issue_token(email: str, superset_user_id: int, plan_code: str = DEFAULT_PLAN) -> str:
    """Upsert the user, revoke previous active tokens, issue a fresh one."""
    raw = _generate()
    con = _connect()
    try:
        with con, con.cursor() as cur:
            cur.execute("SELECT id FROM plans WHERE code = %s AND active", (plan_code,))
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"plan '{plan_code}' not found in tradeapi")
            plan_id = row[0]
            cur.execute(
                "INSERT INTO users(email, superset_user_id, plan_id) VALUES (%s, %s, %s) "
                "ON CONFLICT (email) DO UPDATE SET superset_user_id = EXCLUDED.superset_user_id "
                "RETURNING id",
                (email, superset_user_id, plan_id),
            )
            user_id = cur.fetchone()[0]
            cur.execute(
                "UPDATE tokens SET revoked_at = now() "
                "WHERE user_id = %s AND revoked_at IS NULL",
                (user_id,),
            )
            cur.execute(
                "INSERT INTO tokens(user_id, token_hash, prefix) VALUES (%s, %s, %s)",
                (user_id, _hash(raw), _prefix(raw)),
            )
        return raw
    finally:
        con.close()


def token_info(email: str) -> dict | None:
    """Current active token prefix + this-month usage for a user, or None."""
    con = _connect()
    try:
        with con, con.cursor() as cur:
            cur.execute(
                """
                SELECT t.prefix, t.created_at,
                       (SELECT count(*) FROM audit_log a
                        WHERE a.user_id = u.id AND a.ts >= date_trunc('month', now()))
                FROM users u
                JOIN tokens t ON t.user_id = u.id AND t.revoked_at IS NULL
                WHERE u.email = %s
                ORDER BY t.created_at DESC
                LIMIT 1
                """,
                (email,),
            )
            r = cur.fetchone()
            return None if not r else {"prefix": r[0], "created_at": r[1], "usage": r[2]}
    finally:
        con.close()


# --- FAB view ---------------------------------------------------------------

class ApiKeyView(BaseView):
    route_base = "/apikey"
    default_view = "index"

    def _guard(self):
        """Return a response to short-circuit, or None if access is allowed."""
        if not getattr(current_user, "is_authenticated", False):
            return redirect(f"{LOGIN_URL}?next={request.path}")
        if not _has_role(current_user, REQUIRED_ROLE):
            return self.render_template("api_cabinet_denied.html", role=REQUIRED_ROLE)
        return None

    @expose("/")
    def index(self):
        blocked = self._guard()
        if blocked is not None:
            return blocked
        return self.render_template(
            "api_cabinet.html", info=token_info(current_user.email), raw=None
        )

    @expose("/generate", methods=["POST"])
    def generate(self):
        blocked = self._guard()
        if blocked is not None:
            return blocked
        raw = issue_token(current_user.email, current_user.id)
        return self.render_template(
            "api_cabinet.html", info=token_info(current_user.email), raw=raw
        )


def register(app) -> None:
    """Call from superset_config.py's FLASK_APP_MUTATOR."""
    import os as _os

    from jinja2 import ChoiceLoader, FileSystemLoader

    templates = _os.path.join(_os.path.dirname(__file__), "templates")
    app.jinja_loader = ChoiceLoader([app.jinja_loader, FileSystemLoader(templates)])
    app.appbuilder.add_view(
        ApiKeyView, "Мой API-ключ", category="Настройки", icon="fa-key"
    )

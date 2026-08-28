"""Service settings (env-driven). Prefix: MGIMO_API_ ; optional .env file."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Repo root = two levels up from this file (api/app/config.py -> repo/).
_REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MGIMO_API_", env_file=".env", extra="ignore"
    )

    # Read-only DuckDB serving file. Relative paths resolve from the repo root.
    duckdb_path: Path = Path("db/unified_trade_data.duckdb")
    duckdb_threads: int = 4

    # Postgres DSN for tokens/users/audit. Empty -> dev mode (no Postgres):
    # a single static dev token is accepted and audit is logged to stdout.
    postgres_dsn: str = ""
    dev_token: str = "dev-token"

    # Query guardrails (Phase 1). In prod max rows comes from the user's plan.
    default_page_rows: int = 10_000
    max_page_rows: int = 100_000

    api_title: str = "MGIMO Foreign Trade API"
    api_version: str = "0.1.0"

    def resolved_duckdb_path(self) -> Path:
        p = self.duckdb_path
        return p if p.is_absolute() else _REPO_ROOT / p


settings = Settings()

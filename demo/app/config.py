"""Demo chat service settings (env-driven). Prefix: MGIMO_DEMO_ ; optional .env."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MGIMO_DEMO_", env_file=".env", extra="ignore"
    )

    # DeepSeek (OpenAI-compatible chat completions with tool calling).
    deepseek_api_key: str = ""
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_model: str = "deepseek-chat"

    # The read-only data API, reached inside the docker network, and the single
    # demo-plan key used for every tool call (per-user limits live in demo_access).
    api_base: str = "http://trade-api:8000"
    service_api_key: str = ""

    # Postgres (same instance/schema as the API): users, demo_access, verifications.
    postgres_dsn: str = ""

    # Public base for building verification links.
    site_base_url: str = "https://nts.mgimo.ru"

    # Signed session cookie.
    session_secret: str = "change-me"
    session_ttl_hours: int = 12

    # Limits and cost guards.
    turn_limit: int = 10             # default per-user free turns (stored per row)
    daily_budget: int = 500          # global turns/day backstop
    max_tool_rounds: int = 6         # tool calls per turn before forcing an answer
    max_output_tokens: int = 1200
    max_history_messages: int = 24   # client history kept per turn
    verify_ttl_hours: int = 24

    # Domain gate.
    allow_domains: str = ""          # comma allowlist override (optional)
    extra_free_domains: str = ""     # extend the built-in free/disposable blocklist

    # Verification email.
    mail_from: str = "research@fief.ru"
    smtp_host: str = ""              # empty -> log link to stdout (dev)
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_starttls: bool = True


settings = Settings()

"""Unit tests for the pure token helpers (no Postgres / asyncpg needed)."""

from app.store import generate_token, hash_token, token_prefix


def test_generate_token_format_and_uniqueness():
    t = generate_token()
    assert t.startswith("mgt_")
    assert len(t) >= 24
    assert generate_token() != generate_token()


def test_hash_token_is_deterministic_sha256():
    assert hash_token("abc") == hash_token("abc")
    assert hash_token("abc") != hash_token("abd")
    assert len(hash_token("abc")) == 64  # sha256 hex


def test_token_prefix():
    t = "mgt_abcdefghij_tail"
    assert token_prefix(t) == t[:10]
    assert not token_prefix(t).endswith("tail")

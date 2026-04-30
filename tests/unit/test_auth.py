"""Unit tests for app.auth.auth — bcrypt password hashing + JWT issuance/decoding.

Bcrypt is intentionally slow (~250 ms per hash). The bcrypt-touching tests
are kept minimal to stay inside the pre-commit budget; the JWT tests are
instant and cover the meaningful failure modes (expired, wrong key,
malformed payload).
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from jose import jwt

from app.auth.auth import (
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password,
)
from app.config import JWT_ALGORITHM, JWT_SECRET_KEY

# ── Bcrypt password hashing ───────────────────────────────────────────


@pytest.mark.unit
def test_hash_and_verify_round_trip() -> None:
    h = hash_password("hunter2")
    assert verify_password("hunter2", h) is True


@pytest.mark.unit
def test_verify_fails_on_wrong_password() -> None:
    h = hash_password("hunter2")
    assert verify_password("hunter3", h) is False


@pytest.mark.unit
def test_hash_uses_random_salt_per_call() -> None:
    """Same password hashed twice must produce different hashes (salting)."""
    a = hash_password("same-pw")
    b = hash_password("same-pw")
    assert a != b
    # But both must verify
    assert verify_password("same-pw", a) is True
    assert verify_password("same-pw", b) is True


# ── JWT round-trip ────────────────────────────────────────────────────


@pytest.mark.unit
def test_create_and_decode_access_token_round_trip() -> None:
    token = create_access_token(user_id=4242)
    assert decode_access_token(token) == 4242


@pytest.mark.unit
def test_decode_returns_none_on_garbage_input() -> None:
    assert decode_access_token("not-a-jwt") is None
    assert decode_access_token("") is None


@pytest.mark.unit
def test_decode_returns_none_on_expired_token() -> None:
    """Manually mint a token with a past `exp` — decode must reject it."""
    expired = jwt.encode(
        {"sub": "4242", "exp": datetime.utcnow() - timedelta(hours=1)},
        JWT_SECRET_KEY,
        algorithm=JWT_ALGORITHM,
    )
    assert decode_access_token(expired) is None


@pytest.mark.unit
def test_decode_returns_none_on_wrong_secret() -> None:
    """A token signed with a different key must not validate."""
    foreign = jwt.encode(
        {"sub": "4242", "exp": datetime.utcnow() + timedelta(hours=1)},
        "different-secret-key",
        algorithm=JWT_ALGORITHM,
    )
    assert decode_access_token(foreign) is None


@pytest.mark.unit
def test_decode_returns_none_on_non_integer_sub() -> None:
    """If `sub` isn't coercible to int, decode must return None — not crash."""
    bad = jwt.encode(
        {"sub": "not-a-number", "exp": datetime.utcnow() + timedelta(hours=1)},
        JWT_SECRET_KEY,
        algorithm=JWT_ALGORITHM,
    )
    assert decode_access_token(bad) is None


@pytest.mark.unit
def test_decode_returns_none_when_sub_missing() -> None:
    """A valid-but-incomplete payload (no `sub`) must return None."""
    no_sub = jwt.encode(
        {"exp": datetime.utcnow() + timedelta(hours=1)},
        JWT_SECRET_KEY,
        algorithm=JWT_ALGORITHM,
    )
    assert decode_access_token(no_sub) is None


@pytest.mark.unit
def test_create_access_token_payload_contains_str_sub() -> None:
    """The contract: `sub` is encoded as a string of the user id, decoded back to int."""
    token = create_access_token(user_id=7)
    payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
    assert payload["sub"] == "7"
    assert "exp" in payload

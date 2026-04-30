"""Integration tests for auth-related Postgres operations.

Runs against the seeded test Postgres (CI service container or local DB
pointed at by TEST_DATABASE_URL). Each test owns its own transactionally-
clean state — fixture truncates `auth_users` before each test so order
doesn't matter.

Covers:
- create_auth_user → get_auth_user_by_email round trip
- get_auth_user_by_id with empty allowed_pages / extra_roles
- get_auth_user_by_id parses JSONB extra_roles correctly
- update_auth_user_password
- deactivate_auth_user (is_active flips)
- list_auth_users
- update_auth_user_last_login

These are the slices that contract tests CAN'T cover because they exercise
real SQL semantics (JSONB parsing, UNIQUE constraints, RETURNING clauses).
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg2
import pytest


@pytest.fixture(autouse=True)
def _clean_auth_users(test_database_url: str | None) -> Iterator[None]:
    """Truncate auth_users before every integration test in this file.

    Autouse so tests don't have to remember to ask for it; ensures full order-
    independence which pytest-randomly stresses.
    """
    if test_database_url is None:
        # Test will be skipped by conftest's collection hook; nothing to do here.
        yield
        return
    conn = psycopg2.connect(test_database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE auth_users RESTART IDENTITY CASCADE")
        conn.commit()
    finally:
        conn.close()
    # Also bust get_auth_user_by_id's 60-second in-memory cache so per-test reads see fresh data.
    try:
        from app.db import postgres_conn

        postgres_conn._auth_user_cache.clear()
    except Exception:
        pass
    yield


@pytest.fixture
def db_conn(test_database_url: str | None) -> Iterator[psycopg2.extensions.connection]:
    """Per-test raw psycopg2 connection for tests that need to SELECT directly."""
    assert test_database_url is not None
    conn = psycopg2.connect(test_database_url)
    try:
        yield conn
    finally:
        conn.close()


# ── create + read round trip ──────────────────────────────────────────


@pytest.mark.integration
def test_create_then_get_by_email() -> None:
    from app.db.postgres_conn import create_auth_user, get_auth_user_by_email

    new_id = create_auth_user(
        email="alice@cmtrading.com",
        full_name="Alice Smith",
        password_hash="bcrypt$placeholder",
        role="admin",
        crm_user_id=None,
    )
    assert isinstance(new_id, int)
    fetched = get_auth_user_by_email("alice@cmtrading.com")
    assert fetched is not None
    assert fetched["id"] == new_id
    assert fetched["full_name"] == "Alice Smith"
    assert fetched["role"] == "admin"
    assert fetched["is_active"] == 1
    # New users force a password change on first login
    assert fetched["force_password_change"] == 1


@pytest.mark.integration
def test_get_by_email_returns_none_for_unknown() -> None:
    from app.db.postgres_conn import get_auth_user_by_email

    assert get_auth_user_by_email("ghost@cmtrading.com") is None


@pytest.mark.integration
def test_get_by_id_returns_none_for_unknown() -> None:
    from app.db.postgres_conn import get_auth_user_by_id

    assert get_auth_user_by_id(9_999_999) is None


@pytest.mark.integration
def test_get_by_id_parses_extra_roles_jsonb() -> None:
    """extra_roles JSONB must come out as a Python list, not a JSON string."""
    from app.db.postgres_conn import create_auth_user, get_auth_user_by_id, update_auth_user

    new_id = create_auth_user(
        email="bob@cmtrading.com",
        full_name="Bob",
        password_hash="bcrypt$placeholder",
        role="admin",
        crm_user_id=None,
    )
    update_auth_user(
        user_id=new_id,
        full_name="Bob",
        email="bob@cmtrading.com",
        role="admin",
        is_active=1,
        crm_user_id=None,
        allowed_pages=None,
        extra_roles='["retention_gmt", "sales_gmt"]',
    )

    fetched = get_auth_user_by_id(new_id)
    assert fetched is not None
    assert fetched["extra_roles"] == ["retention_gmt", "sales_gmt"]


@pytest.mark.integration
def test_get_by_id_with_no_allowed_pages_returns_none() -> None:
    from app.db.postgres_conn import create_auth_user, get_auth_user_by_id

    new_id = create_auth_user(
        email="charlie@cmtrading.com",
        full_name="Charlie",
        password_hash="bcrypt$placeholder",
        role="agent",
        crm_user_id=None,
    )
    fetched = get_auth_user_by_id(new_id)
    assert fetched is not None
    assert fetched.get("allowed_pages_list") is None
    assert fetched["extra_roles"] == []


# ── password + deactivation ───────────────────────────────────────────


@pytest.mark.integration
def test_update_password_and_clear_force_change() -> None:
    from app.db.postgres_conn import (
        create_auth_user,
        get_auth_user_by_email,
        update_auth_user_password,
    )

    new_id = create_auth_user(
        email="dave@cmtrading.com",
        full_name="Dave",
        password_hash="old-hash",
        role="agent",
        crm_user_id=None,
    )
    update_auth_user_password(new_id, "new-hash", force_change=0)
    fetched = get_auth_user_by_email("dave@cmtrading.com")
    assert fetched is not None
    assert fetched["password_hash"] == "new-hash"
    assert fetched["force_password_change"] == 0


@pytest.mark.integration
def test_deactivate_flips_is_active() -> None:
    from app.db.postgres_conn import create_auth_user, deactivate_auth_user, get_auth_user_by_email

    new_id = create_auth_user(
        email="eve@cmtrading.com",
        full_name="Eve",
        password_hash="x",
        role="agent",
        crm_user_id=None,
    )
    deactivate_auth_user(new_id)
    fetched = get_auth_user_by_email("eve@cmtrading.com")
    assert fetched is not None
    assert fetched["is_active"] == 0


# ── list + last_login ─────────────────────────────────────────────────


@pytest.mark.integration
def test_list_auth_users_returns_all() -> None:
    from app.db.postgres_conn import create_auth_user, list_auth_users

    create_auth_user("a@x.com", "A", "h", "admin", None)
    create_auth_user("b@x.com", "B", "h", "agent", None)
    create_auth_user("c@x.com", "C", "h", "agent", None)

    users = list_auth_users()
    emails = {u["email"] for u in users}
    assert {"a@x.com", "b@x.com", "c@x.com"} <= emails


@pytest.mark.integration
def test_update_last_login_sets_timestamp(db_conn) -> None:
    from app.db.postgres_conn import create_auth_user, update_auth_user_last_login

    new_id = create_auth_user("frank@x.com", "Frank", "h", "agent", None)
    update_auth_user_last_login(new_id)

    with db_conn.cursor() as cur:
        cur.execute("SELECT last_login FROM auth_users WHERE id = %s", (new_id,))
        row = cur.fetchone()
    assert row is not None
    assert row[0] is not None  # timestamp populated


# ── UNIQUE constraint ──────────────────────────────────────────────────


@pytest.mark.integration
def test_duplicate_email_rejected() -> None:
    from app.db.postgres_conn import create_auth_user

    create_auth_user("dup@x.com", "First", "h", "agent", None)
    with pytest.raises(psycopg2.IntegrityError):
        create_auth_user("dup@x.com", "Second", "h", "agent", None)

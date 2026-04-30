"""End-to-end integration tests: HTTP → real auth via DB → real query → response.

These exercise the full stack — no monkeypatching of `get_auth_user_by_id`,
unlike contract tests. The cookie carries a real JWT; the route looks the
user up in the test Postgres and runs real SQL. Slower than contract tests
but proves the auth + DB integration actually holds.

Phase 3 deliberately keeps these focused on small read paths
(/api/last-sync, /api/mv-status). Bigger surfaces (full /api/performance
with seeded data) are deferred until we either have a richer fixture
loader or move to Phase 4 GUI tests.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import datetime

import psycopg2
import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture(autouse=True)
def _clean_tables(test_database_url: str | None) -> Iterator[None]:
    if test_database_url is None:
        yield
        return
    conn = psycopg2.connect(test_database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE auth_users RESTART IDENTITY CASCADE")
            cur.execute("TRUNCATE sync_log RESTART IDENTITY")
        conn.commit()
    finally:
        conn.close()
    # Bust the in-memory user cache too.
    try:
        from app.db import postgres_conn

        postgres_conn._auth_user_cache.clear()
    except Exception:
        pass
    yield


def _seed_admin_and_mint_token(email: str = "e2e-admin@cmtrading.com") -> tuple[int, str]:
    from app.auth.auth import create_access_token, hash_password
    from app.db.postgres_conn import create_auth_user

    user_id = create_auth_user(
        email=email,
        full_name="E2E Admin",
        password_hash=hash_password("test"),
        role="admin",
        crm_user_id=None,
    )
    return user_id, create_access_token(user_id)


@pytest.fixture
async def admin_e2e_client(test_database_url: str | None) -> AsyncIterator[AsyncClient]:
    """Httpx AsyncClient with a real admin session — auth lookups hit the DB."""
    assert test_database_url is not None
    _user_id, token = _seed_admin_and_mint_token()
    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        cookies={"access_token": token},
        base_url="http://test",
    ) as client:
        yield client


# ── /api/last-sync ────────────────────────────────────────────────────


@pytest.mark.integration
async def test_last_sync_returns_seeded_data_for_authed_user(
    admin_e2e_client: AsyncClient,
    test_database_url: str | None,
) -> None:
    """End-to-end: seed sync_log → admin GETs /api/last-sync → JSON keyed by table."""
    assert test_database_url is not None
    conn = psycopg2.connect(test_database_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO sync_log (table_name, cutoff_used, status, ran_at)
                   VALUES ('accounts', %s, 'success', %s)""",
                (datetime(2026, 4, 30, 12, 0, 0), datetime(2026, 4, 30, 12, 0, 0)),
            )
        conn.commit()
    finally:
        conn.close()

    r = await admin_e2e_client.get("/api/last-sync")
    assert r.status_code == 200
    body = r.json()
    assert "accounts" in body
    assert body["accounts"].startswith("2026-04-30T12:00:00")


@pytest.mark.integration
async def test_last_sync_returns_empty_dict_when_log_empty(
    admin_e2e_client: AsyncClient,
) -> None:
    r = await admin_e2e_client.get("/api/last-sync")
    assert r.status_code == 200
    assert r.json() == {}


# ── /api/mv-status ────────────────────────────────────────────────────


@pytest.mark.integration
async def test_mv_status_returns_list_for_admin(
    admin_e2e_client: AsyncClient,
) -> None:
    """No MVs created in test schema; route should still respond 200 with a list."""
    r = await admin_e2e_client.get("/api/mv-status")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list)
    # Each entry has the expected shape regardless of whether the MV exists
    for entry in body:
        assert "name" in entry
        assert "last_refresh" in entry


# ── Auth flow end-to-end through the DB ───────────────────────────────


@pytest.mark.integration
async def test_login_with_real_credentials_sets_cookie(
    test_database_url: str | None,
) -> None:
    """The full POST /login → DB lookup → bcrypt verify → JWT issue path."""
    assert test_database_url is not None
    from app.auth.auth import hash_password
    from app.db.postgres_conn import create_auth_user
    from app.main import app

    create_auth_user(
        email="login-test@cmtrading.com",
        full_name="Login Test",
        password_hash=hash_password("correct-password"),
        role="admin",
        crm_user_id=None,
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post(
            "/login",
            data={"email": "login-test@cmtrading.com", "password": "correct-password"},
            follow_redirects=False,
        )
    # Successful login → 302 to / with access_token set
    assert r.status_code == 302
    assert r.headers["location"] == "/"
    set_cookie = r.headers.get("set-cookie", "")
    assert "access_token=" in set_cookie


@pytest.mark.integration
async def test_login_with_wrong_password_returns_401(
    test_database_url: str | None,
) -> None:
    assert test_database_url is not None
    from app.auth.auth import hash_password
    from app.db.postgres_conn import create_auth_user
    from app.main import app

    create_auth_user(
        email="wrong-pw@cmtrading.com",
        full_name="X",
        password_hash=hash_password("real-password"),
        role="admin",
        crm_user_id=None,
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post(
            "/login",
            data={"email": "wrong-pw@cmtrading.com", "password": "definitely-wrong"},
        )
    assert r.status_code == 401


@pytest.mark.integration
async def test_login_with_deactivated_user_returns_401(
    test_database_url: str | None,
) -> None:
    assert test_database_url is not None
    from app.auth.auth import hash_password
    from app.db.postgres_conn import create_auth_user, deactivate_auth_user
    from app.main import app

    user_id = create_auth_user(
        email="inactive@cmtrading.com",
        full_name="X",
        password_hash=hash_password("pw"),
        role="admin",
        crm_user_id=None,
    )
    deactivate_auth_user(user_id)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/login", data={"email": "inactive@cmtrading.com", "password": "pw"})
    assert r.status_code == 401


# ── Token decode failure path through real route ──────────────────────


@pytest.mark.integration
async def test_authed_request_with_token_for_deleted_user_redirects(
    test_database_url: str | None,
) -> None:
    """If the user the token references doesn't exist → 302 to /login + cookie cleared."""
    assert test_database_url is not None
    from app.auth.auth import create_access_token
    from app.main import app

    # Mint a token for a user_id that doesn't exist in the DB.
    token = create_access_token(user_id=99_999_999)

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        cookies={"access_token": token},
        base_url="http://test",
    ) as client:
        r = await client.get("/api/last-sync", follow_redirects=False)
    # get_current_user converts a non-existent user to a /login redirect, then the
    # API route turns that into 401.
    assert r.status_code == 401

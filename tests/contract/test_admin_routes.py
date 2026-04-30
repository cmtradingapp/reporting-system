"""Contract tests for admin / system routes.

Covers `/admin/users` (auth + user CRUD APIs), `/holidays`, `/api/last-sync`
and `/api/mv-status`. All these endpoints have stricter auth (require_admin
or page-level allowed_pages) — proving the access boundary holds is the
biggest value here.

DB-touching success paths (creating users, listing holidays) belong in
tests/integration where they can run against a seeded Postgres.
"""

from __future__ import annotations

import pytest

from tests.conftest import make_user

# ────────────────────────────────────────────────────────────────────
# users_mgmt — /admin/users + /api/admin/users/*
# ────────────────────────────────────────────────────────────────────


@pytest.mark.contract
async def test_users_mgmt_page_redirects_anon(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/admin/users", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"


@pytest.mark.contract
async def test_users_mgmt_page_403_for_non_admin(client_factory) -> None:
    """`require_admin` raises HTTPException 403 for non-admin authed users."""
    async with client_factory(user=make_user(role="agent")) as c:
        r = await c.get("/admin/users")
    assert r.status_code == 403


@pytest.mark.contract
async def test_create_user_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.post(
            "/api/admin/users",
            json={"email": "x@x.com", "full_name": "X", "role": "agent"},
        )
    assert r.status_code == 401


@pytest.mark.contract
async def test_create_user_403_for_non_admin(client_factory) -> None:
    async with client_factory(user=make_user(role="agent")) as c:
        r = await c.post(
            "/api/admin/users",
            json={"email": "x@x.com", "full_name": "X", "role": "agent"},
        )
    assert r.status_code == 403


@pytest.mark.contract
async def test_reset_password_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.post("/api/admin/users/42/reset-password")
    assert r.status_code == 401


@pytest.mark.contract
async def test_reset_password_403_for_non_admin(client_factory) -> None:
    async with client_factory(user=make_user(role="agent")) as c:
        r = await c.post("/api/admin/users/42/reset-password")
    assert r.status_code == 403


@pytest.mark.contract
async def test_deactivate_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.post("/api/admin/users/42/deactivate")
    assert r.status_code == 401


@pytest.mark.contract
async def test_deactivate_403_for_non_admin(client_factory) -> None:
    async with client_factory(user=make_user(role="agent")) as c:
        r = await c.post("/api/admin/users/42/deactivate")
    assert r.status_code == 403


# ────────────────────────────────────────────────────────────────────
# holidays
# ────────────────────────────────────────────────────────────────────


@pytest.mark.contract
async def test_holidays_page_redirects_anon(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/holidays", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"


@pytest.mark.contract
async def test_holidays_page_redirects_non_admin_without_access(client_factory) -> None:
    user = make_user(role="agent", allowed_pages_list=["performance"])
    async with client_factory(user=user) as c:
        r = await c.get("/holidays", follow_redirects=False)
    # Default RedirectResponse(url=...) → 307
    assert r.status_code == 307
    assert r.headers["location"] == "/performance"


@pytest.mark.contract
async def test_holidays_post_missing_date_returns_400(client_factory) -> None:
    """Validation runs before the DB call — auth itself isn't required by this route
    (no get_current_user check), but the validator catches the missing date first."""
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.post("/api/holidays", json={"description": "no-date"})
    assert r.status_code == 400
    assert r.json() == {"detail": "date is required"}


# ────────────────────────────────────────────────────────────────────
# last_sync + mv_status
# ────────────────────────────────────────────────────────────────────


@pytest.mark.contract
async def test_last_sync_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/api/last-sync")
    assert r.status_code == 401


@pytest.mark.contract
async def test_mv_status_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/api/mv-status")
    assert r.status_code == 401


@pytest.mark.contract
async def test_mv_status_403_for_non_admin(client_factory) -> None:
    async with client_factory(user=make_user(role="agent")) as c:
        r = await c.get("/api/mv-status")
    assert r.status_code == 403

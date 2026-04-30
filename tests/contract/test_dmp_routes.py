"""Contract tests for the Daily/Monthly Performance routes.

Covers the slices that don't need a Postgres connection:
- auth + redirect behaviour (unauthenticated, expired token, deactivated user)
- page-level access control (allowed_pages routing)
- API response shape on cache hit (no DB call) — including the dTradersHQ
  contract: every retention API response must include `global_daily_traders_hq`
- input validation (invalid date format → 400)

Real-DB success paths belong in tests/integration where they can run against a
seeded Postgres.
"""

from __future__ import annotations

import pytest

from app import cache
from tests.conftest import make_user

# ── GET /daily-monthly (page) ─────────────────────────────────────────


@pytest.mark.contract
async def test_dmp_page_redirects_to_login_without_auth(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/daily-monthly", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"


@pytest.mark.contract
async def test_dmp_page_redirects_to_login_on_garbage_token(client_factory) -> None:
    """A junk cookie value must redirect to /login and clear the cookie."""
    async with client_factory(user=None) as c:
        c.cookies.set("access_token", "not-a-real-jwt")
        r = await c.get("/daily-monthly", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"


@pytest.mark.contract
async def test_dmp_page_renders_for_admin(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/daily-monthly", follow_redirects=False)
    assert r.status_code == 200
    # Server-rendered Jinja — page title is set in the template
    assert "text/html" in r.headers["content-type"]


@pytest.mark.contract
async def test_dmp_page_redirects_non_admin_without_permission(client_factory) -> None:
    """Non-admin user with allowed_pages that doesn't include daily_monthly → redirect."""
    user = make_user(role="agent", allowed_pages_list=["performance"])
    async with client_factory(user=user) as c:
        r = await c.get("/daily-monthly", follow_redirects=False)
    assert r.status_code == 302
    # Falls through to the first matching allowed page
    assert r.headers["location"] == "/performance"


@pytest.mark.contract
async def test_dmp_page_renders_when_in_allowed_pages(client_factory) -> None:
    user = make_user(role="agent", allowed_pages_list=["daily_monthly"], department_="Sales")
    async with client_factory(user=user) as c:
        r = await c.get("/daily-monthly", follow_redirects=False)
    assert r.status_code == 200


@pytest.mark.contract
async def test_deactivated_user_is_redirected(client_factory) -> None:
    """is_active=0 must be treated as no auth, regardless of valid token."""
    user = make_user(role="admin", is_active=0)
    async with client_factory(user=user) as c:
        r = await c.get("/daily-monthly", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"


# ── GET /api/daily-monthly/sales ──────────────────────────────────────


@pytest.mark.contract
async def test_dmp_sales_api_returns_401_without_auth(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/api/daily-monthly/sales", params={"date_from": "2026-04-01", "date_to": "2026-04-30"})
    assert r.status_code == 401
    assert r.json() == {"detail": "Unauthorized"}


@pytest.mark.contract
async def test_dmp_sales_api_returns_400_on_invalid_date(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/api/daily-monthly/sales", params={"date_from": "not-a-date", "date_to": "2026-04-30"})
    assert r.status_code == 400
    assert r.json() == {"detail": "Invalid date format"}


@pytest.mark.contract
async def test_dmp_sales_api_returns_cached_response_on_cache_hit(client_factory) -> None:
    """Prime the cache; route must short-circuit to the cached payload, no DB call."""
    fake_payload = {
        "rows": [],
        "working_days": 22,
        "working_days_passed": 18,
        "working_days_left": 4,
        "target_ratio": 1.0,
        "wd_in_range": 22,
        "global_daily_ftd": 0,
        "global_monthly_ftd": 0,
        "global_daily_ftc": 0,
        "global_monthly_ftc": 0,
        "global_daily_net": 0.0,
        "global_monthly_net": 0.0,
    }
    # Cache key shape from app/routes/daily_monthly_performance.py:
    #   f"dmp_sales_v2:{role}:{extra_roles}:{date_from}:{date_to}{cls_suffix}"
    cache.set("dmp_sales_v2:admin::2026-04-01:2026-04-30", fake_payload)

    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/api/daily-monthly/sales", params={"date_from": "2026-04-01", "date_to": "2026-04-30"})
    assert r.status_code == 200
    assert r.json() == fake_payload


# ── GET /api/daily-monthly/retention ──────────────────────────────────


@pytest.mark.contract
async def test_dmp_retention_api_returns_401_without_auth(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get(
            "/api/daily-monthly/retention",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 401


@pytest.mark.contract
async def test_dmp_retention_api_returns_cached_response_with_dtradershq_key(client_factory) -> None:
    """The dTradersHQ regression at the contract level.

    Every cached response must round-trip through the API including the
    `global_daily_traders_hq` key — the field whose absence (when sales raced
    retention) caused the toLocaleString crash on the Performance page.
    """
    fake_payload = {
        "rows": [],
        "working_days": 22,
        "working_days_passed": 18,
        "working_days_left": 4,
        "target_ratio": 1.0,
        "wd_in_range": 22,
        "global_daily_traders": 0,
        "global_daily_traders_hq": 223,  # <<< the key that mattered
        "global_monthly_loads": 0,
        "global_daily_loads": 0,
    }
    # Cache key matches app/routes/daily_monthly_performance.py: dmp_ret_v4:...
    cache.set("dmp_ret_v4:admin::2026-04-01:2026-04-30", fake_payload)

    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/daily-monthly/retention",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 200
    body = r.json()
    assert "global_daily_traders_hq" in body, "regression: dTradersHQ contract field missing"
    assert body["global_daily_traders_hq"] == 223


@pytest.mark.contract
async def test_dmp_retention_cache_key_includes_extra_roles(client_factory) -> None:
    """Bug 37afb55 — two users with different extra_roles must NOT share a cache entry.

    Prime the cache for a user with no extras; an authed user WITH extras must NOT
    hit the cached entry (its key includes a different `extra_roles` segment).
    """
    fake_payload_for_no_extras = {
        "rows": [],
        "working_days": 22,
        "working_days_passed": 18,
        "working_days_left": 4,
        "target_ratio": 1.0,
        "wd_in_range": 22,
        "global_daily_traders": 0,
        "global_daily_traders_hq": 0,
        "global_monthly_loads": 0,
        "global_daily_loads": 0,
    }
    cache.set("dmp_ret_v4:admin::2026-04-01:2026-04-30", fake_payload_for_no_extras)
    # User with extras — cache key shape: dmp_ret_v4:admin:retention_gmt:date_from:date_to
    user_with_extras = make_user(role="admin", extra_roles=["retention_gmt"])
    other_payload = {**fake_payload_for_no_extras, "global_daily_traders_hq": 999}
    cache.set("dmp_ret_v4:admin:retention_gmt:2026-04-01:2026-04-30", other_payload)

    async with client_factory(user=user_with_extras) as c:
        r = await c.get(
            "/api/daily-monthly/retention",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 200
    # Must hit the extras-specific entry, not the bare-admin entry
    assert r.json()["global_daily_traders_hq"] == 999
